# Migrating givenergy-modbus onto modbus-connection

A survey of ~65 real-world Modbus libraries classified this one **BLOCKED**: a custom "Transparent" framer with an embedded device serial and its own CRC, a mandatory dongle heartbeat, a transmit throttle, FC06-only writes, and last-good data served on a rejected read. The survey's answer to that class of library was that `modbus-connection`'s pure Protocol layer — `ModbusConnection` and the `ModbusUnit` Protocol, neither of which imports a backend — *is* the extension seam, and that such a library needs no library work at all.

This migration tests that claim. The verdict up front: **the transport seam holds completely, and the model framework holds for 1253 of 1260 register definitions.** Nothing in `modbus-connection` had to change, and nothing needed to. Three things had to be reached around — all of them things the shipped backends reach around too — and no gap survived checking as a blocker. Details below, including two findings I withdrew once I measured them.

Written against 4.3, revised on the move to **4.4**, which closed one of the findings below — (i), the live field set — outright, and again on the move to **4.5**, which reopened (b) from the other end: a `ComponentGroup` now merges readable ranges that touch, and every GivEnergy bank touches its neighbour. Where a finding changed, the note says so rather than being deleted.

Where this landed:

| | |
|---|---|
| `givenergy_modbus/connection.py` | 890 lines — the transport as a `ModbusConnection` backend |
| `givenergy_modbus/model/components.py` | 506 lines — every device family as a `Component` |
| `givenergy_modbus/client/client.py` | −471 / +183 lines: the socket, the pump tasks and the retry loop moved out |
| tests | 1777 passing, including the model layer driven end to end over real framing |

---

## 1. What weird things does this library do?

### The wire format is Modbus-shaped, not Modbus

Frames carry a normal 7-byte MBAP header, and then diverge on every field. `tid` is pinned to the constant `0x5959` (`YY` in ASCII), `pid` to `0x0001`, `uid` to `0x01`; `len` counts one byte more than the spec says. Function code `0x02`/"Transparent" wraps a GivEnergy sub-frame that carries the *real* function code (0x03/0x04/0x06), a 10-byte data-adapter serial, an 8-byte pad, a device address, and a CRC of its own. Because the first two header fields are constants, the framer finds frames by scanning for the literal `0x59590001`.

The pad byte is the strangest part: it is not understood, it varies predictably by command, and **setting it to zero makes the inverter stop answering**. Nobody knows why. It is copied from observed traffic and left alone.

**Pinning `tid` to a constant is the consequential quirk.** Modbus's transaction id exists to match answers to questions, and this protocol throws it away. Responses are therefore correlated by *shape hash* — a tuple of (function, device address, base register, register count). Two concurrent requests of identical shape are indistinguishable, so the newer one wins and the older is cancelled. Everything about in-flight tracking follows from that one decision.

### The device you are talking to is three devices away

```
library ↔ TCP ↔ wifi/GPRS dongle ↔ internal serial ↔ inverter ↔ RS485 ↔ BMS(s)
```

Reading "battery pack 1" at device address `0x32` does not reach a battery. The inverter polls its BMSes over RS485 and caches the results; the dongle polls the inverter and re-exposes that cache over TCP. Two consequences the library has to live with:

- **Cache freeze.** If a BMS stops answering on RS485 — say, during a firmware update — the inverter keeps serving the last-known-good page rather than blanking. From the TCP side that looks like a battery emitting byte-identical responses forever while its neighbours update normally. Telling "frozen" from "genuinely unchanged" needs multiple consecutive reads.
- **Error responses come from the inverter**, not the device you addressed. The BMS silently drops malformed frames; anything richer is the inverter refusing a request against its own cached banks.

Layered on that are guards you would not expect in a Modbus library: a "sub-bus splice" detector that rejects a battery bank showing two or more physically impossible deltas at once, cold-start holds that need a corroborating re-read before a battery's first bank commits, and a configurable heal window before a disputed *constant* (cell count, BMS firmware version) is allowed to change.

### The dongle expects to be talked to, and talked to slowly

It sends an unsolicited FC01 heartbeat every ~3 minutes and closes the socket after three unanswered ones — so answering is not optional, and there is no request/response frame to hang it off.

Consecutive frames need a gap. 0.25 s is empirically load-bearing across hardware generations, with additive jitter on top so that polling ticks and retry storms disperse instead of clumping on fixed boundaries. Push frames out faster and the dongle silently drops them.

There is also a multi-second *silent window* in the field where the device answers nothing at all. Retrying immediately lands the retry inside the same window and accomplishes nothing, so retries deliberately wait (0.5 s by default) before the next attempt.

### Success does not mean fresh

`refresh()` can return normally while serving last-known-good data for a device whose live read was rejected — by the CRC guard, the splice guard, or a bank hold. Consumers are told to gate on `Plant.register_age()` / `block_age()`, not on the call returning. A poll that reads some banks and not others raises `RefreshPartiallySucceeded`, which *carries the partial plant*: the exception is the consumer's opportunity to use what did arrive.

### Writing is FC06-only, behind two allowlists

FC16 does not exist on this hardware. Every write is a single-register FC06 — and because the wrong address can damage real hardware, a writable register must appear in **two** separate allowlists (`manifest.write_safe_registers` for the model-aware client-boundary check, `pdu.write_registers.WRITE_SAFE_REGISTERS` enforced at `encode()` time). Miss the second and the request builds fine and dies on the wire.

### Which registers exist depends on the device, and you have to find out

There is no discovery function code. `detect()` probes candidate device addresses and banks, and infers topology from what answers and what stays silent — an all-zero response means "missing", an error response means "this bank isn't served here", silence means "nothing at this address". An AC-coupled inverter answers `HR(300-359)` and a hybrid times out on it; extended charge slots exist above a firmware threshold on some models only. **The readable address map is a runtime discovery, not a static fact**, which is what shapes how the device has to be modelled: per bank, since a bank is what the device serves or refuses as a unit.

---

## 2. What internals of modbus-connection did you have to touch?

Short list, and none of it is fatal. Nothing was monkeypatched, nothing was vendored, and `modbus-connection` itself is unmodified.

### Subclassed as intended

`GivEnergyConnection(ModbusConnection)` implements the three documented hooks — `_connect_client`, `_close_client`, `for_unit`. `GivEnergyUnit` satisfies the `ModbusUnit` Protocol structurally; `isinstance(unit, ModbusUnit)` passes and all 19 methods are present. `GivEnergyField(RegisterField)` overrides `decode`, which the class documents as its only abstract method. That is three public extension points used exactly as designed, and they carried the whole migration.

### Reached around — three things

**`self._client` and `self._lost_callbacks.fire()`.** The base class treats `_client` as "the connected backend client", and `connected` is `_client is not None`. When the link dies — reader EOF, a stalled drain, a socket error — a backend has to make `connected` go false, fire the subscribers, and let the next request start a fresh connect flight. `on_connection_lost()` is public for *subscribing*, and there is no protected helper for *reporting*, so `_note_lost()` does it by hand.

This turns out to be the established pattern rather than a hole: tmodbus's `_on_connection_lost` and pymodbus's `_on_trace_connect` end with the same two lines against the same two privates. Ours is a third copy of it. See (a) below.

**`self._pacer`.** The inherited `Pacer` is protected, which is fine for the connection itself — the producer wraps each wire write in `async with self._pacer.paced(unit_id)`. But `set_message_spacing` lives on the *unit*, and a unit handle is not a subclass of the connection, so `GivEnergyUnit` calls a `set_unit_spacing()` method added to the connection purely to bridge that gap.

### One `type: ignore`

`ModbusConnection.__init__` types `params` as `ModbusTcpParams | ModbusUdpParams | ModbusTlsParams | ModbusSerialParams` — a closed union of the four transports the library ships (4.4 dropped the `ModbusParams` alias for it and spells the union inline, which changes nothing here). A third-party transport with its own params dataclass is precisely what the Protocol seam is for, so the union is too narrow by construction. Everything the base class actually *does* with the value works fine on ours; only the annotation objects. One suppressed line, at one construction site.

### Re-parented our own exception hierarchy

`givenergy_modbus.exceptions.ExceptionBase` now derives from `modbus_connection.ModbusError`, and `ConnectionLost` / `ConnectionFailed` are also `ModbusConnectionError`. Not strictly forced, but a backend that raises outside the library's hierarchy is not a drop-in backend. `ConnectionLost` now has three bases (`CommunicationError`, `ModbusConnectionError`, `TimeoutError`) so the historical `except TimeoutError` contract still holds.

### What we should have done differently: `asyncio.Protocol`, not `StreamReader`

The transport detects a dropped link by polling `reader.at_eof()` around
`reader.read()`, plus a `_DRAIN_TIMEOUT` watchdog on `writer.drain()` to catch a
half-open peer, plus a manual `writer.close()` when a loss is noticed. That is
three mechanisms doing the job of one, and it is the reason a lost session
leaked its socket until a test hung on it.

It is that way because the pre-migration `Client` was built on
`asyncio.open_connection`, and the migration moved that code rather than
reconsidering it. tmodbus does it properly: `ModbusTcpProtocol` is an
`asyncio.Protocol`, so `connection_lost(exc)` fires on reset, error and clean
close alike, the transport closes itself, and there is no drain watchdog because
`transport.write()` doesn't block.

**Why not just use tmodbus's transport, then?** Because it isn't separable from
its framing: `ModbusTcpProtocol` builds MBAP headers inline and keys
`_pending_requests` by transaction id. GivEnergy pins the transaction id to a
constant, so every request would collide on the same key — the correlator is the
one thing that cannot be reused here. The right fix is our own
`asyncio.Protocol` subclass, not a `StreamReader` loop; it would delete the
watchdog, the manual close and the EOF polling. Worth doing, and not done here.

### What we did *not* need

No fork, no vendored copy, no patched planner, no reimplemented `Component`. The retry loop, the heartbeat responder and the frame reassembly all live in our own module and the library never sees them — which is the seam working.

---

## 3. What could modbus-connection do better?

Ordered by how much each one cost. (a), (b) and (c) are corrections — my first pass overstated all three, and checking each against the shipped backends, a measurement and the wire captures deflated them. They are kept rather than deleted because the reasoning that produced them is the interesting part.

### a. Reporting connection loss is copy-pasted into every backend

Both shipped backends end their loss hook with the same lines against base-class privates — tmodbus's `_on_connection_lost`, pymodbus's `_on_trace_connect`:

```python
self._client = None
self._lost_callbacks.fire()
```

So the mechanism isn't missing; it's just not shared, and ours is a third copy. That's a DRY nit, not a capability gap.

**Fix:** a protected `_connection_lost()` on `ModbusConnection` doing exactly what all three do today. Small win, but it would also pin down the contract: both backends guard with `if self._closed or self._client is None`, relying on `close()`/`disconnect()` unpublishing the client first, so a hook that finds no published client knows it is watching our own teardown. That's a neater invariant than the "am I closing?" flag we invented, and it's currently only discoverable by reading two backends.

One thing genuinely differs for a self-detecting backend: tmodbus and pymodbus are *told* by their transport, so the socket is already gone by the time the hook runs. We detect EOF ourselves with the writer still open, so we have to close it — a shared helper should not assume the transport is already down.

### b. A component must not span banks it might lose — but that's the device's fault

`ReadPlan.execute` re-raises a refused block and discards the pass. My first framing of this as the biggest model-layer gap was wrong: it's the correct all-or-nothing default, and modelling per *bank* rather than per *device family* fits GivEnergy exactly, because a bank is precisely what this hardware succeeds or fails at — it serves a page whole or refuses it whole.

`bank_components()` does that, and `tests/.../test_per_bank_components_isolate_a_refused_bank` measures the cost: eleven components, **eleven reads** — byte-for-byte what a single pooled plan would have issued, because the banks are disjoint pages. The three served banks decode; the eight refused ones fail alone. The only real constraint is that you must poll them individually: putting them back in a `ComponentGroup` pools them into one plan, and one plan fails as a whole.

So: not a bug, and the workaround is free. What remains is a documentation gap — nothing says "size a component to your device's failure granularity, and don't group components that can fail independently". That is a non-obvious modelling rule, and the natural instinct (one component per device) is the wrong one for any device with capability-gated banks.

**Fix:** document the rule. If anything more, an opt-in `async_update(partial=True)` that sets a refused block's fields to `None` and reports the failed `ReadBlock`s would let a device be modelled per family *and* tolerate absent banks — but it is a convenience, not a necessity.

**4.5 sharpens this into a real constraint.** `DeviceRanges.merged` now coalesces maps that *touch*, so any component whose map passes through a `ComponentGroup` gets its banks joined into one run — and GivEnergy's banks all touch, because the pages are contiguous. Measured across the eleven families, that turned seven bank-aligned blocks into blocks spanning two banks (`HR(199,59)` crosses 239|240, `HR(258,60)` crosses 299|300, gateway `IR(1700,59)` crosses 1719|1720, and so on), for a net saving of **one request** — on the gateways, and nowhere else. Nothing in the captures shows this hardware answering a read across a bank boundary; the one refusal recorded at an unaligned base, `IR(236,60)` at `0x32`, is exactly that shape.

Neither escape hatch the library offers applies. A **deliberate gap** needs an address no field claims between two banks, and `HR(299)` and `HR(300)` are both modelled — the pages abut with fields on either side of the seam. **`max_span`** is a width cap, not an alignment rule; no value of it stops a 60-register block starting at 258. What does hold the split is that a component's *own* map is never coalesced, so `components_for()` stopped returning a `ComponentGroup` and `read_components()` drives each component's own plan. That restores the 4.4 request pattern exactly, for every family, whole and per bank, and `test_no_planned_block_crosses_a_bank_boundary` pins it.

**Fix:** a component-level way to say "this map's parts are separate reads" — the thing `register_ranges` meant before 4.5. A group that can only ever merge its members' maps cannot model a device whose readable pages are contiguous but independently served, which is most capability-gated hardware.

### c. ~~Blocks must start at a page boundary~~ — withdrawn, I was wrong

I originally claimed the planner's block *trimming* was a gap: GivEnergy's own traffic only ever asks for whole banks, so `HR(242,58)` where the client sends `HR(240,60)` looked unsafe, and I worked around it with placeholder `raw_register` fields at every range boundary.

The captures disagree. Decoding every recorded response and keeping the successful ones, the hardware serves `HR(1110,1)`, `HR(1112,1)`, `HR(1122,1)`, `HR(1120,5)`, `IR(1360,54)`, `IR(1840,20)`, `IR(300,21)` and a run of single-register reads from `IR(2044)` to `IR(2070)`. Arbitrary base, arbitrary count, within the 60-register cap. The only refusals are absent *banks* (`236,60`, `1100,60` — a three-phase range on a device that lacks it), which is about the bank existing, not about alignment.

So trimming is fine, the anchors are deleted, and the reads are now *narrower* than the client's own — `IR(60,56)` for a BMS instead of `IR(60,60)`, `IR(60,29)` for a meter instead of `IR(60,30)`. `test_the_hardware_serves_arbitrary_bases_and_counts` pins the evidence so the next person doesn't re-derive the same wrong conclusion from the same misleading traffic pattern.

**Nothing to fix.** Worth recording as the one place the migration nearly baked a false hardware assumption into the model, on the strength of "all observed traffic looks like X" — which is evidence about the *client*, not the *device*.

### d. `Component` is single-space, and a device usually isn't

`register_space` is one of `"holding"` or `"input"` per component, but 3 of 11 families here (both inverters and the EMS) declare registers in both. Each becomes *two* components — "an inverter" is two objects, for no reason the device would recognise. They were pooled by a `ComponentGroup` until 4.5; the group never saved a request, since the two members address different spaces and blocks are planned per space, and 4.5's range merging made it cost bank boundaries (see (b)), so the pooling is gone.

**Fix:** let a field carry its own space, defaulting to the component's. The planner already keys blocks by space; the change is in field declaration, not planning.

### e. There are only two register spaces

`RegisterSpace = Literal["input", "holding"]`. GivEnergy meters carry their identification block — serial, factory code, hardware and software versions — in a **third** register space under function code `0x16`. Seven fields, and they simply cannot be modelled; `MeterProduct` has no component and has to go through the raw PDU surface.

**Fix:** this one is genuinely awkward, since a vendor-specific space has no place in a generic Modbus model. But given the framework already parameterises reads by space, an escape hatch — a custom space with a reader callable supplied by the backend — would cost little and would close the last gap here.

### f. The params union is closed

Documented above. `ModbusParams` being a closed union means every third-party transport starts with a `type: ignore`. The base class only needs `endpoint`-ish behaviour and something `_target()` can format.

**Fix:** a `ModbusParams` Protocol (or ABC) instead of a union, so a backend's own params dataclass type-checks.

### g. Fields have no plausibility bounds

302 of our 1260 fields declare a min/max and decode to `None` outside it — a guard against a corruption pattern where library-side values appeared well outside physical range (#82). `nan` sentinels cover "this exact value means unset"; they don't cover "anything over 500 V is not a grid voltage".

**Fix:** `min_value`/`max_value` on `RegisterField`, out-of-range decoding to `None`. Ours also skips the check when the raw words are all zero, because an all-zero bank means "the hardware never populated this", not "out of range" — worth copying.

### h. A field is a contiguous span

`RegisterField` is `(address, count)` and `decode` gets that window. 16 of 1260 fields don't fit: 14 are 32-bit values whose high word sits at the *higher* address (a little-endian pair, which `word_order="little"` would actually cover), and 2 name two registers with a hole between them. `GivEnergyField` keeps per-register offsets and re-selects from the window, over-reading a register or two.

Low priority — the workaround is 5 lines and the over-read is free — but worth knowing that "a field is a set of registers" is a shape real devices have.

### i. ~~`restrict_fields` doesn't update `declared_fields`~~ — closed in 4.4

Narrowing a component left the public mapping advertising fields the instance no longer had, so anything introspecting the live field set had to read `_register_fields`.

4.4's `resolved_fields` is exactly the missing surface, and better than the `fields` property suggested here: it is per instance, narrowed by `restrict_fields`, and each entry carries where the field actually lands (absolute address, register count, scale register, space) rather than only the declared object. `modelled_fields()` and `restrict_to_banks()` are written on it, and the last private read into `Component` is gone. `declared_fields` still means the class's declared layout, which is the right thing for it to mean.

### j. No retry policy

The README's framing suggested one; there isn't one. Every consumer facing a flaky device writes the same attempt loop. Ours is ~60 lines and encodes something non-obvious — that firing a retry immediately lands it in the same silent window, so the delay is load-bearing. Not a blocker (retry policy is genuinely arguable), but if "retry policies" stays in the pitch, something should back it.

### k. Smaller notes

- **`message_spacing` is write-only.** The base class hands it to the `Pacer` and keeps no readable copy, so a backend that needs it for its own timeout arithmetic has to store its own.
- **No jitter.** `Pacer` gives an exact minimum gap. Coordinated bursts across consumers clump on that boundary; we kept an additive jitter of our own on top. A `jitter` param would fold that in.
- **`_RANGE_ATTR` maps holding and input to the same `register_ranges` name**, which the source itself notes is ambiguous. Fixing (d) fixes this too.
- **The 19-method Protocol is a lot of surface for a device with three function codes.** 16 of our 19 methods are one-line `IllegalFunctionError` raises. A mixin supplying those defaults would delete ~80 lines from every partial backend. (The review's suggestion to *trim* the surface was declined; a `PartialModbusUnit` base would get the same benefit without removing anything.)

### What was better than expected

Worth saying plainly, because it is most of the story:

- **`for_unit()` fits perfectly.** GivEnergy's device address byte *is* a Modbus unit id — inverter at `0x11`, BMSes at `0x32`–`0x36`, BCU stacks at `0x37`+, meters at `0x01`–`0x08`. The read planner addresses each device separately without knowing anything about GivEnergy.
- **`disconnect()` is exactly the right primitive.** The old code did `close()` then `connect()` to recycle a half-open link, with a leftover-resource check to stop the old pump tasks racing the new ones. `disconnect()` — drop the link, stay usable, reconnect on next request — replaced all of it, and it's *better*: it doesn't close a connection other consumers may be sharing.
- **Connect-on-demand deleted the whole reconnect dance.** The client no longer tracks whether it is connected.
- **The `Pacer` replaced the hand-rolled throttle cleanly**, and per-unit spacing came free.
- **The shared-connection model is right for this hardware.** The dongle *is* the bottleneck; two consumers each opening a socket is strictly worse. Making `Client` a consumer rather than an owner was the most clarifying change in the migration.
- **4.4's readable-range validation found nothing, and that is the point.** The planner now refuses a field its component's map cannot contain, at plan-build time. Every field here already fit, because the ranges are stated at bank granularity and a bank is what the device serves. What it did expose is that our own import-time check was the *weaker* of the two — it asked whether a field's addresses were readable, not whether they sat in one range, so a field straddling two adjacent banks would have passed here and failed at the first poll. `component_class` now applies the planner's rule instead, so a bad LUT edit still fails at import.
- **`MockModbusUnit` and the `ReadEvent` log made the equivalence tests easy** — seed it from a wire capture, run the real planner, compare against the pydantic model, and assert the exact blocks that got planned.
- **The converter vocabulary is richer than it looks.** Once `decode` is overridden, every one of this library's converters — scaled numbers, enums, bitfields, byte splits, time slots, datetimes, serials, fault-code bitmask lists — composes into it without fighting the framework.

### Was the Protocol seam sufficient?

**Yes, for the transport.** No library change was needed, or wanted, to make a custom framer with an embedded serial, its own CRC, an inbound heartbeat and a shape-hash correlator behave as a first-class `modbus-connection` backend. The survey's claim holds.

**With one caveat the survey didn't name:** the seam is sufficient for *request/response*, and GivEnergy dongles also volunteer frames. The heartbeat has to be answered, LAN-config broadcasts arrive unbidden, and a shared dongle delivers another consumer's register responses — all carrying data as current as our own. The Protocol has no room for any of it, so `GivEnergyConnection` grew `add_frame_listener()`. That is not a defect in the Protocol — it should stay request/response — but it does mean "implement `ModbusUnit` and you're done" is only true for devices that speak when spoken to. A backend for a chatty device will always need a second, native surface, and the docs should say so.

**The model framework: also yes, with one modelling rule.** 1253 of 1260 fields translate, and the planner's reads are narrower than the library's own hand-written ones. Two things I first called blockers weren't: (b) is a modelling rule — size a component to the device's failure granularity, and poll independently-failing components separately — and (c) was me mistaking the client's habits for the device's requirements. What's left is (d) through (i): real but small, and none of them stopped anything.

The pattern in both mistakes is worth naming: I generalised from what this library's *existing code* does, rather than from what the *device* accepts. The captures were sitting right there and settled both questions in one query each.
