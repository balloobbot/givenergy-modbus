# Migrating givenergy-modbus onto modbus-connection

A survey of ~65 real-world Modbus libraries classified this one **BLOCKED**: a custom "Transparent" framer with an embedded device serial and its own CRC, a mandatory dongle heartbeat, a transmit throttle, FC06-only writes, and last-good data served on a rejected read. The survey's answer to that class of library was that `modbus-connection`'s pure Protocol layer — `ModbusConnection` and the `ModbusUnit` Protocol, neither of which imports a backend — *is* the extension seam, and that such a library needs no library work at all.

This migration tests that claim. The verdict up front: **the transport seam holds completely, and the model framework holds for 1253 of 1260 register definitions.** Nothing in `modbus-connection` had to change. Three things had to be reached around, and a handful of gaps cost real code — all detailed below.

Where this landed:

| | |
|---|---|
| `givenergy_modbus/connection.py` | 890 lines — the transport as a `ModbusConnection` backend |
| `givenergy_modbus/model/components.py` | 506 lines — every device family as a `Component` |
| `givenergy_modbus/client/client.py` | −471 / +183 lines: the socket, the pump tasks and the retry loop moved out |
| tests | 1764 passing, including the model layer driven end to end over real framing |

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

There is no discovery function code. `detect()` probes candidate device addresses and banks, and infers topology from what answers and what stays silent — an all-zero response means "missing", an error response means "this bank isn't served here", silence means "nothing at this address". An AC-coupled inverter answers `HR(300-359)` and a hybrid times out on it; extended charge slots exist above a firmware threshold on some models only. **The readable address map is a runtime discovery, not a static fact** — which turns out to be the single biggest friction point with the model framework.

---

## 2. What internals of modbus-connection did you have to touch?

Short list, and none of it is fatal. Nothing was monkeypatched, nothing was vendored, and `modbus-connection` itself is unmodified.

### Subclassed as intended

`GivEnergyConnection(ModbusConnection)` implements the three documented hooks — `_connect_client`, `_close_client`, `for_unit`. `GivEnergyUnit` satisfies the `ModbusUnit` Protocol structurally; `isinstance(unit, ModbusUnit)` passes and all 19 methods are present. `GivEnergyField(RegisterField)` overrides `decode`, which the class documents as its only abstract method. That is three public extension points used exactly as designed, and they carried the whole migration.

### Reached around — four things

**`self._client`, read and written directly.** The base class treats `_client` as "the connected backend client", and `connected` is `_client is not None`. When the transport tells us the link died — reader EOF, a stalled drain, a socket error — a backend has to make `connected` go false and let the next request start a fresh connect flight. There is no hook for that. So `_note_lost()` sets `self._client = None` itself, and `_live_session()` clears it again if a session died inside the connect flight.

**`self._lost_callbacks.fire()`.** Same cause. `on_connection_lost()` is public for *subscribing*; there is no protected way for a backend to say the link went away.

**`self._pacer`.** The inherited `Pacer` is protected, which is fine for the connection itself — the producer wraps each wire write in `async with self._pacer.paced(unit_id)`. But `set_message_spacing` lives on the *unit*, and a unit handle is not a subclass of the connection, so `GivEnergyUnit` calls a `set_unit_spacing()` method added to the connection purely to bridge that gap.

**`Component._register_fields`.** `declared_fields` is the class's declared layout, and `restrict_fields()` does not narrow it — so after capability gating the public mapping still lists fields the instance no longer has. Reading the current field set means reading the private dict.

### One `type: ignore`

`ModbusConnection.__init__` types `params` as `ModbusTcpParams | ModbusUdpParams | ModbusTlsParams | ModbusSerialParams` — a closed union of the four transports the library ships. A third-party transport with its own params dataclass is precisely what the Protocol seam is for, so the union is too narrow by construction. Everything the base class actually *does* with the value works fine on ours; only the annotation objects. One suppressed line, at one construction site.

### Re-parented our own exception hierarchy

`givenergy_modbus.exceptions.ExceptionBase` now derives from `modbus_connection.ModbusError`, and `ConnectionLost` / `ConnectionFailed` are also `ModbusConnectionError`. Not strictly forced, but a backend that raises outside the library's hierarchy is not a drop-in backend. `ConnectionLost` now has three bases (`CommunicationError`, `ModbusConnectionError`, `TimeoutError`) so the historical `except TimeoutError` contract still holds.

### What we did *not* need

No fork, no vendored copy, no patched planner, no reimplemented `Component`. The retry loop, the heartbeat responder and the frame reassembly all live in our own module and the library never sees them — which is the seam working.

---

## 3. What could modbus-connection do better?

Ordered by how much each one cost.

### a. A backend has no way to say "the link died" — **the biggest gap**

Every custom transport that owns its own socket needs this, and every one will write the same three lines against private attributes:

```python
self._client = None
self._lost_callbacks.fire()
```

**Fix:** a protected `_connection_lost(exc)` on `ModbusConnection` that clears the client, fires the callbacks, and is a no-op during a deliberate `close()`/`disconnect()`. Two of those three behaviours we had to reimplement anyway (the third — not firing during a deliberate teardown — we got wrong first and fixed after a test hung). Give it a documented contract and every backend gets it right.

While you're there: a lost session's transport is never closed. `close()` works off `_client`, which is exactly what the loss cleared, so the socket leaks and the peer keeps its handler alive. We found this because a test hung in `Server.wait_closed()`. Whatever ships as `_connection_lost` should close the client it drops.

### b. Partial reads are all-or-nothing — **the biggest gap in the model layer**

`ReadPlan.execute` re-raises a refused block, and everything already read in that pass is discarded. On this hardware that is not an edge case: absent banks are *routine* — an inverter model that doesn't serve `HR(300-359)`, a three-phase bank on a single-phase unit, a battery slot with nothing in it. `tests/test_components_end_to_end.py::test_an_absent_bank_aborts_the_whole_update` pins the behaviour: the inverter's identity bank is read successfully and then thrown away because a later bank was refused.

This is finding **C** from the original review, marked "Not planned". Having now hit it with real hardware traces, it is the difference between the model layer being usable for a GivEnergy poll and not. The library already tolerates *per-field* failures; the gap is per-*block*.

**Fix:** let a refused block set its fields to `None` and continue, and report what failed. Something like `async_update(partial=True)` returning the refused `ReadBlock`s, or an `on_block_error` callback. Raising by default is a reasonable choice; raising with *no option* is not.

### c. `register_ranges` says what's readable, not where a block may start

GivEnergy answers in fixed pages. The dongle's own traffic only ever asks for a bank's whole width from its origin — `IR(60,60)`, `IR(60,30)`, `HR(240,60)`. The planner sizes a block to the fields inside it, so a page whose first *modelled* register is not its first register gets read from the wrong base: we got `HR(242,58)` where the hardware expects `HR(240,60)`, and `IR(1001,20)` where it expects `IR(1000,60)`.

`register_ranges` is exactly where "this device answers in fixed pages" belongs, and it only declares which addresses are readable. The workaround is a hack: an unused `raw_register` at each end of every range, purely to pull the block out to the page boundary. It works — the planned reads now match the client's own request pattern exactly — but 11 device families carry placeholder fields that exist to defeat an optimisation.

**Fix:** a `read_whole_range: bool` on `Component` (or a per-range flag), meaning "read each declared range in full rather than trimming to fields". Cheap to implement, and this is not an exotic device: any gateway or dongle that re-exposes a cached page has the same property.

### d. `Component` is single-space, and a device usually isn't

`register_space` is one of `"holding"` or `"input"` per component, but 3 of 11 families here (both inverters and the EMS) declare registers in both. Each becomes *two* components pooled by a `ComponentGroup` — which works correctly, and the range merging even does the right thing — but "an inverter" is now two objects and a group, for no reason the device would recognise.

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

### i. `restrict_fields` doesn't update `declared_fields`

Narrowing a component leaves the public mapping advertising fields the instance no longer has, so anything introspecting the live field set has to read `_register_fields`.

**Fix:** make `declared_fields` instance-aware after a restriction, or add a public `fields` property that reflects the current set.

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
- **`MockModbusUnit` and the `ReadEvent` log made the equivalence tests easy** — seed it from a wire capture, run the real planner, compare against the pydantic model, and assert the exact blocks that got planned.
- **The converter vocabulary is richer than it looks.** Once `decode` is overridden, every one of this library's converters — scaled numbers, enums, bitfields, byte splits, time slots, datetimes, serials, fault-code bitmask lists — composes into it without fighting the framework.

### Was the Protocol seam sufficient?

**Yes, for the transport.** No library change was needed, or wanted, to make a custom framer with an embedded serial, its own CRC, an inbound heartbeat and a shape-hash correlator behave as a first-class `modbus-connection` backend. The survey's claim holds.

**With one caveat the survey didn't name:** the seam is sufficient for *request/response*, and GivEnergy dongles also volunteer frames. The heartbeat has to be answered, LAN-config broadcasts arrive unbidden, and a shared dongle delivers another consumer's register responses — all carrying data as current as our own. The Protocol has no room for any of it, so `GivEnergyConnection` grew `add_frame_listener()`. That is not a defect in the Protocol — it should stay request/response — but it does mean "implement `ModbusUnit` and you're done" is only true for devices that speak when spoken to. A backend for a chatty device will always need a second, native surface, and the docs should say so.

**The model framework is a different answer: mostly yes, with (b) and (c) as the real blockers.** 1253 of 1260 fields translate, and the planned reads match the library's own request pattern exactly — but a component that can't tolerate a refused block can't poll a device whose banks depend on its model, and one that can't be told to read whole pages will ask a page-oriented device for the wrong base. Both are small, well-defined changes.
