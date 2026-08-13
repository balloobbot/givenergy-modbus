import asyncio
import logging
import re
import warnings
from asyncio import Future
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from modbus_connection import ModbusConnectionError, ModbusExceptionError

from givenergy_modbus.connection import Direction, GivEnergyConnection, GivEnergyParams
from givenergy_modbus.exceptions import (
    CommunicationError,
    ConnectionLost,
    InvalidPduState,
    PlantNotDetected,
    PlantTopologyMismatch,
    ReadFailure,
    RefreshFailed,
    RefreshPartiallySucceeded,
)
from givenergy_modbus.model import manifest
from givenergy_modbus.model.ems import EmsRegisterGetter
from givenergy_modbus.model.inverter import Model, resolve_model
from givenergy_modbus.model.lv_bcu import LV_BCU_ADDRESS
from givenergy_modbus.model.plant import (
    _COLD_LV_BATTERY_RANGE,
    _COLD_METER_RANGE,
    Plant,
    PlantCapabilities,
    _aio_module_candidates,
    _bcu_module_count,
    _bms_bcu_count,
    _derive_capabilities,
    _hv_bmu_candidates,
)
from givenergy_modbus.model.register import HR, IR
from givenergy_modbus.model.register_cache import RegisterCache
from givenergy_modbus.pdu import (
    ReadHoldingRegistersRequest,
    ReadInputRegistersRequest,
    TransparentRequest,
    TransparentResponse,
)
from givenergy_modbus.pdu.base import BasePDU
from givenergy_modbus.pdu.write_registers import INSTALLER_WRITE_REGISTERS, WriteHoldingRegisterRequest

_logger = logging.getLogger(__name__)

# Standard GE 10-char serial in the textual / decoded form — `[A-Z]{2}\d{4}[A-Z]\d{3}`,
# matches both real serials (e.g. ``CE2231G454``) and the CLI-redacted form
# (``CE0000G000``). Used to sanity-check serial strings decoded out of the EMS rollup.
_GE_SERIAL_STR_PATTERN = re.compile(r"^[A-Z]{2}\d{4}[A-Z]\d{3}$")

# A read the device did not serve: it either stayed silent (``TimeoutError``) or refused
# outright with an error response (``ModbusExceptionError``). Detection treats both as
# "nothing here" — only the transport cares which, and it already distinguishes them.
# ``ConnectionLost`` is a ``TimeoutError``, so handlers that must not swallow a dead link
# still catch it first.
DEVICE_DID_NOT_ANSWER = (TimeoutError, ModbusExceptionError)


# Serial-register groups (which register addresses carry serial values) come from the
# single canonical builder, register_cache._get_serial_groups() — the same set
# RegisterCache.redact_serials() uses. FrameRedactor previously kept its own builder,
# which diverged: it omitted the manually-decoded BMU serial groups (plus identifier=True
# fields and dynamic module discovery), silently leaking HV per-module serials from
# captured HV stacks (#375). One source of truth now — see _redact_frame.

# MBAP start marker used by the framer to locate frames within a byte stream.
_FRAME_MARKER = bytes.fromhex("59590001")

# Floor for the "producer hasn't sent our frame yet" safety-net timeout. The actual wait
# also scales with the queue backlog (see send_request_and_await_response); this floor keeps
# it sane when the queue is idle. Module-level so tests can shrink it.
_FRAME_SENT_MIN_TIMEOUT = 5.0

# Upper bound on writer.drain() inside the producer loop (#356). On a healthy link
# drain completes near-instantly (it's local socket-buffer backpressure); a stall
# means the peer stopped ACKing — a half-open connection — so it's treated as
# connection loss rather than left to wedge the producer until close() (hass#233).
_DRAIN_TIMEOUT = 10.0

# Re-warn cadence for the coalesced reader-EOF reconnect churn. A marginal dongle that
# idle-reaps the TCP connection every ~10s (hass#95) would otherwise emit thousands of
# 'connection lost (reader at EOF)' WARNING lines/day on a setup that is working fine —
# each drop transparently recovers. The first drop warns; subsequent drops within this
# window are demoted to DEBUG; one re-warn per window carries the running tally.
_EOF_REWARN_SECONDS = 300.0


class FrameRedactor:
    """Frame-aware stateful redactor for a captured GivEnergy byte stream.

    Replaces ``StreamRedactor``: instead of running byte-level regex over raw socket
    chunks, it reassembles complete GivEnergy frames (using the same 0x5959 marker
    scan the ``Framer`` uses), decodes each one, redacts only the known-sensitive
    fields by type (envelope serials, C.serial-tagged register values, LAN-config IPs),
    and re-encodes with a freshly-computed CRC.

    Any bytes that cannot be decoded — ``InvalidFrame`` results, inter-frame garbage,
    or a partial frame held at stream end — are emitted **intact** (not mangled) with
    a log message.  Nothing on the wire is ever dropped: the capture is always complete.

    Not thread-safe; use one instance per capture direction.

    See #158 B-3 for the design rationale and the ``LanConfigBroadcast`` PDU that
    handles the #100 WO-dongle LAN-config broadcasts.
    """

    def __init__(self, direction: "Direction" = "rx") -> None:
        self._buf = b""
        self._direction = direction  # determines which PDU decoder to use

    def feed(self, chunk: bytes) -> bytes:
        """Absorb raw bytes; return redacted output for any complete frames found."""
        self._buf += chunk
        return self._process()

    def flush(self) -> bytes:
        """Emit any remaining buffered bytes intact and reset. Call at stream end."""
        tail = self._buf
        self._buf = b""
        if tail:
            _logger.debug("FrameRedactor flushing %db of incomplete/trailing bytes intact", len(tail))
        return tail

    def _process(self) -> bytes:
        out = b""
        while self._buf:
            marker_pos = self._buf.find(_FRAME_MARKER)
            if marker_pos < 0:
                # No frame marker in buffer — keep the last 3 bytes (a split marker
                # could arrive next chunk) and emit the rest intact.
                keep = len(_FRAME_MARKER) - 1
                if len(self._buf) > keep:
                    garbage, self._buf = self._buf[:-keep], self._buf[-keep:]
                    _logger.debug("FrameRedactor: %db pre-marker garbage emitted intact", len(garbage))
                    out += garbage
                break
            if marker_pos > 0:
                # Garbage before the marker — emit intact.
                garbage, self._buf = self._buf[:marker_pos], self._buf[marker_pos:]
                _logger.debug("FrameRedactor: %db inter-frame garbage emitted intact", len(garbage))
                out += garbage
                continue
            # Marker is at position 0. Read the length field to know frame size.
            if len(self._buf) < 6:
                break  # not enough bytes for the MBAP length field yet
            hdr_len = int.from_bytes(self._buf[4:6], "big")
            if hdr_len > 300:
                # A real frame's MBAP length never exceeds ~300 (60-register cap). A larger
                # value means this marker is a false positive (random bytes that happen to
                # match) — emit it intact as garbage and resume scanning, rather than buffering
                # up to ~64 KB for a frame that will never complete. Mirrors framer.py's guard.
                skip = len(_FRAME_MARKER)
                garbage, self._buf = self._buf[:skip], self._buf[skip:]
                _logger.debug("FrameRedactor: false marker (len=0x%04x), %db emitted intact", hdr_len, len(garbage))
                out += garbage
                continue
            frame_len = 6 + hdr_len
            if len(self._buf) < frame_len:
                break  # partial frame — wait for more data
            frame, self._buf = self._buf[:frame_len], self._buf[frame_len:]
            out += self._redact_frame(frame)
        return out

    def _redact_frame(self, frame: bytes) -> bytes:
        from givenergy_modbus.model.register import Converter
        from givenergy_modbus.model.register_cache import _get_serial_groups
        from givenergy_modbus.pdu import ClientIncomingMessage, ClientOutgoingMessage
        from givenergy_modbus.pdu.lan_config import LanConfigBroadcast
        from givenergy_modbus.pdu.read_registers import ReadRegistersResponse

        # TX frames are ClientOutgoingMessage (requests); RX frames are
        # ClientIncomingMessage (responses/heartbeats).  Using the wrong decoder
        # silently falls through to intact-passthrough, leaking the adapter serial
        # in every captured request.  Pass the right decoder by direction.
        decoder_class = ClientOutgoingMessage if self._direction == "tx" else ClientIncomingMessage
        try:
            pdu = decoder_class.decode_bytes(frame)
        except Exception:
            _logger.warning("FrameRedactor: undecodable frame (%db) emitted intact", len(frame))
            return frame

        # LanConfigBroadcast: delegate to its own redact() — handles serial + IPs
        if isinstance(pdu, LanConfigBroadcast):
            return pdu.redact().encode()

        # Redact envelope serials (present on all Transparent PDUs). FAIL-CLOSED
        # (redact_serial_strict): an envelope value that doesn't parse as a GE serial is
        # blanked, not passed through. The Gateway's write-response envelopes carry a
        # NUL-interrupted serial variant that fails the pattern, and the fail-open
        # redact_serial leaked its real unit digits verbatim (first write-path capture,
        # 2026-07-09). Unlike the register-payload path below — which must fail open
        # because serial groups overlap ordinary data — the envelope is unambiguous,
        # so it takes the same strict treatment Plant.redact() applies to these exact
        # fields (#212/#214).
        if hasattr(pdu, "data_adapter_serial_number"):
            pdu.data_adapter_serial_number = Converter.redact_serial_strict(pdu.data_adapter_serial_number)
        if hasattr(pdu, "inverter_serial_number"):
            pdu.inverter_serial_number = Converter.redact_serial_strict(pdu.inverter_serial_number)

        # Redact payload serials in register responses.
        # A serial is stored across 5 consecutive registers; decode the group as a
        # string, apply redact_serial, and re-encode back into register values.
        if isinstance(pdu, ReadRegistersResponse) and not pdu.error:
            reg_type = "HR" if pdu.transparent_function_code == 3 else "IR"
            win_base = pdu.base_register
            win_end = win_base + len(pdu.register_values)  # safer than register_count
            for g_type, g_base, g_count in _get_serial_groups():
                if g_type != reg_type:
                    continue
                g_end = g_base + g_count
                if g_base < win_base or g_end > win_end:
                    continue  # group not fully within this response window
                offset = g_base - win_base
                raw_bytes = b"".join(v.to_bytes(2, "big") for v in pdu.register_values[offset : offset + g_count])
                serial_str = raw_bytes.decode("latin1").replace("\x00", "").upper()
                redacted = Converter.redact_serial(serial_str)
                if redacted is None or redacted == serial_str:
                    # Not a recognised serial — leave unchanged. Mirrors
                    # RegisterCache.redact_serials: rewriting a passthrough value would
                    # zero-pad and corrupt an overlapping group (e.g. the battery serial
                    # group IR(110-114) reading register 114 of a BMU serial at 114-118).
                    continue
                # Re-encode: right-pad to g_count*2 bytes, split back into registers
                redacted_bytes = redacted.encode("latin1").ljust(g_count * 2, b"\x00")[: g_count * 2]
                for i in range(g_count):
                    pdu.register_values[offset + i] = int.from_bytes(redacted_bytes[i * 2 : i * 2 + 2], "big")

        return pdu.encode()


@dataclass(frozen=True)
class ProbeRange:
    """A single Modbus read to issue during detect, with its timeout tier.

    ``tier="known"`` → full ``timeout``/``retries``; ``tier="probe"`` → fast
    ``probe_timeout``/``probe_retries`` and ``retry_delay=0``.
    """

    reg_type: str  # "HR" or "IR"
    device_address: int
    base_register: int
    register_count: int
    tier: str  # "known" | "probe"


def _strategise(
    caps: PlantCapabilities,
    prior: PlantCapabilities | None,
    step: str,
) -> list[ProbeRange]:
    """Pure: return the ProbeRanges for one detect step given current caps and prior hint.

    Calls the same candidate helpers as ``_derive_capabilities`` so candidate generation
    has one implementation.  No I/O.
    """
    ranges: list[ProbeRange]

    if step == "aio_modules":
        num = caps.bcu_stacks[0][1] if caps.bcu_stacks else 0
        addrs: list[int] | range = (
            list(prior.aio_battery_module_addresses) if prior is not None else _aio_module_candidates(num)
        )
        ranges = [ProbeRange("IR", addr, 60, 60, "probe") for addr in addrs]

    elif step == "hv_bmus":
        if not (caps.is_hv and caps.device_type is not Model.ALL_IN_ONE and caps.bcu_stacks):
            ranges = []
        else:
            addrs = (
                list(prior.hv_bmu_addresses)
                if prior is not None and prior.hv_bmu_addresses
                else _hv_bmu_candidates(caps.bcu_stacks)
            )
            ranges = [ProbeRange("IR", addr, 60, 60, "probe") for addr in addrs]

    elif step == "meters":
        addrs = prior.meter_addresses if prior is not None else _COLD_METER_RANGE
        ranges = [ProbeRange("IR", addr, 60, 30, "probe") for addr in addrs]

    elif step == "lv_bcu":
        addr = prior.lv_bcu_address if prior is not None else LV_BCU_ADDRESS
        if addr is None:
            ranges = []
        else:
            ranges = [ProbeRange("IR", addr, 60, 60, "probe")]

    else:
        raise ValueError(f"_strategise: unknown step {step!r}")

    _logger.debug(
        "_strategise(%s, prior=%s): %d range(s) → %s",
        step,
        "hinted" if prior is not None else "cold",
        len(ranges),
        [(f"0x{r.device_address:02x}", r.reg_type, r.base_register, r.register_count) for r in ranges],
    )
    return ranges


def _request_for_range(r: manifest.RegisterRange, device_address: int) -> TransparentRequest:
    """Build the read request for one RegisterRange at device_address."""
    if r.reg_type == "HR":
        return ReadHoldingRegistersRequest(
            base_register=r.base_register, register_count=r.register_count, device_address=device_address
        )
    if r.reg_type == "IR":
        return ReadInputRegistersRequest(
            base_register=r.base_register, register_count=r.register_count, device_address=device_address
        )
    raise ValueError(f"Unsupported RegisterRange.reg_type: {r.reg_type!r}")


def _refresh_banks(caps: PlantCapabilities) -> list[tuple[int, int, int]]:
    """Return (device_address, base_register, register_count) for every IR bank to poll."""
    inverter = caps.inverter_address
    banks: list[tuple[int, int, int]] = []
    if not caps.is_ems:
        banks += [(inverter, 0, 60), (inverter, 180, 60)]
    banks += [
        (inverter, r.base_register, r.register_count)
        for r in manifest.gated_ranges(manifest.REFRESH_IR_RANGES, caps.device_type, caps.arm_firmware_version)
    ]
    for addr in caps.lv_battery_addresses:
        banks.append((addr, 60, 60))
    if caps.lv_bcu_address is not None:
        banks.append((caps.lv_bcu_address, 60, 60))
    for addr in caps.meter_addresses:
        banks.append((addr, 60, 30))
    for offset, _ in caps.bcu_stacks:
        banks.append((0x70 + offset, 60, 60))
    for addr in caps.aio_battery_module_addresses:
        banks.append((addr, 60, 60))
    for addr in caps.hv_bmu_addresses:
        banks.append((addr, 60, 60))
    return banks


def _refresh_ranges(
    caps: PlantCapabilities,
    max_age: float | None,
    plant: Plant,
    *,
    now: datetime | None = None,
) -> list[TransparentRequest]:
    """Return the TransparentRequests for one refresh cycle, skipping absent and fresh banks.

    A bank that detect marked ABSENT (``plant.block_present()`` is False) is skipped
    unconditionally — the presence marker is a stronger, cheaper signal than a timeout,
    so a known-absent device is never re-solicited (call ``detect()`` or
    ``invalidate_presence()`` to recheck). Of the remaining banks, when ``max_age`` is
    set any whose ``plant.block_age()`` is not None and ≤ ``max_age`` seconds is also
    omitted. With ``max_age`` None and no absent banks every bank is included
    (bit-identical to the pre-#268 behaviour). No I/O.
    """
    reqs: list[TransparentRequest] = []
    for addr, base, count in _refresh_banks(caps):
        if plant.block_present(addr, "IR", base, count) is False:
            _logger.debug("refresh: skipping IR(%d,%d)@0x%02x — detect marked it absent", base, count, addr)
            continue
        if max_age is not None:
            age = plant.block_age(addr, "IR", base, count, now=now)
            if age is not None and age <= max_age:
                _logger.debug(
                    "refresh: skipping IR(%d,%d)@0x%02x — %.1fs ≤ %.1fs max_age",
                    base,
                    count,
                    addr,
                    age,
                    max_age,
                )
                continue
        reqs.append(ReadInputRegistersRequest(base_register=base, register_count=count, device_address=addr))
    return reqs


class Client:
    """Asynchronous client for talking to a GivEnergy plant over Modbus TCP.

    The client is a *consumer* of a :class:`~givenergy_modbus.connection.GivEnergyConnection`,
    not the owner of a socket. That split is ``modbus-connection``'s model: one
    physical link addresses many units, and sharing a single internally-serialised
    connection between consumers beats each opening its own competing socket to a
    dongle that is already the bottleneck. Build the connection once and hand it
    to whoever needs it::

        connection = GivEnergyConnection(GivEnergyParams(host="192.168.1.50"))
        client = Client(connection)

    :meth:`for_host` is the shorthand for the single-consumer case; the client it
    returns owns its connection and closes it on :meth:`close`.

    The link is established on demand — the first request connects — so
    :meth:`connect` is only needed to establish it eagerly. All public methods
    are coroutines and assume they're awaited from the same asyncio event loop.

    Concurrency contract
    --------------------

    The client is designed to be used from multiple concurrent callers — e.g. a
    polling loop calling ``refresh_plant()`` and entity-write handlers calling
    ``one_shot_command()`` independently. The following invariants hold:

    **Safe to interleave**

    - Reads (``refresh_plant``, ``load_config``, ``refresh``) and writes
      (``one_shot_command``) may run concurrently. Their request/response pairs
      occupy disjoint shape-hash spaces, so they never collide in the in-flight
      tracking dict.
    - The connection's transmit queue is a FIFO drained by a single producer task
      with rate limiting between frames; bytes from one frame never interleave
      with another. A queued frame whose response future is already done (i.e.
      resolved by a late arrival from a previous attempt) is skipped at dequeue
      time rather than written to the wire, so retry storms don't duplicate work
      the inverter has already done.
    - Incoming frames are reassembled and dispatched serially by the connection's
      consumer task, so register-cache mutations are applied one PDU at a time.

    **Must be serialised**

    - ``detect()`` mutates ``plant.capabilities`` (including in-place appends to
      its address lists) and must not run concurrently with anything that reads
      those fields — most importantly ``refresh()`` and ``load_config()``.
      In typical use ``detect()`` runs once at connect time before the polling
      loop starts, which satisfies this naturally. Downstream consumers caching
      capabilities across restarts can bypass ``detect()`` on reconnect entirely.

    **Practical guidance for downstream consumers**

    - Take a per-client lock around ``refresh_plant()`` so successive polls don't
      overlap. Writes don't need the same lock — they're free to land between
      polls.
    - Connection loss is surfaced three ways: ``self.connected`` flips to
      ``False``, the connection logs a WARNING, and every in-flight or
      subsequently attempted request raises ``ConnectionLost`` (a
      ``CommunicationError`` that is also a ``ModbusConnectionError`` and a
      ``TimeoutError``, so legacy ``except TimeoutError`` handling keeps working
      — catch ``ConnectionLost`` first to distinguish reconnect-me from a genuine
      stall). Recovery needs no explicit step: the next request reconnects.
      ``connection.disconnect()`` forces a fresh link for a peer that holds the
      socket open but stops answering.
    """

    plant: Plant
    connection: GivEnergyConnection

    _capture_sink: Callable[[Direction, bytes], None] | None = None
    # Per-direction stream redactors for an active capture — carry a small tail
    # across socket-read chunks so a serial split across a boundary is still
    # redacted (#117). Created in capture_frames(), None when no capture runs.
    _capture_redactor_rx: "FrameRedactor | None" = None
    _capture_redactor_tx: "FrameRedactor | None" = None

    def __init__(
        self,
        connection: GivEnergyConnection,
        *,
        plant: Plant | None = None,
        splice_heal_seconds: float | None = None,
        splice_reject_heal_seconds: float | None = None,
    ) -> None:
        """Drive the plant reachable over ``connection``.

        The connection is not owned: :meth:`close` leaves it open for its other
        consumers. Use :meth:`for_host` when this client is the only consumer.
        """
        self.connection = connection
        self._owns_connection = False
        # ``plant`` is for single-owner pre-built plants only (e.g. restoring a
        # persisted PlantCapabilities). Do NOT share one Plant across two active
        # Clients: both call plant.update() into the same register_caches, and
        # two devices that answer at the same Modbus address (e.g. EMS + direct
        # inverter both at 0x11) will overwrite each other's cache. The safe
        # multi-Client path is separate Plants + plant.add_direct_source().
        self.plant = plant if plant is not None else Plant()
        # How long to hold last-good for a disputed *constant* battery register (num_cells,
        # bms_firmware_version) before healing to a sustained new value (#286). Applied only when
        # explicitly given, so an injected plant's own splice_heal_seconds isn't silently clobbered;
        # otherwise the Plant field's own default (900 s) stands. Larger = more robust against
        # ongoing splice corruption (which reverts in minutes); smaller = faster recovery from a
        # genuinely poisoned cold-start baseline. Consumers can watch plant.splice_held_count +
        # plant.block_age() to see when data is being held.
        if splice_heal_seconds is not None:
            self.plant.splice_heal_seconds = splice_heal_seconds
        # Opt-in recovery for a sustained *legitimate* >=2-physics battery step — the near-full-SOC
        # charge knee, which otherwise hard-rejects and freezes telemetry until it settles (#299).
        # None (default) leaves it disabled (the >=2-physics reject stays terminal); a float (e.g.
        # 300) enables the heal with that time bound. Off by default because the positive path can't
        # be validated against the existing corpus — opt in on a pack that tops out regularly.
        # Applied only when explicitly given, so an injected plant's own value isn't clobbered.
        if splice_reject_heal_seconds is not None:
            self.plant.splice_reject_heal_seconds = splice_reject_heal_seconds
        # Every decoded frame the connection sees is ingested, not only the answers
        # to this client's own requests. A GivEnergy dongle volunteers register
        # responses (its heartbeat traffic, and — when another consumer shares the
        # link — that consumer's replies), and those carry data that is just as
        # current as our own. The request/response Protocol has no room for them,
        # so the connection hands them out here.
        self._unsubscribe_frames = connection.add_frame_listener(self._ingest_frame)

    @classmethod
    def for_host(
        cls,
        host: str,
        port: int = 8899,
        *,
        connect_timeout: float = 2.0,
        tx_message_wait: float = 0.25,
        tx_jitter: float = 0.1,
        plant: Plant | None = None,
        splice_heal_seconds: float | None = None,
        splice_reject_heal_seconds: float | None = None,
    ) -> "Client":
        """Build a client that owns a fresh connection to ``host``.

        The shorthand for the single-consumer case. ``tx_message_wait`` is the
        minimum gap between consecutive frames on the wire, empirically
        load-bearing across hardware generations (#71); ``tx_jitter`` bounds the
        additive random jitter on top of it, which disperses coordinated bursts
        so they don't clump at fixed 250 ms boundaries. The jitter is asymmetric
        by design — it only ever lengthens the gap; set it to 0 to disable.
        """
        client = cls(
            GivEnergyConnection(
                GivEnergyParams(host=host, port=port),
                timeout=connect_timeout,
                message_spacing=tx_message_wait,
                tx_jitter=tx_jitter,
            ),
            plant=plant,
            splice_heal_seconds=splice_heal_seconds,
            splice_reject_heal_seconds=splice_reject_heal_seconds,
        )
        client._owns_connection = True
        return client

    @property
    def connected(self) -> bool:
        """Whether the underlying link is up."""
        return self.connection.connected

    @property
    def host(self) -> str:
        """Host name or IP address of the dongle."""
        return self.connection.host

    @property
    def port(self) -> int:
        """TCP port of the dongle's Modbus server."""
        return self.connection.port

    def _ingest_frame(self, pdu: BasePDU) -> None:
        """Commit a decoded register response to the plant's caches."""
        if isinstance(pdu, TransparentResponse):
            self.plant.update(pdu)

    async def connect(self) -> None:
        """Establish the link eagerly.

        Optional: the connection is established on demand by the first request.
        Call this to fail fast at startup, or to pay the connect cost before a
        latency-sensitive first poll. A no-op when already connected.
        """
        await self.connection.connect()

    async def close(self) -> None:
        """Release this client's hold on the connection.

        A client built with :meth:`for_host` owns its connection and closes it.
        A client handed a connection does not: other consumers may still be using
        it, and closing a shared link out from under them is the failure mode the
        shared-connection model exists to prevent. Close it yourself when done.
        """
        self._unsubscribe_frames()
        if self._owns_connection:
            await self.connection.close()

    async def __aenter__(self) -> "Client":
        """Enter a client context; the link is established on first use."""
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Release this client's hold on the connection (see :meth:`close`)."""
        await self.close()

    async def _probe(self, request: TransparentRequest, timeout: float, retries: int) -> bool:
        """Send a request; return True if the device answered, False otherwise.

        Both outcomes count as "absent": silence (a timeout) and an explicit
        refusal (an error response, which surfaces as ``IllegalDataAddressError``).
        Uses ``retry_delay=0`` so absent-device probes don't pay the silent-
        window-survival cost — detect() does many of these and most are
        expected to fail.

        A dead link is not an absent device, so ``ModbusConnectionError``
        propagates rather than answering False. Callers latch that False as a
        permanent ``mark_absent`` that ``refresh()`` then skips every cycle, so
        swallowing a link failure here would silently amputate the topology —
        an HV plant whose connection dropped mid-detect came back as an inverter
        with no BCUs, no modules and no meters, and stayed that way until the
        next detect(). Letting it out instead leaves capabilities unset, which
        detect()'s teardown turns into a clean retry (#274).
        """
        try:
            await self.send_request_and_await_response(
                request, timeout=timeout, retries=retries, retry_delay=0, warn_timeout=False
            )
            return True
        except ModbusConnectionError:
            # Before DEVICE_DID_NOT_ANSWER: ConnectionLost is also a TimeoutError.
            raise
        except DEVICE_DID_NOT_ANSWER:
            return False

    async def _probe_ranges(
        self,
        ranges: list[ProbeRange],
        timeout: float,
        retries: int,
        probe_timeout: float,
        probe_retries: int,
    ) -> None:
        """Issue each ProbeRange in order by tier; mark_absent on probe-tier failures."""
        for pr in ranges:
            req_cls = ReadHoldingRegistersRequest if pr.reg_type == "HR" else ReadInputRegistersRequest
            request = req_cls(
                base_register=pr.base_register,
                register_count=pr.register_count,
                device_address=pr.device_address,
            )
            if pr.tier == "known":
                await self.send_request_and_await_response(request, timeout=timeout, retries=retries)
            else:
                ok = await self._probe(request, timeout=probe_timeout, retries=probe_retries)
                _logger.debug(
                    "_probe_ranges: 0x%02x %s(%d,%d) → %s",
                    pr.device_address,
                    pr.reg_type,
                    pr.base_register,
                    pr.register_count,
                    "present" if ok else "absent",
                )
                if not ok:
                    self.plant.mark_absent(pr.device_address, pr.reg_type, pr.base_register, pr.register_count)
                    self.plant.register_caches.pop(pr.device_address, None)

    async def _detect_bcu_stacks(
        self,
        caps: PlantCapabilities,
        prior: PlantCapabilities | None,
        probe_timeout: float,
        probe_retries: int,
    ) -> None:
        """Populate caps.bcu_stacks. Hinted mode trusts prior layout; cold mode reads BMS at 0xA0."""
        if prior is not None:
            # Hinted: probe each previously-seen BCU and record what the BCU actually
            # reports for its module count (rather than trusting `prior`). The probe
            # populates IR(60–64) into the register cache; IR(64) is the BCU's own
            # module count. Letting actual values flow into `caps` here means a
            # change in stack composition is surfaced by the subsequent comparison
            # against `prior` rather than silently accepted. BMS read at 0xA0 is
            # skipped entirely — prior already tells us which BCUs to look at.
            for offset, _stored_modules in prior.bcu_stacks:
                if await self._probe(
                    ReadInputRegistersRequest(base_register=60, register_count=5, device_address=0x70 + offset),
                    timeout=probe_timeout,
                    retries=probe_retries,
                ):
                    bcu_cache = self.plant.register_caches.get(0x70 + offset, RegisterCache())
                    actual_modules = _bcu_module_count(bcu_cache)
                    caps.bcu_stacks.append((offset, actual_modules))
                else:
                    self.plant.mark_absent(0x70 + offset, "IR", 60, 5)
                    self.plant.register_caches.pop(0x70 + offset, None)
            return

        # Cold path: ask the BMS how many BCUs exist, then probe each.
        # 0xA0 is the BMS device address; IR(61) holds the number of BCUs present.
        if not await self._probe(
            ReadInputRegistersRequest(base_register=60, register_count=5, device_address=0xA0),
            timeout=probe_timeout,
            retries=probe_retries,
        ):
            self.plant.mark_absent(0xA0, "IR", 60, 5)
            self.plant.register_caches.pop(0xA0, None)
            return
        bms_cache: RegisterCache = self.plant.register_caches.get(0xA0, RegisterCache())
        num_bcus = _bms_bcu_count(bms_cache)
        for i in range(num_bcus):
            if await self._probe(
                ReadInputRegistersRequest(base_register=60, register_count=60, device_address=0x70 + i),
                timeout=probe_timeout,
                retries=probe_retries,
            ):
                bcu_cache = self.plant.register_caches.get(0x70 + i, RegisterCache())
                num_modules = _bcu_module_count(bcu_cache)
                caps.bcu_stacks.append((i, num_modules))
            else:
                self.plant.mark_absent(0x70 + i, "IR", 60, 60)
                self.plant.register_caches.pop(0x70 + i, None)

    #: Maximum number of battery modules on a single-BCU AIO (addresses 0x50–0x53).
    _AIO_MAX_MODULES = 4

    async def _ems_rollup_cross_check(self, timeout: float, retries: int) -> None:
        """Read IR(2040,55) at detect time and sanity-check the per-managed-inverter rollup.

        Populating the rollup during discovery means consumers don't need to
        wait for the first refresh cycle to see per-managed-inverter and
        per-meter data. The sanity check catches malformed rollups (or
        parser regressions) early.

        Best-effort end-to-end: a timeout on the read, or any anomaly during
        validation, only logs a warning — discovery never fails on this soft
        data check. See #95.
        """
        try:
            await self.send_request_and_await_response(
                ReadInputRegistersRequest(base_register=2040, register_count=55, device_address=0x11),
                timeout=timeout,
                retries=retries,
            )
        except DEVICE_DID_NOT_ANSWER:
            _logger.warning("detect: EMS rollup read at IR(2040,55) went unanswered — skipping cross-check")
            return
        self._validate_ems_rollup()

    def _validate_ems_rollup(self) -> None:
        """Sanity-check the EMS IR(2040,55) rollup decoded into the inverter's register cache.

        Logs warnings for any anomaly (no data, decode failure, implausible
        ``inverter_count``, malformed serial strings) but never raises —
        ``detect()`` shouldn't fail discovery on a soft data check. The
        intent is to surface parser regressions early without breaking the
        rest of the discovery flow.
        """
        cache = self.plant.register_caches.get(0x11)
        # EMS data is served at 0x11 (the rollup read above targets it, and Step 1's
        # HR(0,60) read populated the same cache). `cache is None` is therefore
        # unreachable in practice — the meaningful check is whether the rollup's IR
        # registers actually landed. `RegisterCache` is a defaultdict returning 0 for
        # missing keys, so without this guard a silently-failed rollup read would decode
        # as inverter_count=0 and mis-fire the implausible-count warning.
        if cache is None or IR(2040) not in cache:
            _logger.warning("detect: EMS rollup read returned no data at 0x11 — skipping cross-check")
            return
        try:
            ems = EmsRegisterGetter(cache).build()
        except Exception as e:  # noqa: BLE001 — best-effort sanity check, log and move on
            _logger.warning("detect: EMS rollup decode failed during cross-check: %s", e)
            return
        inverter_count = ems.get("inverter_count")
        if inverter_count is None or not (0 < inverter_count <= 4):
            _logger.warning(
                "detect: EMS rollup reports implausible inverter_count=%r (expected 1..4)",
                inverter_count,
            )
            return
        serials: list[str | None] = []
        for i in range(1, inverter_count + 1):
            raw = ems.get(f"inverter_{i}_serial_number")
            # Decoded serial fields can carry trailing NUL or space padding when the
            # underlying registers were partially populated; strip before matching so
            # a padded-but-valid serial doesn't fire a false warning.
            cleaned = raw.strip("\x00 ") if isinstance(raw, str) else raw
            serials.append(cleaned)
            if not (isinstance(cleaned, str) and _GE_SERIAL_STR_PATTERN.fullmatch(cleaned)):
                _logger.warning(
                    "detect: EMS rollup inverter_%d_serial_number=%r doesn't match GE serial format",
                    i,
                    cleaned,
                )
        # Decoded serials carry identifying information; keep them out of INFO-level
        # application logs to stay consistent with the wire-capture redaction posture
        # (`redact()` / PR #99). The per-slot WARNING already surfaces anomalies.
        _logger.debug(
            "detect: EMS rollup cross-check — inverter_count=%d, serials=[%s]",
            inverter_count,
            ", ".join(repr(s) for s in serials),
        )

    async def detect(
        self,
        timeout: float = 2.0,
        retries: int = 3,
        probe_timeout: float = 0.5,
        probe_retries: int = 1,
        prior: PlantCapabilities | None = None,
    ) -> PlantCapabilities:
        """Discover device type and peripheral topology.

        Reads HR(0) and HR(21) from the inverter to resolve the model, then
        probes for BCUs (HV systems), meters, and LV battery devices.

        Both returns the PlantCapabilities instance and assigns it to
        `self.plant.capabilities` — the returned object and the one stored on
        the plant are the same. Subsequent calls to Client.refresh() and
        Client.load_config() will use it automatically.

        When `prior` is supplied, the probe sweep restricts itself to the
        addresses listed in it — empty addresses from a cold sweep are skipped.
        If reality doesn't match prior (device_type changed, or any hinted
        address fails to confirm), raises PlantTopologyMismatch and leaves
        `self.plant.capabilities` as None. The exception carries `prior` and
        `actual` so callers can decide whether to retry, fall back to a cold
        detect(), or surface the change to the user.

        Uses a two-tier timeout: `timeout`/`retries` for the known inverter device
        (where a response is expected), and `probe_timeout`/`probe_retries` for
        speculative probes where absence is the common case.

        On a connection-level failure (TimeoutError / CommunicationError) the
        connection is torn down via close(), so connect()+detect() is atomic:
        `connected` flips to False and the standard "reconnect if not connected"
        idiom recovers (#274). A PlantTopologyMismatch is raised on a healthy
        connection (only the hint was wrong) and leaves it up so the caller can
        retry a cold detect().
        """
        try:
            return await self._detect(
                timeout=timeout,
                retries=retries,
                probe_timeout=probe_timeout,
                probe_retries=probe_retries,
                prior=prior,
            )
        except PlantTopologyMismatch:
            # Healthy connection — only the hint was wrong; capabilities already cleared.
            raise
        except (*DEVICE_DID_NOT_ANSWER, CommunicationError):
            # A connection-level failure leaves a half-open socket with capabilities
            # unset. Drop the link so connect()+detect() is atomic (#274). ``disconnect()``
            # rather than ``close()``: the connection stays usable and reconnects on the
            # next request — and it may be shared with other consumers, who have done
            # nothing to deserve having it closed. Guarded so a teardown error (e.g. a
            # flaky writer.wait_closed()) can't mask the original failure we're propagating.
            try:
                await self.connection.disconnect()
            except Exception:
                _logger.exception("detect: error during connection teardown after failure")
            raise

    async def probe_alive(self, timeout: float = 2.0, retries: int = 0) -> bool:
        """Cheap reconnect liveness gate: read HR(0) once and report whether the inverter answered.

        Distinct from :meth:`detect` by design. Its ONLY job is "is the inverter responding at
        all?", so on a hung dongle it fails fast (default ``retries=0``, no peripheral sweep) and
        its failure is free — the caller just probes again next tick. On the first success the
        caller runs a full, robust ``detect(retries=3)`` to re-establish topology; that detect
        stays robust because it no longer carries the probe's fail-fast constraint (which is the
        whole reason this is a separate method rather than ``detect(retries=0)`` — one call cannot
        be both the cheap gate and the robust recovery, since the identity read shares a single
        ``retries``).

        Mirrors :meth:`detect`'s connection discipline, returning a bool instead of raising:

        - **Alive** (HR(0) came back): the socket is left **open**, so the caller's follow-up
          ``detect`` reuses the same live connection.
        - **Not alive** (timeout / :class:`CommunicationError`, or a response that left no usable
          HR(0)): the link is **dropped** (mirroring detect's #274 teardown), releasing the socket
          for the dongle's quiet window; the next request re-establishes it. Never raises.

        Reuses the exact HR(0,60)@0x11 read :meth:`detect` uses (the read proven against real
        hardware). Reconnect cadence/backoff stays the caller's concern (#356): this owns only the
        single check.
        """
        # Liveness must come from THIS probe's fresh response, not any cached HR(0): a
        # stale HR(0) from an earlier healthy connection would otherwise make a fresh
        # non-committing probe look alive (e.g. the dongle returns an all-zero bank that
        # _commit_bank rejects while the read still returns — Codex review). The block's
        # ingestion timestamp is stamped ONLY on a successful commit (which applies every
        # guard: Modbus error, CRC, all-zero rejection), so "did this probe re-stamp the
        # HR(0,60) block" is the honest, guard-delegating liveness signal.
        block = (0x11, "HR", 0, 60)
        stamped_before = self.plant.register_block_updated_at.get(block)
        try:
            await self.send_request_and_await_response(
                ReadHoldingRegistersRequest(base_register=0, register_count=60, device_address=0x11),
                timeout=timeout,
                retries=retries,
            )
            stamped_after = self.plant.register_block_updated_at.get(block)
            alive = stamped_after is not None and stamped_after != stamped_before
        except (*DEVICE_DID_NOT_ANSWER, CommunicationError):
            alive = False
        if not alive:
            # Release the socket on any not-alive outcome, mirroring detect()'s teardown so a
            # failed liveness check leaves no half-open connection. Guarded so a teardown
            # error can't turn a clean False into an exception.
            try:
                await self.connection.disconnect()
            except Exception:
                _logger.exception("probe_alive: error during connection teardown after failed liveness probe")
        return alive

    async def _detect_lv_batteries(
        self,
        prior: PlantCapabilities | None,
        timeout: float,
        retries: int,
        probe_timeout: float,
        probe_retries: int,
    ) -> None:
        """Populate register caches for LV battery addresses (detect step 4).

        Battery pack #1 is at 0x32, additional packs at 0x33–0x37 (the inverter lives at 0x11, not
        0x32 — issues #119/#189). Per-slot (not break-on-fail) like the meter sweep: addresses can be
        non-contiguous and a transient BMS timeout on pack N must not drop pack N+1 onward.
        is_valid() gating and caps population are handled by _derive_capabilities.
        """
        batt_candidates = prior.lv_battery_addresses if prior is not None else _COLD_LV_BATTERY_RANGE
        # The 0x32 preamble keeps the patient known-tier treatment (the primary pack matters — the
        # #350 family), but silence is NOT fatal (#358): gateways keep battery data register-embedded
        # in the rollup, and a hybrid with no pack attached is a valid AC/PV-only plant. Skipped
        # entirely when prior capabilities carry no 0x32 (a hinted gateway reconnect would otherwise
        # stall the full timeout on a read that can never answer, every reconnect).
        if 0x32 in batt_candidates:
            try:
                await self.send_request_and_await_response(
                    ReadInputRegistersRequest(base_register=60, register_count=60, device_address=0x32),
                    timeout=timeout,
                    retries=retries,
                    warn_timeout=False,  # expected-absence probe semantics: don't pollute retry counters
                )
                # #352/#289: since #352 a caps-absent 0x32 read routes through the battery getter, so
                # its first preamble frame is held by the cold-start splice guard (the cache stays empty
                # pending a corroborating re-read) exactly like 0x33+. detect() reads each address once,
                # so without this confirming read the primary pack is dropped at the _derive_capabilities
                # gate below and refresh() never re-polls it. One healthy re-read corroborates and
                # commits; a flapping/spliced bank fails to corroborate and correctly stays out (#289
                # anti-poison intact).
                if not self.plant.register_caches.get(0x32):
                    await self.send_request_and_await_response(
                        ReadInputRegistersRequest(base_register=60, register_count=60, device_address=0x32),
                        timeout=timeout,
                        retries=retries,
                        warn_timeout=False,
                    )
            except ConnectionLost:
                raise  # a dead connection is not an absent battery (#356 dual-base ordering)
            except DEVICE_DID_NOT_ANSWER:
                _logger.info("No LV battery answered at 0x32 — valid for gateways and battery-less plants (#358)")
                self.plant.mark_absent(0x32, "IR", 60, 60)
                self.plant.register_caches.pop(0x32, None)
            else:
                if not self.plant.register_caches.get(0x32):
                    self.plant.mark_absent(0x32, "IR", 60, 60)
        for batt_addr in batt_candidates:
            if batt_addr > 0x32:
                if not await self._probe(
                    ReadInputRegistersRequest(base_register=60, register_count=60, device_address=batt_addr),
                    timeout=probe_timeout,
                    retries=probe_retries,
                ):
                    self.plant.mark_absent(batt_addr, "IR", 60, 60)
                    self.plant.register_caches.pop(batt_addr, None)
                    continue
                # #233/#289: the first battery bank against an empty cache is held by the cold-start
                # splice guard (the cache stays empty pending a corroborating re-read). detect()
                # probes each address once, so without this confirming read the address is dropped at
                # the gate below and refresh() never re-polls it — a permanent hold for a recovered/
                # returned pack. One healthy re-read corroborates and commits; a flapping/spliced bank
                # fails to corroborate and correctly stays out (#289 anti-poison intact). Removed once
                # #213's placeholder model decouples enumeration from data adoption.
                if not self.plant.register_caches.get(batt_addr):
                    await self._probe(
                        ReadInputRegistersRequest(base_register=60, register_count=60, device_address=batt_addr),
                        timeout=probe_timeout,
                        retries=probe_retries,
                    )
                if not self.plant.register_caches.get(batt_addr):
                    self.plant.mark_absent(batt_addr, "IR", 60, 60)

    async def _detect(
        self,
        timeout: float,
        retries: int,
        probe_timeout: float,
        probe_retries: int,
        prior: PlantCapabilities | None,
    ) -> PlantCapabilities:
        """Implementation of detect(); see detect() for the contract and error semantics."""
        if prior is not None:
            _logger.info(
                "detect: hinted mode — assuming device_type=Model.%s, inverter=0x%02x, "
                "meters=[%s], lv_batteries=[%s], bcus=[%s], lv_bcu=%s",
                prior.device_type.name,
                prior.inverter_address,
                ", ".join(f"0x{a:02x}" for a in prior.meter_addresses),
                ", ".join(f"0x{a:02x}" for a in prior.lv_battery_addresses),
                ", ".join(f"0x{0x70 + offset:02x} (x{n})" for offset, n in prior.bcu_stacks),
                f"0x{prior.lv_bcu_address:02x}" if prior.lv_bcu_address is not None else "None",
            )

        # Step 1 — read the inverter's configuration block to get DTC and ARM firmware.
        # 0x11 is the inverter's canonical address for every model (#189); discovery reads
        # there and the response is cached under 0x11 (issue #119). resolve_model() below maps
        # the DTC to the model; PlantCapabilities derives the same 0x11 for later polling.
        await self.send_request_and_await_response(
            ReadHoldingRegistersRequest(base_register=0, register_count=60, device_address=0x11),
            timeout=timeout,
            retries=retries,
        )
        cache: RegisterCache = self.plant.register_caches.get(0x11, RegisterCache())
        raw_dtc = cache.get(HR(0))
        if raw_dtc is None:
            raise CommunicationError(
                "detect: HR(0) not populated after reading device 0x11 — cannot determine device type"
            )
        arm_fw = cache.get(HR(21)) or 0
        # arm_firmware_version here is consistency-only: this intermediate `caps` only
        # feeds the probing steps below (is_hv, device_type), none of which read
        # firmware. The object detect() actually returns is built separately in
        # _derive_capabilities() (#293 Slice B), which populates the field for real.
        caps = PlantCapabilities(device_type=resolve_model(raw_dtc, arm_fw), arm_firmware_version=arm_fw or None)
        _logger.info("detect: device_type=Model.%s", caps.device_type.name)

        if prior is not None and prior.device_type != caps.device_type:
            self.plant.capabilities = None
            raise PlantTopologyMismatch(
                f"detect: device_type changed since prior capture "
                f"(prior={prior.device_type}, actual={caps.device_type}) — discarding hint",
                prior=prior,
                actual=caps,
            )

        # Step 2 — BCU probing for HV systems.
        if caps.is_hv:
            await self._detect_bcu_stacks(caps, prior, probe_timeout, probe_retries)
            _logger.info(
                "detect: bcu_stacks=[%s]",
                ", ".join(f"0x{0x70 + o:02x} (x{n})" for o, n in caps.bcu_stacks),
            )

        # Step 2b — AIO per-module battery probing (#192). The All-in-One exposes each
        # battery module at its own device address (0x50+), distinct from the bcu_stacks
        # stride layout, so its per-module cell/temperature/serial data is reachable.
        if caps.device_type is Model.ALL_IN_ONE and caps.bcu_stacks:
            _offset, num_modules = caps.bcu_stacks[0]
            if num_modules > self._AIO_MAX_MODULES:
                _logger.warning(
                    "detect: BCU reports %d modules but AIO maximum is %d — clamping",
                    num_modules,
                    self._AIO_MAX_MODULES,
                )
            await self._probe_ranges(
                _strategise(caps, prior, "aio_modules"), timeout, retries, probe_timeout, probe_retries
            )

        # Step 2c — HV BMU per-module probing (#265). Non-AIO HV stacks expose per-cell data at
        # their own BMU addresses (0x50+), distinct from the bcu_stacks stride decode (which read
        # the BCU's cluster registers as cells). Self-gated inside _strategise to non-AIO HV.
        await self._probe_ranges(_strategise(caps, prior, "hv_bmus"), timeout, retries, probe_timeout, probe_retries)

        # Step 3 — meter probing. Hinted: only previously-seen addresses. Cold: full 0x01–0x08 sweep.
        # In both modes, a probe response is necessary but not sufficient — some EMS firmwares
        # ACK every slot in 0x01..0x08 with all-zero registers regardless of whether a meter is
        # actually wired. Validate via Meter.is_valid() to filter those ghosts (in the validate step
        # below). Per-slot (not break-on-fail): meters can be non-contiguous. See #95.
        await self._probe_ranges(_strategise(caps, prior, "meters"), timeout, retries, probe_timeout, probe_retries)

        # Step 4 — LV battery + LV BCU detection. Skipped for HV systems (handled at step 2) and
        # EMS plant controllers (don't expose IR at the inverter address — see #86).
        if not caps.is_hv and not caps.is_ems:
            # _detect_lv_batteries stays imperative: it issues a known-tier preamble read
            # at 0x32 and contains the cold-start splice-guard reprobe (#233/#289/#213).
            await self._detect_lv_batteries(prior, timeout, retries, probe_timeout, probe_retries)

            # Step 4b — LV BCU page probe (#241).
            await self._probe_ranges(_strategise(caps, prior, "lv_bcu"), timeout, retries, probe_timeout, probe_retries)

        # Step 5 — EMS rollup cross-check. See `_ems_rollup_cross_check()` for the contract.
        if caps.is_ems:
            await self._ems_rollup_cross_check(timeout=timeout, retries=retries)

        # Validate: derive the authoritative capabilities from the now-populated register_caches.
        # is_valid() gating, mark_absent on invalid, and all candidate logic live in _derive_capabilities;
        # no duplicated enumeration here. on_reject threads mark_absent into the validate step.
        final_caps = _derive_capabilities(self.plant.register_caches, prior, on_reject=self.plant.mark_absent)
        _logger.info(
            "detect: meters=[%s], lv_batteries=[%s], lv_bcu=%s, aio_modules=[%s], hv_bmus=[%s]",
            ", ".join(f"0x{a:02x}" for a in final_caps.meter_addresses),
            ", ".join(f"0x{a:02x}" for a in final_caps.lv_battery_addresses),
            f"0x{final_caps.lv_bcu_address:02x}" if final_caps.lv_bcu_address is not None else "None",
            ", ".join(f"0x{a:02x}" for a in final_caps.aio_battery_module_addresses),
            ", ".join(f"0x{a:02x}" for a in final_caps.hv_bmu_addresses),
        )

        # arm_firmware_version is informational, not part of the topology contract — a
        # firmware upgrade on otherwise-unchanged hardware must not raise a mismatch
        # here (#293 Slice B added the field; excluding it from this comparison keeps
        # PlantTopologyMismatch scoped to genuine address/device-type/count changes).
        if prior is not None and prior.model_dump(exclude={"arm_firmware_version"}) != final_caps.model_dump(
            exclude={"arm_firmware_version"}
        ):
            self.plant.capabilities = None
            raise PlantTopologyMismatch(
                f"detect: plant topology does not match prior — prior={prior!r}, actual={final_caps!r}",
                prior=prior,
                actual=final_caps,
            )

        self.plant.capabilities = final_caps
        return final_caps

    async def _execute_reads(
        self,
        requests: list[TransparentRequest],
        *,
        timeout: float,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Run a batch of register reads, tolerating partial failure.

        Successful reads have already been written to the register caches by the
        network consumer task, so this only decides how to *signal* the failures:

        - no failures → return (the caller returns the populated plant);
        - some failed → raise ``RefreshPartiallySucceeded`` carrying the partial
          plant plus the structured failures — the caller's one chance to use
          the data that did come back;
        - all failed → raise ``RefreshFailed`` (link effectively dead).
        """
        if not requests:
            return
        results = await self.execute(
            requests, timeout=timeout, retries=retries, retry_delay=retry_delay, return_exceptions=True
        )
        failures: list[ReadFailure] = []
        causes: list[Exception] = []
        for req, res in zip(requests, results, strict=True):
            if isinstance(res, Exception):
                # base_register/register_count live on read requests, which is all
                # _execute_reads is ever handed; getattr keeps mypy happy without a
                # never-taken else branch.
                failures.append(
                    ReadFailure(
                        req.device_address,
                        type(req).__name__,
                        getattr(req, "base_register", 0),
                        getattr(req, "register_count", 0),
                    )
                )
                causes.append(res)
            elif isinstance(res, BaseException):
                # Control-flow exceptions (e.g. CancelledError) must never be swallowed.
                raise res
        if not failures:
            return
        group = ExceptionGroup(f"{len(failures)}/{len(requests)} register reads failed", causes)
        summary = ", ".join(f"{f.request_type}(0x{f.device_address:02x},{f.base_register})" for f in failures)
        if len(failures) == len(requests):
            _logger.warning("All %d register reads failed; treating plant as unreachable", len(requests))
            raise RefreshFailed(f"all {len(requests)} register reads failed", failures=failures, cause=group)
        _logger.warning("%d of %d register reads failed: %s", len(failures), len(requests), summary)
        raise RefreshPartiallySucceeded(
            f"{len(failures)} of {len(requests)} register reads failed",
            plant=self.plant,
            failures=failures,
            cause=group,
        )

    async def load_config(self, timeout: float = 2.0, retries: int = 3, retry_delay: float = 0.5) -> Plant:
        """Read HR configuration blocks for the inverter.

        Returns the populated plant on full success. On partial/total read
        failure raises ``RefreshPartiallySucceeded`` / ``RefreshFailed``.

        Success does not imply *fresh*: the keep-last-good guards (CRC #255, sub-bus
        splice #256, bank holds) report a successful poll while serving last-known-good
        content for a device whose live read was rejected. Display consumers should gate
        on ``Plant.register_age()`` / ``Plant.block_age()``, not on a poll returning.
        """
        caps = self.plant.capabilities
        if caps is None:
            raise PlantNotDetected(
                "load_config() requires plant capabilities — call detect() once first, "
                "or restore a persisted PlantCapabilities onto client.plant.capabilities."
            )
        inverter = caps.inverter_address
        is_ems = caps.is_ems
        # HR(0,60) is the identity/firmware/serial bank that every device type — including EMS —
        # answers; it's the same bank detect() reads to identify the device. The HR(60,60),
        # HR(120,60) and IR(120,60) banks are inverter-specific; EMS plant controllers don't
        # expose them and the reads time out every poll. The EMS's own window at HR(2040,36)
        # is covered by the EMS-conditional append below. See #86 (wire capture confirmed via
        # dewet22/givenergy-hass#52).
        reqs: list[TransparentRequest] = [
            ReadHoldingRegistersRequest(base_register=0, register_count=60, device_address=inverter),
        ]
        if not is_ems:
            reqs += [
                ReadHoldingRegistersRequest(base_register=60, register_count=60, device_address=inverter),
                ReadHoldingRegistersRequest(base_register=120, register_count=60, device_address=inverter),
                ReadInputRegistersRequest(base_register=120, register_count=60, device_address=inverter),
            ]
        if caps.is_three_phase:
            reqs += [_request_for_range(r, inverter) for r in manifest.LOAD_CONFIG_THREE_PHASE_RANGES]
        if caps.has_extended_slots:
            reqs.append(ReadHoldingRegistersRequest(base_register=240, register_count=60, device_address=inverter))
        # getattr(caps, name), not manifest.gated_ranges(...): the latter reads
        # manifest.CAPABILITIES directly and is blind to PropertyMock-patched
        # instance properties, which two tests rely on for facts with no confirmed
        # model yet (has_hv_cabinet_block/has_peak_shaving_block, #293). Every
        # LOAD_CONFIG_RANGES fact today is firmware-independent, so this is safe;
        # a future firmware-gated fact here would need manifest.gated_ranges(...,
        # caps.arm_firmware_version) instead, since getattr() can't pass arm_fw.
        for name, entries in manifest.LOAD_CONFIG_RANGES.items():
            if getattr(caps, name):
                reqs += [_request_for_range(r, inverter) for r in entries]
        await self._execute_reads(reqs, timeout=timeout, retries=retries, retry_delay=retry_delay)
        return self.plant

    async def refresh(
        self,
        timeout: float = 2.0,
        retries: int = 1,
        retry_delay: float = 0.5,
        ir0_max_age: float | None = None,
        *,
        max_age: float | None = None,
    ) -> Plant:
        """Read IR measurement blocks for all known devices.

        Returns the populated plant on full success. On partial/total read
        failure raises ``RefreshPartiallySucceeded`` / ``RefreshFailed``.

        Success does not imply *fresh*: the keep-last-good guards (CRC #255, sub-bus
        splice #256, bank holds) report a successful poll while serving last-known-good
        content for a device whose live read was rejected. Display consumers should gate
        on ``Plant.register_age()`` / ``Plant.block_age()``, not on a poll returning.

        The ``timeout=2.0, retries=1`` defaults are tuned for a contended bus: the
        inverter serialises requests, so when other clients (GivTCP, the vendor app,
        Predbat) poll the same unit a tighter budget produces spurious timeouts even
        though the device is responsive (#132). Pass a tighter budget if you own the
        bus exclusively and want genuine failures surfaced faster.

        ``max_age`` (seconds) opts in to skip-if-fresh for any IR bank (#196, #207):
        GivEnergy dongles fan out the responses to whoever is polling them (the cloud,
        the app, another client), so the consumer often already has recent data in cache
        without us asking. When set, any IR bank committed within ``max_age`` seconds
        is not re-solicited this cycle. Defaults to ``None`` — always solicit, the
        historic behaviour. Note the fan-out only exists while something else is polling
        the unit; on a cloud-disconnected dongle the blocks age out and we solicit them.
        The fan-out is opportunistic, not a promise that concurrent access is free —
        some dongles idle-reap the connection under multiple clients (hass#95) and
        reconnect transparently; the skip degrades safely (blocks age out, we solicit).

        ``ir0_max_age`` is deprecated — use ``max_age`` instead. It applied the same
        logic to IR(0,60) only; ``max_age`` extends it to every bank. Will be removed
        in 3.0.
        """
        caps = self.plant.capabilities
        if caps is None:
            raise PlantNotDetected(
                "refresh() requires plant capabilities — call detect() once first, "
                "or restore a persisted PlantCapabilities onto client.plant.capabilities."
            )
        if ir0_max_age is not None:
            warnings.warn(
                "refresh(ir0_max_age=...) is deprecated; use max_age= instead "
                "(applies to all banks, not just IR(0,60)). ir0_max_age will be "
                "removed in 3.0.",
                DeprecationWarning,
                stacklevel=2,
            )
            if max_age is None:
                max_age = ir0_max_age
        await self._execute_reads(
            _refresh_ranges(caps, max_age, self.plant),
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
        )
        return self.plant

    async def refresh_plant(
        self,
        full_refresh: bool = True,
        max_batteries: int = 5,
        timeout: float = 2.0,
        retries: int = 1,
        retry_delay: float = 0.5,
    ) -> Plant:
        """Deprecated orchestrator — run ``detect()`` once, then drive your own loop.

        .. deprecated::
            Will be removed in 3.0 (soon). This composes ``detect()`` (when needed) +
            ``load_config()`` + ``refresh()``, which is trivial to do in the consumer
            where the partial-failure policy belongs. It propagates
            ``RefreshPartiallySucceeded`` / ``RefreshFailed`` like the primitives —
            note that on a full refresh a partial failure in ``load_config()``
            short-circuits before ``refresh()`` runs; call the primitives directly for
            full control.

            Unlike the primitives, this wrapper runs ``detect()`` for you if
            capabilities are absent (preserving the legacy connect-then-refresh shape).
            New code should call ``detect()`` then ``load_config()`` / ``refresh()``
            directly — the primitives raise ``PlantNotDetected`` rather than guessing
            an address.
        """
        warnings.warn(
            "Client.refresh_plant() is deprecated and will be removed in 3.0. Run detect() once, then "
            "drive your own poll loop over load_config()/refresh(). It now propagates "
            "RefreshPartiallySucceeded/RefreshFailed on partial/total read failure.",
            DeprecationWarning,
            stacklevel=2,
        )
        if max_batteries != 5:
            # Battery addresses now come from detect()/capabilities, so this argument
            # no longer does anything — warn rather than silently ignore a custom value.
            warnings.warn(
                "The max_batteries argument to refresh_plant() is ignored — battery "
                "addresses are now discovered by detect(). It will be removed with "
                "refresh_plant() in 3.0.",
                DeprecationWarning,
                stacklevel=2,
            )
        # The primitives require capabilities; as the legacy one-call wrapper, detect
        # them here if the caller hasn't, so connect()-then-refresh_plant() still works
        # (it now addresses correctly per model — issue #105, where an AIO answering at
        # 0x11 timed out under the old 0x32 fallback).
        if self.plant.capabilities is None:
            self.plant.capabilities = await self.detect(timeout=timeout, retries=retries)
        if full_refresh:
            await self.load_config(timeout=timeout, retries=retries, retry_delay=retry_delay)
        await self.refresh(timeout=timeout, retries=retries, retry_delay=retry_delay)
        return self.plant

    async def watch_plant(
        self,
        handler: Callable | None = None,
        refresh_period: float = 15.0,
        max_batteries: int = 5,
        timeout: float = 2.0,
        retries: int = 1,
        retry_delay: float = 0.5,
        passive: bool = False,
    ):
        """Deprecated poll loop — own the loop in the consumer instead.

        .. deprecated::
            Will be removed in 3.0. Connect, ``detect()``, then loop over
            ``load_config()`` / ``refresh()`` yourself, handling
            ``RefreshPartiallySucceeded`` / ``RefreshFailed`` as suits the consumer.
        """
        warnings.warn(
            "Client.watch_plant() is deprecated and will be removed in 3.0. Own your poll loop: "
            "connect(), detect(), then loop over load_config()/refresh() handling "
            "RefreshPartiallySucceeded/RefreshFailed as you see fit.",
            DeprecationWarning,
            stacklevel=2,
        )
        await self.connect()
        await self.refresh_plant(
            True,
            max_batteries=max_batteries,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
        )
        while True:
            if handler:
                handler()
            await asyncio.sleep(refresh_period)
            if not passive:
                # Defer to refresh_plant so capability-aware polling (EMS, gateway,
                # three-phase, HV stacks, meters) is included on each tick rather
                # than the legacy single-phase IR(0)/IR(180) + battery shape.
                await self.refresh_plant(
                    full_refresh=False,
                    max_batteries=max_batteries,
                    timeout=timeout,
                    retries=retries,
                    retry_delay=retry_delay,
                )

    def _resolve_write_safe(self) -> frozenset[int]:
        """Registers the detected model permits at the normal (non-installer) write tier.

        The single source of the caps→manifest resolution shared by one_shot_command()
        and installer_command() — write-gating logic that must stay identical across
        both call sites. Composition (base selection, AC-config union with its
        not-three-phase guard, undetected→single-phase fallback) lives in
        manifest.write_safe_registers (#293 Slice D).
        """
        caps = self.plant.capabilities
        return manifest.write_safe_registers(
            caps.device_type if caps is not None else None,
            caps.arm_firmware_version if caps is not None else None,
        )

    async def one_shot_command(
        self,
        requests: list[TransparentRequest],
        timeout: float = 1.5,
        retries: int = 0,
        retry_delay: float = 0.5,
        dry_run: bool = False,
    ) -> None:
        """Execute write requests, validating each against the detected inverter model.

        Raises InvalidPduState for any write to a register not permitted for the
        detected model. When capabilities are not yet known, falls back to the
        universally-applicable single-phase register set (conservative).

        If dry_run is True, validates but does not transmit — running the same PDU
        validation (``ensure_valid_state``) the live encode path runs, so a dry run
        never passes for a request real execution would reject.
        """
        caps = self.plant.capabilities
        safe = self._resolve_write_safe()
        model_label = caps.device_type.name if caps is not None else "undetected"
        for req in requests:
            if isinstance(req, WriteHoldingRegisterRequest):
                if req.installer:
                    raise InvalidPduState(
                        f"HR({req.register}) is an installer-tier request; use installer_command() instead",
                        req,
                    )
                if req.register not in safe:
                    raise InvalidPduState(f"HR({req.register}) is not permitted for {model_label} inverter", req)
            # Run the same PDU-level validation encode() runs (value bounds, global
            # safe-register set), so dry-run and live paths reject identically.
            req.ensure_valid_state()
        if not dry_run:
            await self.execute(requests, timeout=timeout, retries=retries, retry_delay=retry_delay)

    async def installer_command(
        self,
        requests: list[TransparentRequest],
        timeout: float = 1.5,
        retries: int = 0,
        retry_delay: float = 0.5,
        dry_run: bool = False,
    ) -> None:
        """Execute installer-tier write requests.

        Like one_shot_command() but admits registers from INSTALLER_WRITE_REGISTERS.
        Requests must be constructed with installer=True via the dedicated helpers in
        client.commands (e.g. set_battery_nominal_power, restore_factory_defaults).

        one_shot_command() always rejects installer-flagged requests — the two methods
        are non-overlapping by design (dual-gate separation).

        If dry_run is True, validates but does not transmit.
        """
        caps = self.plant.capabilities
        model_safe = self._resolve_write_safe()
        installer_safe = model_safe | INSTALLER_WRITE_REGISTERS
        model_label = caps.device_type.name if caps is not None else "undetected"
        for req in requests:
            if isinstance(req, WriteHoldingRegisterRequest):
                effective_safe = installer_safe if req.installer else model_safe
                if req.register not in effective_safe:
                    raise InvalidPduState(f"HR({req.register}) is not permitted for {model_label} inverter", req)
            req.ensure_valid_state()
        if not dry_run:
            await self.execute(requests, timeout=timeout, retries=retries, retry_delay=retry_delay)

    def _emit_to_sink(self, direction: "Direction", data: bytes) -> None:
        """Hand redacted bytes to the active capture sink, swallowing sink errors.

        The sink is a user-supplied callback. It runs inside the connection's
        long-lived pump tasks (and the capture-close flush), so an exception it
        raises would otherwise crash that background task and break the client. A
        capture is a diagnostic tee, never load-bearing — log and carry on.
        """
        sink = self._capture_sink
        if sink is None or not data:
            return
        try:
            sink(direction, data)
        except Exception:  # noqa: BLE001 — a capture sink must never break the client
            _logger.exception("capture sink raised on %s frame; dropping it and continuing", direction)

    async def capture_frames(
        self,
        sink: Callable[[Direction, bytes], None],
        duration: float = 60.0,
    ) -> None:
        """Tee redacted TX/RX wire frames to *sink* for *duration* seconds.

        *sink* is called with the direction ('rx' or 'tx') and the redacted bytes.
        The library always redacts before invoking the sink so callers can't
        accidentally see raw hardware identifiers; persistence, formatting and
        forwarding are the caller's choice.

        Redaction is frame-aware: each complete GivEnergy frame is decoded, its
        serial-bearing fields (envelope serials, C.serial-tagged register values,
        LAN-config IPs) are zeroed by type, and the frame is re-encoded with a
        freshly-computed CRC. Frames that cannot be decoded (unknown function codes,
        malformed/truncated frames) are emitted intact with a log message — they are
        never dropped or mangled. The sink sees complete frames (one call per
        complete frame) rather than raw socket chunks.

        Runs alongside the normal refresh loop — does not suspend reads or writes,
        just tees a copy of each frame to *sink*. Only one capture may run on a
        Client at a time; calling while one is in flight raises RuntimeError.
        """
        if self._capture_sink is not None:
            raise RuntimeError("a frame capture is already running on this client")
        self._capture_sink = sink
        self._capture_redactor_rx = FrameRedactor("rx")
        self._capture_redactor_tx = FrameRedactor("tx")
        self.connection.set_byte_tap(self._redact_and_emit)
        try:
            await asyncio.sleep(duration)
        finally:
            self.connection.set_byte_tap(None)
            # Flush each direction's held tail so the final bytes aren't lost.
            for direction, redactor in (("rx", self._capture_redactor_rx), ("tx", self._capture_redactor_tx)):
                if redactor is not None:
                    self._emit_to_sink(direction, redactor.flush())  # type: ignore[arg-type]
            self._capture_sink = None
            self._capture_redactor_rx = None
            self._capture_redactor_tx = None

    def _redact_and_emit(self, direction: "Direction", data: bytes) -> None:
        """Redact a chunk of raw wire bytes and hand the result to the sink."""
        redactor = self._capture_redactor_rx if direction == "rx" else self._capture_redactor_tx
        if redactor is not None:
            self._emit_to_sink(direction, redactor.feed(data))

    def execute(
        self,
        requests: list[TransparentRequest],
        timeout: float,
        retries: int,
        retry_delay: float = 0.5,
        return_exceptions: bool = False,
    ) -> Future[list[TransparentResponse]]:
        """Helper to perform multiple requests in bulk."""
        return asyncio.gather(  # type: ignore[return-value]
            *[
                self.send_request_and_await_response(m, timeout=timeout, retries=retries, retry_delay=retry_delay)
                for m in requests
            ],
            return_exceptions=return_exceptions,
        )

    async def send_request_and_await_response(
        self,
        request: TransparentRequest,
        timeout: float,
        retries: int,
        retry_delay: float = 0.5,
        warn_timeout: bool = True,
    ) -> TransparentResponse:
        """Send a request to the remote, await and return the response.

        A thin wrapper over the connection's raw-PDU surface that adds the one
        thing the transport has no business knowing: which device's retry budget
        a consumed retry belongs to.

        On timeout, ``retry_delay`` seconds pass before the next attempt is
        enqueued. The default of 0.5s was chosen to overcome the multi-second
        silent-window failure mode observed in the field — firing the retry
        immediately tends to land it inside the same silent window as the
        original request, accomplishing nothing. Callers that want the
        original "retry immediately" behaviour (e.g. fast probes, latency-
        sensitive interactive commands) should pass ``retry_delay=0``.
        """
        return await self.connection.execute(
            request,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            warn_timeout=warn_timeout,
            on_retry=lambda: self.plant.record_retry(request.device_address),
        )
