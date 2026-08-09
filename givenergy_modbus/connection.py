"""GivEnergy's Transparent transport, implemented against modbus-connection's seam.

``modbus-connection`` deliberately keeps its Protocol layer free of any backend
import: :class:`modbus_connection.ModbusConnection` is an ABC with three hooks
(``_connect_client``, ``_close_client``, ``for_unit``) and
:class:`modbus_connection.ModbusUnit` is a structural Protocol. A device that
speaks something other than MBAP/RTU is supposed to implement those two and drop
into anything typed against the Protocol. This module is that implementation for
GivEnergy.

What lives here is everything about *how bytes reach the inverter*:

* the long-lived TCP session and the producer/consumer task pair that drains it
* answering the dongle's unsolicited FC01 heartbeat (three misses and the dongle
  hangs up on us)
* correlating responses by *shape hash* rather than transaction id — GivEnergy
  pins ``tid`` to a constant ``0x5959``, so the only way to match an answer to a
  question is (function, device address, base register, register count)
* the inter-frame gap the hardware needs, via the base class's ``Pacer``
* retrying a request that timed out or came back flagged as an error

What does *not* live here is any notion of what a register means — that is the
model layer's business.

Two surfaces are exposed, and the split is the interesting part of this
migration:

:class:`GivEnergyUnit`
    The ``ModbusUnit`` Protocol, with ``unit_id`` bound to a GivEnergy *device
    address* (``0x11`` inverter/EMS, ``0x32``–``0x36`` battery BMSes, and so on).
    Only FC03/FC04/FC06 have hardware behind them; the remaining sixteen methods
    of the Protocol raise :class:`IllegalFunctionError`. This is what the
    declarative model layer in :mod:`givenergy_modbus.model.components` talks to.

:meth:`GivEnergyConnection.execute`
    A raw-PDU escape hatch. The typed Protocol cannot express a LAN-config
    broadcast, an installer-flagged write, or "read this bank and hand me the
    decoded PDU so the plant cache can ingest it", so the native surface stays
    available alongside the Protocol adapter.
"""

from __future__ import annotations

import asyncio
import logging
import random
import socket
from asyncio import Future, Queue, StreamReader, StreamWriter, Task
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from modbus_connection import (
    IllegalDataAddressError,
    IllegalFunctionError,
    ModbusConnection,
    ModbusConnectionError,
    ModbusTimeoutError,
    ModbusUnit,
)

from givenergy_modbus.exceptions import ConnectionFailed, ConnectionLost, ExceptionBase
from givenergy_modbus.framer import ClientFramer
from givenergy_modbus.pdu import (
    HeartbeatRequest,
    ReadHoldingRegistersRequest,
    ReadInputRegistersRequest,
    ReadRegistersResponse,
    TransparentRequest,
    TransparentResponse,
    WriteHoldingRegisterResponse,
)
from givenergy_modbus.pdu.base import BasePDU
from givenergy_modbus.pdu.write_registers import WriteHoldingRegisterRequest

_logger = logging.getLogger(__name__)

Direction = Literal["rx", "tx"]

#: Callback invoked with every PDU the consumer decodes, solicited or not.
FrameListener = Callable[[BasePDU], None]

#: Callback invoked with raw wire bytes in either direction, for capture tooling.
ByteTap = Callable[[Direction, bytes], None]

# The largest read GivEnergy's Transparent function accepts in one request.
MAX_REGISTERS_PER_READ = 60

# Floor for the "producer hasn't sent our frame yet" safety-net timeout. The actual wait
# also scales with the queue backlog (see ``execute``); this floor keeps it sane when the
# queue is idle. Module-level so tests can shrink it.
_FRAME_SENT_MIN_TIMEOUT = 5.0

# Upper bound on writer.drain() inside the producer loop. On a healthy link drain
# completes near-instantly (it's local socket-buffer backpressure); a stall means the
# peer stopped ACKing — a half-open connection — so it's treated as connection loss
# rather than left to wedge the producer.
_DRAIN_TIMEOUT = 10.0

# Re-warn cadence for the coalesced reader-EOF reconnect churn. A marginal dongle that
# idle-reaps the TCP connection every ~10s would otherwise emit thousands of
# 'connection lost (reader at EOF)' WARNING lines/day on a setup that is working fine —
# each drop transparently recovers. The first drop warns; subsequent drops within this
# window are demoted to DEBUG; one re-warn per window carries the running tally.
_EOF_REWARN_SECONDS = 300.0

# Depth of the transmit queue. Deep enough to absorb a whole poll's worth of reads,
# shallow enough that a wedged producer is noticed rather than silently buffering.
_TX_QUEUE_DEPTH = 20

# The device address a heartbeat response is paced under. Heartbeats carry no device
# address of their own; 0 is not a real GivEnergy address so it can never collide with
# a per-unit spacing a caller has set.
_HEARTBEAT_UNIT = 0


@dataclass(frozen=True, kw_only=True)
class GivEnergyParams:
    """Connection parameters for a GivEnergy dongle's TCP server.

    Mirrors the shape of ``modbus_connection.ModbusTcpParams`` — including the
    ``endpoint`` identity property the library uses to tell "same device" from
    "same settings" — but without a ``framer`` field, because the framing is not
    one of the library's three and is not selectable.
    """

    host: str
    """Host name or IP address of the wifi/GPRS/ethernet dongle."""

    port: int = 8899
    """TCP port the dongle's Modbus server listens on."""

    @property
    def endpoint(self) -> tuple[str, str, int]:
        """Hashable identity of the addressed dongle: transport, host, and port."""
        return ("tcp", self.host.lower(), self.port)


class _QueuedFrame:
    """One frame awaiting transmission, plus the futures that track its fate.

    ``response`` is consulted at dequeue time: a frame whose answer already
    arrived (a late reply to a previous attempt) is dropped rather than put on
    the wire again, so a retry storm doesn't make the inverter redo work.
    """

    __slots__ = ("device_address", "raw", "response", "sent")

    def __init__(
        self,
        raw: bytes,
        device_address: int,
        sent: Future[bool] | None = None,
        response: Future[TransparentResponse] | None = None,
    ) -> None:
        self.raw = raw
        self.device_address = device_address
        self.sent = sent
        self.response = response


class _Session:
    """One live TCP link to a dongle: streams, framer, queue and pump tasks.

    The connection replaces this wholesale on reconnect, so nothing survives a
    drop — in particular no in-flight future outlives the session that created
    it. ``lost`` records the exception that killed the session so a request that
    races the teardown fails with the real reason rather than a generic one.
    """

    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.framer = ClientFramer()
        self.tx_queue: Queue[_QueuedFrame] = Queue(maxsize=_TX_QUEUE_DEPTH)
        self.expected_responses: dict[int, Future[TransparentResponse]] = {}
        self.consumer_task: Task[None] | None = None
        self.producer_task: Task[None] | None = None
        self.lost: ConnectionLost | None = None
        self.closing = False


class GivEnergyConnection(ModbusConnection):
    """A shared link to one GivEnergy dongle, speaking the Transparent protocol.

    Inherits connect-on-demand (with a single shared connect flight),
    ``close()``/``disconnect()``, the ``Pacer`` and the connection-lost callback
    registry from ``modbus_connection.ModbusConnection``; supplies the transport
    itself.
    """

    def __init__(
        self,
        params: GivEnergyParams,
        *,
        timeout: float = 2.0,
        message_spacing: float = 0.25,
        tx_jitter: float = 0.1,
    ) -> None:
        """Build a connection to the dongle described by ``params``.

        ``message_spacing`` is the minimum gap between consecutive frames on the
        wire; 0.25s is empirically load-bearing across GivEnergy hardware
        generations. ``tx_jitter`` is an upper bound on additive random jitter on
        top of it, dispersing coordinated bursts (polling ticks, retry storms) so
        they don't clump on fixed boundaries. It only ever lengthens the gap.
        """
        # ``ModbusConnection.__init__`` types ``params`` as a closed union of the
        # four transports the library ships. A third-party transport with its own
        # params dataclass is exactly what the Protocol seam is for, so the union
        # is too narrow; everything the base class actually does with the value
        # (``_target``, storage) works fine on ours.
        super().__init__(params, timeout=timeout, message_spacing=message_spacing)  # type: ignore[arg-type]
        self._ge_params = params
        self.tx_jitter = tx_jitter
        # The base class hands message_spacing to its Pacer and keeps no readable
        # copy, so hold our own for the frame-sent safety-net bound below.
        self.message_spacing = message_spacing
        self._frame_listeners: list[FrameListener] = []
        self._byte_tap: ByteTap | None = None
        # Reader-EOF reconnect-churn coalescing. Deliberately lives on the
        # connection rather than the session: a marginal dongle drops repeatedly,
        # so the burst must be tracked across reconnects to be coalesced at all.
        self._eof_drop_count = 0
        self._eof_last_warn_at: datetime | None = None
        self._eof_last_drop_at: datetime | None = None

    # -- identity ---------------------------------------------------------------

    @property
    def host(self) -> str:
        """Host name or IP address of the dongle."""
        return self._ge_params.host

    @property
    def port(self) -> int:
        """TCP port of the dongle's Modbus server."""
        return self._ge_params.port

    def __repr__(self) -> str:
        return f"GivEnergyConnection({self.host}:{self.port})"

    # -- taps -------------------------------------------------------------------

    def add_frame_listener(self, listener: FrameListener) -> Callable[[], None]:
        """Subscribe to every decoded PDU; returns an unsubscribe callable.

        The ``ModbusUnit`` Protocol is strictly request/response, but a
        GivEnergy dongle volunteers frames nobody asked for: the periodic
        heartbeat, LAN-config broadcasts, and — because several clients may share
        one dongle — register responses addressed to somebody else's request.
        Those carry real, current register data, so the plant cache wants them.
        This hook is where they surface.
        """
        self._frame_listeners.append(listener)

        def unsubscribe() -> None:
            try:
                self._frame_listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def set_byte_tap(self, tap: ByteTap | None) -> None:
        """Tee raw wire bytes in both directions to ``tap`` (None to stop)."""
        self._byte_tap = tap

    def _emit_bytes(self, direction: Direction, data: bytes) -> None:
        """Hand raw bytes to the tap, swallowing its errors.

        The tap runs inside the long-lived pump tasks, so an exception it raises
        would otherwise kill the connection. A capture is a diagnostic tee, never
        load-bearing — log and carry on.
        """
        tap = self._byte_tap
        if tap is None or not data:
            return
        try:
            tap(direction, data)
        except Exception:  # noqa: BLE001 — a byte tap must never break the connection
            _logger.exception("byte tap raised on %s data; dropping it and continuing", direction)

    def _dispatch_frame(self, pdu: BasePDU) -> None:
        """Notify frame listeners, swallowing their errors for the same reason."""
        for listener in list(self._frame_listeners):
            try:
                listener(pdu)
            except Exception:  # noqa: BLE001 — a listener must never break the connection
                _logger.exception("frame listener raised on %s; dropping it and continuing", pdu)

    # -- pacing -----------------------------------------------------------------

    def set_unit_spacing(self, unit_id: int, seconds: float) -> None:
        """Set (or, with ``0``, clear) the minimum gap between one unit's frames.

        Backs ``GivEnergyUnit.set_message_spacing``. The ``Pacer`` the base class
        builds is protected, and a unit handle is not a subclass of the
        connection, so a backend needs a method like this to reach it.
        """
        self._pacer.set_unit_spacing(unit_id, seconds)

    # -- ModbusConnection hooks -------------------------------------------------

    async def _connect_client(self) -> _Session:
        """Open the TCP link and start the pump tasks."""
        try:
            opening = asyncio.open_connection(host=self.host, port=self.port, flags=socket.TCP_NODELAY)
            reader, writer = await asyncio.wait_for(opening, timeout=self._timeout)
        except TimeoutError as err:
            raise ConnectionFailed(f"Timed out connecting to {self.host}:{self.port}") from err
        except OSError as err:
            raise ConnectionFailed(f"Error connecting to {self.host}:{self.port}") from err
        session = _Session(reader, writer)
        session.consumer_task = asyncio.create_task(self._consume(session), name="ge_network_consumer")
        session.producer_task = asyncio.create_task(self._produce(session), name="ge_network_producer")
        _logger.info("Connection established to %s:%d", self.host, self.port)
        return session

    async def _close_client(self, client: Any) -> None:
        """Tear a session down deliberately; never fires connection-lost callbacks."""
        session: _Session = client
        session.closing = True
        if session.producer_task is not None:
            session.producer_task.cancel()
        # Fail anything still waiting so callers unblock rather than hang on a
        # session that will never answer again.
        self._fail_pending(session, ConnectionLost("connection closed"))
        writer = session.writer
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass
        if session.consumer_task is not None:
            session.consumer_task.cancel()

    def for_unit(self, unit_id: int) -> ModbusUnit:
        """Return a ``ModbusUnit`` handle bound to GivEnergy device address ``unit_id``."""
        return GivEnergyUnit(self, unit_id)

    # -- teardown ---------------------------------------------------------------

    def _fail_pending(self, session: _Session, exc: ConnectionLost) -> None:
        """Resolve every future the session still owns with ``exc``."""
        for fut in session.expected_responses.values():
            if not fut.done():
                fut.set_exception(exc)
        session.expected_responses = {}
        while not session.tx_queue.empty():
            self._fail_queued(session.tx_queue.get_nowait(), exc)

    @staticmethod
    def _fail_queued(queued: _QueuedFrame, exc: ConnectionLost) -> None:
        """Resolve both futures of one queued frame with ``exc``."""
        for fut in (queued.sent, queued.response):
            if fut is not None and not fut.done():
                fut.set_exception(exc)

    def _note_lost(self, session: _Session, exc: ConnectionLost) -> None:
        """Tear down after an *unexpected* drop and fire the lost callbacks.

        Idempotent, and a no-op during a deliberate ``close()``/``disconnect()``
        (which is what ``session.closing`` marks) — those are not losses.

        The base class owns ``_client`` and the callback registry but offers a
        backend no hook to say "the transport went away", so this reaches for
        both directly. Clearing ``_client`` is what makes ``connected`` flip and
        what lets the next request start a fresh connect flight.
        """
        if session.closing or session.lost is not None:
            return
        session.lost = exc
        if self._client is session:
            self._client = None
        # Release the socket. A half-open drop only kills one direction, so
        # without this the peer keeps the other end (and its handler task) alive
        # for as long as the process runs. close() can't do it for us: it works
        # off ``_client``, which this method has just cleared.
        try:
            session.writer.close()
        except Exception:  # noqa: BLE001 — teardown must not raise over an already-dead link
            _logger.debug("closing the writer of a lost session failed", exc_info=True)
        self._fail_pending(session, exc)
        current = asyncio.current_task()
        for task in (session.consumer_task, session.producer_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
        self._lost_callbacks.fire()

    def _note_eof_drop(self, now: datetime) -> tuple[bool, int]:
        """Coalesce a burst of reader-EOF reconnect churn; decide WARNING vs DEBUG.

        Returns ``(warn_now, count_since_last_warn)``. The first drop of a burst, and the
        first drop past each ``_EOF_REWARN_SECONDS`` window within a *sustained* burst,
        warn and carry the running tally of drops since the previous warning; drops in
        between return ``(False, 0)`` for DEBUG.

        A burst is a run of drops each within ``_EOF_REWARN_SECONDS`` of the previous one.
        When the gap since the *last drop* exceeds the window the churn has clearly
        stopped, so the burst is closed and the next drop starts fresh rather than
        escalating with a stale tally — keeping the reported count honest. A genuinely
        fresh problem is therefore never swallowed; only sustained churn is throttled.
        """
        last_drop = self._eof_last_drop_at
        if last_drop is not None and (now - last_drop).total_seconds() >= _EOF_REWARN_SECONDS:
            self._eof_drop_count = 0
            self._eof_last_warn_at = None
        self._eof_last_drop_at = now
        self._eof_drop_count += 1
        last_warn = self._eof_last_warn_at
        if last_warn is None or (now - last_warn).total_seconds() >= _EOF_REWARN_SECONDS:
            count = self._eof_drop_count
            self._eof_last_warn_at = now
            self._eof_drop_count = 0
            return True, count
        return False, 0

    # -- pump tasks -------------------------------------------------------------

    async def _consume(self, session: _Session) -> None:
        """Reassemble inbound frames, answer heartbeats, resolve awaiting futures."""
        reader = session.reader
        while not reader.at_eof():
            chunk = await reader.read(300)
            self._emit_bytes("rx", chunk)
            async for message in session.framer.decode(chunk):
                if isinstance(message, ExceptionBase):
                    _logger.warning("Expected response never arrived but resulted in exception: %s", message)
                    continue
                if isinstance(message, HeartbeatRequest):
                    # The dongle sends this every ~3 minutes and closes the socket
                    # after three unanswered ones. Answering is not optional.
                    _logger.debug("Responding to HeartbeatRequest")
                    self._dispatch_frame(message)
                    await session.tx_queue.put(_QueuedFrame(message.expected_response().encode(), _HEARTBEAT_UNIT))
                    continue
                if not isinstance(message, TransparentResponse):
                    _logger.warning("Received unexpected message type for a client: %s", message)
                    self._dispatch_frame(message)
                    continue
                if isinstance(message, WriteHoldingRegisterResponse):
                    _logger.log(logging.WARNING if message.error else logging.INFO, "%s", message)

                # Hand the frame out *before* resolving the awaiting future, so a
                # listener's state (the plant cache) is guaranteed up to date by the
                # time the awaiter runs, regardless of scheduling order.
                self._dispatch_frame(message)
                # Don't resolve the future for a discarded CRC-failed frame — leave it
                # pending so the timeout/retry fires a fresh request rather than treating
                # a corrupt frame as a successful read.
                if getattr(message, "crc_failed", False) and not getattr(message, "lenient_crc_commit", False):
                    continue
                future = session.expected_responses.get(message.shape_hash())
                if future is not None and not future.done():
                    future.set_result(message)
        if session.closing:
            _logger.debug("network consumer exiting on intentional shutdown")
            return
        warn_now, count = self._note_eof_drop(datetime.now(UTC))
        if warn_now and count > 1:
            _logger.warning(
                "network consumer: connection lost (reader at EOF) — %d drops since the previous "
                "warning, recovering transparently each time",
                count,
            )
        elif warn_now:
            _logger.warning("network consumer: connection lost (reader at EOF)")
        else:
            _logger.debug("network consumer: connection lost (reader at EOF) — coalesced, recovering transparently")
        self._note_lost(session, ConnectionLost("reader at EOF — connection lost"))

    async def _produce(self, session: _Session) -> None:
        """Drain the transmit queue onto the wire, one paced frame at a time."""
        writer = session.writer
        while not writer.is_closing():
            queued = await session.tx_queue.get()
            if queued.response is not None and queued.response.done():
                _logger.debug("Skipping wire send — response already resolved")
                session.tx_queue.task_done()
                if queued.sent is not None and not queued.sent.done():
                    queued.sent.set_result(True)
                continue
            try:
                # The pacer holds its lock across the write and the jitter sleep,
                # and measures the next gap from when the block exits — so the
                # effective gap is message_spacing + jitter, matching the hand-rolled
                # sleep this replaced. Only the *send* is paced: requests stay
                # pipelined, so a poll doesn't serialise on the round-trip time.
                async with self._pacer.paced(queued.device_address):
                    writer.write(queued.raw)
                    self._emit_bytes("tx", queued.raw)
                    await asyncio.wait_for(writer.drain(), timeout=_DRAIN_TIMEOUT)
                    if self.tx_jitter:
                        # B311: plain random is right for non-cryptographic burst dispersal.
                        await asyncio.sleep(random.uniform(0, self.tx_jitter))  # nosec B311
            except TimeoutError:
                _logger.warning(
                    "network producer: writer drain stalled >%.0fs — treating connection as lost",
                    _DRAIN_TIMEOUT,
                )
                self._fail_dequeued(session, queued, ConnectionLost("writer drain stalled — connection lost"))
                return
            except OSError as err:
                _logger.warning(
                    "network producer: socket error during write/drain (%s) — treating connection as lost", err
                )
                self._fail_dequeued(
                    session, queued, ConnectionLost(f"socket error during write/drain — connection lost: {err}")
                )
                return
            session.tx_queue.task_done()
            if queued.sent is not None and not queued.sent.done():
                queued.sent.set_result(True)
        if session.closing:
            _logger.debug("network producer exiting on intentional shutdown")
            return
        _logger.warning("network producer: connection lost (writer closing)")
        self._note_lost(session, ConnectionLost("writer closing — connection lost"))

    def _fail_dequeued(self, session: _Session, queued: _QueuedFrame, exc: ConnectionLost) -> None:
        """Fail an already-dequeued frame's futures, then tear the session down.

        The teardown's queue drain can't reach a frame the producer has already
        taken, so it is failed here before the drain runs.
        """
        self._fail_queued(queued, exc)
        session.tx_queue.task_done()
        self._note_lost(session, exc)

    # -- raw PDU surface --------------------------------------------------------

    async def _live_session(self) -> _Session:
        """Connect if needed and return the live session.

        Raises ``ConnectionLost`` if the session died between publication and use.
        """
        await self.connect()
        session: _Session | None = self._client
        if session is None or session.lost is not None:
            # The link died inside (or just after) the connect flight; clear it so
            # the next attempt starts a fresh one rather than reusing a corpse.
            if session is not None and self._client is session:
                self._client = None
            raise session.lost if session is not None and session.lost else ConnectionLost("connection lost")
        return session

    async def _put_on_wire(
        self,
        session: _Session,
        queued: _QueuedFrame,
        frame_sent: Future[bool],
        timeout: float,
    ) -> None:
        """Enqueue a frame and wait for the producer to have written it.

        Raises ``ConnectionLost`` if the link died around the enqueue and
        ``TimeoutError`` if the queue never drained.
        """
        try:
            await asyncio.wait_for(session.tx_queue.put(queued), timeout=5.0)
        except TimeoutError as err:
            raise TimeoutError("TX queue full — producer task has likely died") from err
        if session.lost is not None:
            # Lost the race with teardown's queue drain: this frame was enqueued
            # after it and will never be sent. The caller discards the response
            # future, which makes a post-reconnect producer skip the stale frame.
            raise session.lost
        try:
            await asyncio.wait_for(frame_sent, timeout=timeout)
        except ConnectionLost:
            raise
        except TimeoutError as err:
            # Drain is bounded, so reaching this means the producer is wedged
            # somewhere unknown — a genuine bug. Tear down so the system recovers.
            self._note_lost(session, ConnectionLost("producer stuck — tearing down"))
            raise TimeoutError("Producer task is stuck — frame not sent") from err

    async def execute(
        self,
        request: TransparentRequest,
        *,
        timeout: float,
        retries: int,
        retry_delay: float = 0.5,
        warn_timeout: bool = True,
        on_retry: Callable[[], None] | None = None,
    ) -> TransparentResponse:
        """Send a PDU, await its answer, and return it.

        Responses are matched by *shape hash* — GivEnergy pins the MBAP
        transaction id to a constant, so (function, device address, base
        register, register count) is the only correlator available. Two
        concurrent requests of the same shape therefore cannot be told apart;
        the newer one wins and the older is cancelled.

        On timeout, ``retry_delay`` seconds pass before the next attempt is
        enqueued. The default of 0.5s overcomes a multi-second silent-window
        failure mode seen in the field — firing the retry immediately tends to
        land it inside the same silent window as the original request. Callers
        that want "retry immediately" (fast probes, latency-sensitive commands)
        should pass ``retry_delay=0``.

        Raises ``ConnectionLost`` if the link drops, ``IllegalDataAddressError``
        if the device kept answering with an error response (its way of saying
        "I don't serve that bank"), and ``TimeoutError`` if it simply never
        answered. Distinguishing the two matters: an error response is a
        definitive absence, a timeout is not.
        """
        session = await self._live_session()
        expected_response = request.expected_response()
        expected_shape_hash = expected_response.shape_hash()
        existing = session.expected_responses.get(expected_shape_hash)
        if existing is not None and not existing.done():
            _logger.debug("Cancelling existing in-flight request and replacing: %s", request)
            existing.cancel()

        raw_frame = request.encode()
        loop = asyncio.get_running_loop()

        def discard(fut: Future[TransparentResponse]) -> None:
            # Abandon a future and remove its registration — but only if it's still the
            # one mapped under expected_shape_hash. A newer same-shaped caller may have
            # replaced it; evicting that newer mapping would leave the newer caller
            # unable to receive its response.
            fut.cancel()
            if session.expected_responses.get(expected_shape_hash) is fut:
                del session.expected_responses[expected_shape_hash]

        # Worst case a frame sits behind a full queue while the producer pauses
        # message_spacing + up to tx_jitter between sends, so scale the safety-net
        # bound by the whole queue depth. Only fires if the producer is genuinely stuck.
        frame_sent_timeout = max(
            _FRAME_SENT_MIN_TIMEOUT,
            _TX_QUEUE_DEPTH * (self.message_spacing + self.tx_jitter) * 1.5,
        )

        tries = 0
        # Remembers whether the last exhausted attempt ended in a device error
        # response rather than silence, so the raise below can say which.
        errored = False
        while tries <= retries:
            response_future: Future[TransparentResponse] = loop.create_future()
            session.expected_responses[expected_shape_hash] = response_future
            frame_sent: Future[bool] = loop.create_future()
            queued = _QueuedFrame(raw_frame, request.device_address, frame_sent, response_future)
            try:
                await self._put_on_wire(session, queued, frame_sent, frame_sent_timeout)
            except BaseException:
                discard(response_future)
                raise
            try:
                await asyncio.wait_for(response_future, timeout=timeout)
            except ConnectionLost:
                raise  # a drop mid-await propagates immediately; never a retry
            except TimeoutError:
                tries += 1
                errored = False
                _logger.debug("Timeout awaiting %s, attempting retry %d of %d", expected_response, tries, retries)
                if tries <= retries:
                    if warn_timeout and on_retry is not None:
                        on_retry()
                    if retry_delay > 0:
                        # Discard the orphaned future so a late response from this attempt
                        # doesn't accidentally resolve into the next attempt's future.
                        response_future.cancel()
                        await asyncio.sleep(retry_delay)
                continue
            response = response_future.result()
            if response.error:
                _logger.error("Received error response, retrying: %s", response)
                tries += 1
                errored = True
                if tries <= retries:
                    if warn_timeout and on_retry is not None:
                        on_retry()
                    if retry_delay > 0:
                        await asyncio.sleep(retry_delay)
                continue
            if tries > 0:
                _logger.debug("Received %s after %d tries", response, tries)
            return response

        if errored:
            _logger.log(
                logging.WARNING if warn_timeout else logging.DEBUG,
                "Device rejected %s with an error response after %d tries, giving up",
                expected_response,
                tries,
            )
            raise IllegalDataAddressError(message=f"device refused {expected_response} with an error response")
        if warn_timeout:
            _logger.warning("Timeout awaiting %s after %d tries at %ss, giving up", expected_response, tries, timeout)
        else:
            _logger.debug("Timeout awaiting %s after %d tries at %ss (probe miss)", expected_response, tries, timeout)
        raise TimeoutError()


class GivEnergyUnit:
    """One GivEnergy device address, exposed as a ``modbus_connection.ModbusUnit``.

    A GivEnergy plant is genuinely a multi-unit Modbus network behind one socket:
    ``0x11`` is the inverter (or the EMS rollup on an All-in-One), ``0x32``–
    ``0x36`` are the battery BMSes, ``0x37``+ the BCU stacks, ``0x01``–``0x08``
    external meters. So ``for_unit()`` maps one-to-one onto the device address
    byte in the Transparent sub-frame, and the read planner in
    ``modbus_connection.model`` addresses each device separately without knowing
    anything about GivEnergy.

    Only three of the Protocol's nineteen function codes exist on this hardware.
    The rest raise :class:`IllegalFunctionError`, which is the honest answer: the
    device does not implement them.
    """

    def __init__(
        self,
        connection: GivEnergyConnection,
        unit_id: int,
        *,
        timeout: float = 1.0,
        retries: int = 0,
        retry_delay: float = 0.5,
    ) -> None:
        self._connection = connection
        self._unit_id = unit_id
        self._timeout = timeout
        self._retries = retries
        self._retry_delay = retry_delay

    def __repr__(self) -> str:
        return f"GivEnergyUnit(0x{self._unit_id:02x} on {self._connection.host}:{self._connection.port})"

    @property
    def unit_id(self) -> int:
        """The GivEnergy device address this handle is bound to."""
        return self._unit_id

    @property
    def connected(self) -> bool:
        """Whether the underlying link is up."""
        return self._connection.connected

    # -- register I/O -----------------------------------------------------------

    async def read_holding_registers(self, address: int, count: int) -> list[int]:
        """Read ``count`` holding registers from ``address`` (FC03)."""
        return await self._read(ReadHoldingRegistersRequest, address, count)

    async def read_input_registers(self, address: int, count: int) -> list[int]:
        """Read ``count`` input registers from ``address`` (FC04)."""
        return await self._read(ReadInputRegistersRequest, address, count)

    async def _read(
        self, request_class: type[ReadHoldingRegistersRequest | ReadInputRegistersRequest], address: int, count: int
    ) -> list[int]:
        """Issue one or more reads and return the concatenated register values.

        GivEnergy caps a read at 60 registers, so a wider block is split. The
        model layer's ``max_span`` normally keeps blocks inside the cap already;
        this is the backstop for a caller that plans its own reads.
        """
        if count <= 0:
            raise ValueError(f"register count must be positive, got {count}")
        values: list[int] = []
        for start in range(address, address + count, MAX_REGISTERS_PER_READ):
            chunk = min(MAX_REGISTERS_PER_READ, address + count - start)
            request = request_class(base_register=start, register_count=chunk, device_address=self._unit_id)
            response = await self._execute(request)
            assert isinstance(response, ReadRegistersResponse)
            values.extend(response.register_values[:chunk])
        return values

    async def write_register(self, address: int, value: int) -> None:
        """Write one holding register (FC06).

        The register must appear in the library's write-safe allowlist; writing
        an arbitrary address to this hardware can do real damage, so an
        unlisted address is refused before it reaches the wire.
        """
        await self._execute(WriteHoldingRegisterRequest(register=address, value=value, device_address=self._unit_id))

    async def write_registers(self, address: int, values: list[int]) -> None:
        """Not supported: GivEnergy implements FC06 only, never FC16."""
        if len(values) == 1:
            # A single-register FC16 is expressible as FC06, so serve it rather than
            # refusing on a technicality the caller cannot do anything about.
            await self.write_register(address, values[0])
            return
        raise IllegalFunctionError(message="GivEnergy inverters implement FC06 only; FC16 is not available")

    async def _execute(self, request: TransparentRequest) -> TransparentResponse:
        """Run a PDU through the connection, mapping failures to library errors."""
        try:
            return await self._connection.execute(
                request, timeout=self._timeout, retries=self._retries, retry_delay=self._retry_delay
            )
        except ConnectionLost as err:
            raise ModbusConnectionError(str(err) or "connection lost") from err
        except TimeoutError as err:
            raise ModbusTimeoutError(f"timed out awaiting a response to {request}") from err

    # -- unsupported function codes ---------------------------------------------

    def _unsupported(self, function_code: int, name: str) -> IllegalFunctionError:
        return IllegalFunctionError(message=f"GivEnergy inverters do not implement {name} (FC 0x{function_code:02x})")

    async def read_coils(self, address: int, count: int) -> list[bool]:
        """Not supported: GivEnergy exposes no coil space."""
        raise self._unsupported(0x01, "read coils")

    async def read_discrete_inputs(self, address: int, count: int) -> list[bool]:
        """Not supported: GivEnergy exposes no discrete-input space."""
        raise self._unsupported(0x02, "read discrete inputs")

    async def write_coil(self, address: int, value: bool) -> None:
        """Not supported: GivEnergy exposes no coil space."""
        raise self._unsupported(0x05, "write coil")

    async def write_coils(self, address: int, values: list[bool]) -> None:
        """Not supported: GivEnergy exposes no coil space."""
        raise self._unsupported(0x0F, "write coils")

    async def read_exception_status(self) -> int:
        """Not supported."""
        raise self._unsupported(0x07, "read exception status")

    async def report_server_id(self) -> bytes:
        """Not supported."""
        raise self._unsupported(0x11, "report server id")

    async def mask_write_register(self, address: int, and_mask: int, or_mask: int) -> None:
        """Not supported."""
        raise self._unsupported(0x16, "mask write register")

    async def read_write_registers(
        self, read_address: int, read_count: int, write_address: int, write_values: list[int]
    ) -> list[int]:
        """Not supported."""
        raise self._unsupported(0x17, "read/write registers")

    async def read_fifo_queue(self, address: int) -> list[int]:
        """Not supported."""
        raise self._unsupported(0x18, "read FIFO queue")

    async def read_device_identification(self) -> dict[int, bytes]:
        """Not supported."""
        raise self._unsupported(0x2B, "read device identification")

    async def read_file_record(self, file: int, record: int, length: int) -> list[int]:
        """Not supported."""
        raise self._unsupported(0x14, "read file record")

    async def write_file_record(self, file: int, record: int, values: list[int]) -> None:
        """Not supported."""
        raise self._unsupported(0x15, "write file record")

    async def diagnostics(self, sub_function: int, data: int = 0) -> int:
        """Not supported."""
        raise self._unsupported(0x08, "diagnostics")

    async def get_comm_event_counter(self) -> tuple[int, int]:
        """Not supported."""
        raise self._unsupported(0x0B, "get comm event counter")

    async def get_comm_event_log(self) -> bytes:
        """Not supported."""
        raise self._unsupported(0x0C, "get comm event log")

    # -- link management --------------------------------------------------------

    def set_message_spacing(self, seconds: float) -> None:
        """Set the minimum interval between requests to this device address."""
        self._connection.set_unit_spacing(self._unit_id, seconds)

    def on_connection_lost(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Register a callback fired when the link drops; returns an unsubscribe."""
        return self._connection.on_connection_lost(callback)


__all__ = [
    "MAX_REGISTERS_PER_READ",
    "ByteTap",
    "Direction",
    "FrameListener",
    "GivEnergyConnection",
    "GivEnergyParams",
    "GivEnergyUnit",
    "IllegalDataAddressError",
]
