"""Transport behaviour of the modbus-connection backend.

These tests used to live in ``tests/client/test_client.py``, driving the socket,
the pump tasks and the transmit queue that ``Client`` owned. All of that moved
into :class:`~givenergy_modbus.connection.GivEnergyConnection`, so the tests
moved with it and now drive the connection directly. The end-to-end tests
against a real socket are in ``tests/test_connection.py``; these exercise the
edges — stalls, drops, retries, teardown ordering — that need mock streams.
"""

import asyncio
import datetime
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from givenergy_modbus.connection import _EOF_REWARN_SECONDS, _QueuedFrame
from givenergy_modbus.exceptions import ConnectionLost
from givenergy_modbus.pdu import ClientOutgoingMessage, HeartbeatRequest, ReadInputRegistersResponse
from givenergy_modbus.pdu.write_registers import WriteHoldingRegisterRequest, WriteHoldingRegisterResponse
from tests.transport import connection_with_pumps, make_connection, prime_session, stop_pumps

LOGGER = "givenergy_modbus.connection"

pytestmark = pytest.mark.timeout(20)


def _write_response(**kwargs):
    return WriteHoldingRegisterResponse(inverter_serial_number="", register=35, value=20, **kwargs)


# ---------------------------------------------------------------------------
# request / response round trip
# ---------------------------------------------------------------------------


async def test_request_round_trip():
    """A queued request is written, and its answer resolves the awaiting future."""
    conn = make_connection()
    session = connection_with_pumps(conn)
    request = WriteHoldingRegisterRequest(register=35, value=20)

    sending = asyncio.create_task(conn.execute(request, timeout=1.0, retries=0))
    await asyncio.sleep(0)
    session.reader.feed_data(_write_response().encode())

    response = await asyncio.wait_for(sending, timeout=2)
    assert response.register == 35
    assert response.value == 20
    session.writer.write.assert_called_once_with(request.encode())
    await stop_pumps(session)


async def test_expected_responses_are_per_session():
    """In-flight tracking belongs to one live link, never shared between them."""
    first = make_connection(host="a", port=1)
    second = make_connection(host="b", port=2)
    prime_session(first).expected_responses[42] = "sentinel"
    assert 42 not in prime_session(second).expected_responses


async def test_consumer_auto_responds_to_heartbeat_request():
    """An inbound HeartbeatRequest is answered by queueing a HeartbeatResponse frame."""
    conn = make_connection()
    session = prime_session(conn)
    session.reader = asyncio.StreamReader()
    session.reader.feed_data(HeartbeatRequest(data_adapter_serial_number="AB1234G567", data_adapter_type=2).encode())
    session.closing = True
    session.reader.feed_eof()

    await conn._consume(session)

    assert not session.tx_queue.empty()
    queued = session.tx_queue.get_nowait()
    assert queued.sent is None and queued.response is None
    reply = ClientOutgoingMessage.decode_bytes(queued.raw)
    assert reply.data_adapter_type == 2


async def test_consumer_logs_warning_on_write_error_response(caplog):
    """A WriteHoldingRegisterResponse flagged as an error is surfaced at WARNING."""
    conn = make_connection()
    session = prime_session(conn)
    session.reader = asyncio.StreamReader()
    session.reader.feed_data(_write_response(error=True).encode())
    session.closing = True
    session.reader.feed_eof()

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        await conn._consume(session)

    assert any("WriteHoldingRegisterResponse" in r.message and r.levelno == logging.WARNING for r in caplog.records), (
        f"expected a WARNING for the errored write response, got: {[(r.levelname, r.message) for r in caplog.records]}"
    )


async def test_consumer_does_not_resolve_future_for_crc_failed_frame():
    """A CRC-failed frame must leave the response future pending so retries fire."""
    conn = make_connection()
    session = prime_session(conn)
    session.reader = asyncio.StreamReader()

    message = MagicMock(spec=ReadInputRegistersResponse)
    message.error = False
    message.crc_failed = True
    message.lenient_crc_commit = False
    message.device_address = 0x32
    message.base_register = 0
    message.shape_hash.return_value = 42

    future = asyncio.get_running_loop().create_future()
    session.expected_responses[42] = future

    async def fake_decode(frame):
        if frame:
            yield message

    with patch.object(session.framer, "decode", new=fake_decode):
        session.reader.feed_data(b"\x00")
        session.closing = True
        session.reader.feed_eof()
        await conn._consume(session)

    assert not future.done(), "future must stay pending for a CRC-failed frame"


# ---------------------------------------------------------------------------
# teardown: deliberate vs unexpected
# ---------------------------------------------------------------------------


async def test_close_succeeds_when_connection_closed_cleanly():
    conn = make_connection()
    session = prime_session(conn)

    await conn.close()

    session.writer.close.assert_called_once()
    session.writer.wait_closed.assert_called_once()


async def test_close_handles_connection_reset_on_wait_closed():
    """close() must not propagate ConnectionResetError when the peer tears down first."""
    conn = make_connection()
    session = prime_session(conn)
    session.writer.wait_closed = AsyncMock(side_effect=ConnectionResetError)

    await conn.close()  # must not raise

    session.writer.close.assert_called_once()


async def test_close_marks_the_session_closing():
    """close() flags the session so the pump exit paths take the quiet branch."""
    conn = make_connection()
    session = prime_session(conn)
    assert session.closing is False
    await conn.close()
    assert session.closing is True


async def test_close_is_a_noop_without_a_session():
    """Closing a connection that never connected must not raise."""
    await make_connection().close()


async def test_consumer_logs_debug_not_critical_on_intentional_shutdown(caplog):
    """Regression for #50: a deliberate teardown exits at DEBUG, not CRITICAL."""
    conn = make_connection()
    session = prime_session(conn)
    session.reader = asyncio.StreamReader()
    session.closing = True
    session.reader.feed_eof()

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        await conn._consume(session)

    assert not any(r.levelno >= logging.WARNING for r in caplog.records), (
        f"intentional shutdown should not emit WARNING+: {[(r.levelname, r.message) for r in caplog.records]}"
    )
    assert any("intentional shutdown" in r.message for r in caplog.records)


async def test_consumer_logs_warning_on_unexpected_eof(caplog):
    """A peer-initiated EOF is a recoverable drop → WARNING, not CRITICAL."""
    conn = make_connection()
    session = prime_session(conn)
    session.reader = asyncio.StreamReader()
    session.reader.feed_eof()  # closing stays False

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        await conn._consume(session)

    assert any(r.levelno == logging.WARNING for r in caplog.records)
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)  # a routine drop must not alarm


async def test_producer_logs_debug_not_critical_on_intentional_shutdown(caplog):
    """Regression for #50: the producer exits quietly on a deliberate teardown."""
    conn = make_connection()
    session = prime_session(conn)
    session.writer.is_closing.return_value = True
    session.closing = True

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        await conn._produce(session)

    assert not any(r.levelno >= logging.WARNING for r in caplog.records)
    assert any("intentional shutdown" in r.message for r in caplog.records)


async def test_producer_logs_warning_on_unexpected_writer_close(caplog):
    """A peer-initiated writer close is a recoverable drop → WARNING, not CRITICAL."""
    conn = make_connection()
    session = prime_session(conn)
    session.writer.is_closing.return_value = True  # closing stays False

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        await conn._produce(session)

    assert any(r.levelno == logging.WARNING for r in caplog.records)
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)


async def test_lost_session_fails_inflight_and_queued():
    """Teardown unblocks every waiter fast with the typed exception (#356)."""
    conn = make_connection()
    session = prime_session(conn)
    loop = asyncio.get_running_loop()
    inflight = loop.create_future()
    session.expected_responses[1234] = inflight
    queued = _QueuedFrame(b"frame", 0x32, loop.create_future(), loop.create_future())
    session.tx_queue.put_nowait(queued)

    conn._note_lost(session, ConnectionLost("test drop"))

    assert session.lost is not None
    assert conn.connected is False
    assert isinstance(inflight.exception(), ConnectionLost)
    assert isinstance(queued.sent.exception(), ConnectionLost)
    assert isinstance(queued.response.exception(), ConnectionLost)
    assert session.expected_responses == {}
    assert session.tx_queue.empty()


async def test_lost_is_idempotent_and_respects_deliberate_teardown():
    """A second loss is a no-op; a loss during close() is not a loss at all."""
    conn = make_connection()
    session = prime_session(conn)
    conn._note_lost(session, ConnectionLost("first"))
    conn._note_lost(session, ConnectionLost("second"))  # must not raise
    assert str(session.lost) == "first"

    quiet_conn = make_connection(host="bar")
    quiet = prime_session(quiet_conn)
    quiet.closing = True
    future = asyncio.get_running_loop().create_future()
    quiet.expected_responses[1] = future
    quiet_conn._note_lost(quiet, ConnectionLost("during close"))
    assert quiet.lost is None  # early-returned
    assert not future.done()  # close() owns intentional-shutdown cleanup


async def test_lost_cancels_only_the_other_task():
    """The task NOT reporting the loss is cancelled; the reporter exits on its own."""
    conn = make_connection()
    session = prime_session(conn)

    async def _sleeper():
        await asyncio.sleep(30)

    other = asyncio.create_task(_sleeper())
    session.consumer_task = other
    session.producer_task = None

    conn._note_lost(session, ConnectionLost("drop"))
    await asyncio.sleep(0)
    assert other.cancelled()


async def test_lost_releases_the_socket():
    """A dropped link closes its writer, so the peer's handler doesn't linger."""
    conn = make_connection()
    session = prime_session(conn)
    conn._note_lost(session, ConnectionLost("drop"))
    session.writer.close.assert_called_once()


async def test_lost_fires_the_connection_lost_callbacks():
    conn = make_connection()
    session = prime_session(conn)
    fired = []
    conn.on_connection_lost(lambda: fired.append(True))
    conn._note_lost(session, ConnectionLost("drop"))
    assert fired == [True]


async def test_producer_drain_stall_drops_the_link(monkeypatch, caplog):
    """A wedged writer.drain() (half-open socket, hass#233) is bounded.

    The producer treats the stall as connection loss, fails the current frame's
    futures, and tears down — instead of hanging until close().
    """
    monkeypatch.setattr("givenergy_modbus.connection._DRAIN_TIMEOUT", 0.05)
    conn = make_connection()
    session = prime_session(conn)
    never = asyncio.get_running_loop().create_future()  # a drain() that never completes
    session.writer.drain = MagicMock(return_value=never)

    loop = asyncio.get_running_loop()
    queued = _QueuedFrame(b"frame", 0x32, loop.create_future(), loop.create_future())
    session.expected_responses[99] = queued.response
    session.tx_queue.put_nowait(queued)

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        await asyncio.wait_for(conn._produce(session), timeout=2.0)

    assert isinstance(queued.sent.exception(), ConnectionLost)
    assert isinstance(queued.response.exception(), ConnectionLost)
    assert session.lost is not None
    assert conn.connected is False
    assert any("drain stalled" in r.message for r in caplog.records if r.levelno == logging.WARNING)


async def test_producer_socket_error_during_drain_drops_the_link(caplog):
    """A peer reset surfacing as OSError from write/drain runs the shared teardown (#356)."""
    conn = make_connection()
    session = prime_session(conn)
    session.writer.drain = AsyncMock(side_effect=ConnectionResetError("peer reset"))

    loop = asyncio.get_running_loop()
    queued = _QueuedFrame(b"frame", 0x32, loop.create_future(), loop.create_future())
    session.expected_responses[42] = queued.response
    session.tx_queue.put_nowait(queued)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        await asyncio.wait_for(conn._produce(session), timeout=2.0)  # must NOT raise

    assert isinstance(queued.sent.exception(), ConnectionLost)
    assert isinstance(queued.response.exception(), ConnectionLost)
    assert session.lost is not None
    assert any("socket error" in r.message for r in caplog.records if r.levelno == logging.WARNING)


async def test_producer_unexpected_writer_close_drops_the_link():
    conn = make_connection()
    session = prime_session(conn)
    session.writer.is_closing.return_value = True
    inflight = asyncio.get_running_loop().create_future()
    session.expected_responses[7] = inflight

    await conn._produce(session)

    assert session.lost is not None
    assert isinstance(inflight.exception(), ConnectionLost)


async def test_consumer_unexpected_eof_drops_the_link():
    """In-flight senders unblock immediately instead of burning their full timeouts."""
    conn = make_connection()
    session = prime_session(conn)
    session.reader = asyncio.StreamReader()
    inflight = asyncio.get_running_loop().create_future()
    session.expected_responses[11] = inflight
    session.reader.feed_eof()  # closing stays False

    await conn._consume(session)

    assert session.lost is not None
    assert isinstance(inflight.exception(), ConnectionLost)


# ---------------------------------------------------------------------------
# EOF churn coalescing
# ---------------------------------------------------------------------------


def test_note_eof_drop_first_drop_warns():
    """The first reader-EOF drop always warns, carrying a tally of 1."""
    conn = make_connection()
    t0 = datetime.datetime(2026, 7, 12, 12, 0, 0, tzinfo=datetime.UTC)
    assert conn._note_eof_drop(t0) == (True, 1)


def test_note_eof_drop_coalesces_within_interval():
    """Rapid repeat drops within the re-warn interval are demoted."""
    conn = make_connection()
    t0 = datetime.datetime(2026, 7, 12, 12, 0, 0, tzinfo=datetime.UTC)
    conn._note_eof_drop(t0)  # first — warns
    for seconds in range(10, int(_EOF_REWARN_SECONDS), 10):
        assert conn._note_eof_drop(t0 + datetime.timedelta(seconds=seconds)) == (False, 0)


def test_note_eof_drop_rewarns_after_interval_with_tally():
    """Past the re-warn interval, the next drop warns and reports the coalesced count."""
    conn = make_connection()
    t0 = datetime.datetime(2026, 7, 12, 12, 0, 0, tzinfo=datetime.UTC)
    conn._note_eof_drop(t0)
    conn._note_eof_drop(t0 + datetime.timedelta(seconds=10))
    conn._note_eof_drop(t0 + datetime.timedelta(seconds=20))
    warn_now, count = conn._note_eof_drop(t0 + datetime.timedelta(seconds=_EOF_REWARN_SECONDS))
    assert warn_now is True
    assert count == 3  # the two coalesced + this escalation drop, since the last warning


def test_note_eof_drop_rewarns_after_long_quiet_spell():
    """A single drop after a long stable period warns afresh."""
    conn = make_connection()
    t0 = datetime.datetime(2026, 7, 12, 12, 0, 0, tzinfo=datetime.UTC)
    conn._note_eof_drop(t0)
    later = t0 + datetime.timedelta(seconds=_EOF_REWARN_SECONDS + 3600)
    assert conn._note_eof_drop(later) == (True, 1)


def test_note_eof_drop_reset_keeps_tally_honest_after_quiet_gap():
    """A lone drop after a long quiet gap warns fresh, not with a stale tally."""
    conn = make_connection()
    t0 = datetime.datetime(2026, 7, 12, 12, 0, 0, tzinfo=datetime.UTC)
    conn._note_eof_drop(t0)
    conn._note_eof_drop(t0 + datetime.timedelta(seconds=10))
    conn._note_eof_drop(t0 + datetime.timedelta(seconds=20))
    assert conn._note_eof_drop(t0 + datetime.timedelta(hours=1)) == (True, 1)


async def test_consumer_coalesces_repeat_eof_to_debug(caplog):
    """A second EOF drop within the interval lands at DEBUG (hass#95 log spam)."""
    conn = make_connection()
    conn._note_eof_drop(datetime.datetime.now(datetime.UTC))  # a recent prior drop
    session = prime_session(conn)
    session.reader = asyncio.StreamReader()
    session.reader.feed_eof()

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        await conn._consume(session)

    eof_records = [r for r in caplog.records if "reader at EOF" in r.message]
    assert eof_records, "expected an EOF log line"
    assert all(r.levelno == logging.DEBUG for r in eof_records)


async def test_consumer_escalation_warning_carries_tally(caplog):
    """A sustained burst crossing the re-warn window escalates with the running tally."""
    conn = make_connection()
    now = datetime.datetime.now(datetime.UTC)
    conn._eof_drop_count = 4
    conn._eof_last_warn_at = now - datetime.timedelta(seconds=_EOF_REWARN_SECONDS + 1)
    conn._eof_last_drop_at = now - datetime.timedelta(seconds=10)
    session = prime_session(conn)
    session.reader = asyncio.StreamReader()
    session.reader.feed_eof()

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        await conn._consume(session)

    warn = [r for r in caplog.records if "reader at EOF" in r.getMessage() and r.levelno == logging.WARNING]
    assert warn, f"expected an escalation WARNING: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
    assert "5 drops" in warn[0].getMessage()  # 4 coalesced + this escalation drop
    assert "~" not in warn[0].getMessage()  # no misleading fixed-window claim


# ---------------------------------------------------------------------------
# the transmit queue
# ---------------------------------------------------------------------------


async def _drain(session, *, respond=None):
    """Stand in for the producer: release each frame's `sent`, optionally answer it."""
    attempt = 0
    while True:
        queued = await session.tx_queue.get()
        session.tx_queue.task_done()
        attempt += 1
        if queued.sent is not None and not queued.sent.done():
            queued.sent.set_result(True)
        if respond is not None:
            await asyncio.sleep(0)
            respond(session, attempt)


async def test_producer_skips_wire_send_when_response_already_resolved():
    """A frame whose answer already arrived is dropped rather than re-sent.

    Models the late-arrival case where a response from a previous attempt
    resolved the future between enqueue and dequeue. ``sent`` is still released
    so the caller-side awaiter unblocks normally.
    """
    conn = make_connection()
    session = prime_session(conn)
    loop = asyncio.get_running_loop()
    resolved = loop.create_future()
    resolved.set_result("already here")
    queued = _QueuedFrame(b"the-frame", 0x32, loop.create_future(), resolved)
    await session.tx_queue.put(queued)

    producer = asyncio.create_task(conn._produce(session))
    try:
        await asyncio.wait_for(queued.sent, timeout=0.5)
    finally:
        producer.cancel()

    session.writer.write.assert_not_called()
    assert queued.sent.result() is True


async def test_producer_sends_normally_when_response_future_pending():
    """The inverse: a pending answer means the frame is written."""
    conn = make_connection()
    session = prime_session(conn)
    loop = asyncio.get_running_loop()
    queued = _QueuedFrame(b"the-frame", 0x32, loop.create_future(), loop.create_future())
    await session.tx_queue.put(queued)

    producer = asyncio.create_task(conn._produce(session))
    try:
        await asyncio.wait_for(queued.sent, timeout=0.5)
    finally:
        producer.cancel()

    session.writer.write.assert_called_once_with(b"the-frame")


async def test_send_raises_timeout_when_tx_queue_is_full():
    """A full transmit queue must raise quickly, not block forever."""
    conn = make_connection()
    session = prime_session(conn)
    for _ in range(session.tx_queue.maxsize):
        session.tx_queue.put_nowait(_QueuedFrame(b"", 0x32))

    async def timeout_wait_for(awaitable, timeout):
        # Close the Queue.put coroutine rather than leaking it.
        awaitable.close()
        raise TimeoutError

    with patch("givenergy_modbus.connection.asyncio.wait_for", new=timeout_wait_for):
        with pytest.raises(TimeoutError, match="TX queue full"):
            await conn.execute(WriteHoldingRegisterRequest(register=35, value=20), timeout=1.0, retries=0)


async def test_send_blocked_in_full_queue_raises_connection_lost_on_drop():
    """A sender blocked in put() when the link drops must get ConnectionLost.

    The teardown's queue drain wakes the blocked putter, which would otherwise
    enqueue into a dead queue and ride the misleading 'producer is stuck' path.
    """
    conn = make_connection()
    session = prime_session(conn)
    session.tx_queue = asyncio.Queue(maxsize=1)
    session.tx_queue.put_nowait(_QueuedFrame(b"filler", 0x32))

    sending = asyncio.create_task(
        conn.execute(WriteHoldingRegisterRequest(register=35, value=20), timeout=30.0, retries=0)
    )
    await asyncio.sleep(0.05)  # let the sender block in put()
    conn._note_lost(session, ConnectionLost("drop while putter blocked"))

    with pytest.raises(ConnectionLost):
        await asyncio.wait_for(sending, timeout=2.0)


# ---------------------------------------------------------------------------
# retries
# ---------------------------------------------------------------------------


async def test_send_raises_timeout_after_all_retries_exhausted():
    conn = make_connection()
    session = prime_session(conn)
    drainer = asyncio.create_task(_drain(session))
    try:
        with pytest.raises(TimeoutError):
            await conn.execute(
                WriteHoldingRegisterRequest(register=35, value=20), timeout=0.02, retries=1, retry_delay=0
            )
    finally:
        drainer.cancel()


async def test_send_succeeds_after_timeout_retry():
    """A first attempt that times out is retried, and the retry's answer is returned."""
    conn = make_connection()
    session = prime_session(conn)
    request = WriteHoldingRegisterRequest(register=35, value=20)
    shape = request.expected_response().shape_hash()

    def respond_on_retry(sess, attempt):
        if attempt >= 2:
            future = sess.expected_responses.get(shape)
            if future is not None and not future.done():
                future.set_result(_write_response())

    drainer = asyncio.create_task(_drain(session, respond=respond_on_retry))
    try:
        result = await conn.execute(request, timeout=0.02, retries=2, retry_delay=0)
        assert result.register == 35
    finally:
        drainer.cancel()


async def test_send_retries_on_error_response_then_reports_the_refusal():
    """An error response is retried, and exhaustion reports a refusal, not silence."""
    from modbus_connection import IllegalDataAddressError

    conn = make_connection()
    session = prime_session(conn)
    request = WriteHoldingRegisterRequest(register=35, value=20)
    shape = request.expected_response().shape_hash()

    def respond_with_error(sess, attempt):
        future = sess.expected_responses.get(shape)
        if future is not None and not future.done():
            future.set_result(_write_response(error=True))

    drainer = asyncio.create_task(_drain(session, respond=respond_with_error))
    try:
        with pytest.raises(IllegalDataAddressError):
            await conn.execute(request, timeout=0.02, retries=1, retry_delay=0)
    finally:
        drainer.cancel()


async def test_on_retry_fires_once_per_consumed_retry():
    """The retry hook is how the plant's per-device retry counter is fed (#284)."""
    conn = make_connection()
    session = prime_session(conn)
    request = WriteHoldingRegisterRequest(register=35, value=20)
    shape = request.expected_response().shape_hash()
    retries_seen = []

    def respond_on_retry(sess, attempt):
        if attempt >= 2:
            future = sess.expected_responses.get(shape)
            if future is not None and not future.done():
                future.set_result(_write_response())

    drainer = asyncio.create_task(_drain(session, respond=respond_on_retry))
    try:
        await conn.execute(request, timeout=0.02, retries=2, retry_delay=0, on_retry=lambda: retries_seen.append(True))
    finally:
        drainer.cancel()
    assert retries_seen == [True]


async def test_on_retry_is_skipped_for_probe_semantics():
    """warn_timeout=False marks an expected-absence probe; its retries aren't counted (#284)."""
    conn = make_connection()
    session = prime_session(conn)
    retries_seen = []

    drainer = asyncio.create_task(_drain(session))
    try:
        with pytest.raises(TimeoutError):
            await conn.execute(
                WriteHoldingRegisterRequest(register=35, value=20),
                timeout=0.02,
                retries=1,
                retry_delay=0,
                warn_timeout=False,
                on_retry=lambda: retries_seen.append(True),
            )
    finally:
        drainer.cancel()
    assert retries_seen == []


async def test_send_sleeps_between_retries_on_timeout():
    """retry_delay > 0 imposes a gap between a timed-out attempt and the next.

    This protects against the multi-second silent-window failure mode where
    firing the retry immediately lands it inside the same window as the original.
    """
    conn = make_connection()
    session = prime_session(conn)
    send_times: list[float] = []

    async def timing_drain():
        while True:
            queued = await session.tx_queue.get()
            session.tx_queue.task_done()
            send_times.append(asyncio.get_running_loop().time())
            if queued.sent is not None and not queued.sent.done():
                queued.sent.set_result(True)

    drainer = asyncio.create_task(timing_drain())
    try:
        with pytest.raises(TimeoutError):
            await conn.execute(
                WriteHoldingRegisterRequest(register=35, value=20), timeout=0.02, retries=1, retry_delay=0.08
            )
        assert len(send_times) == 2
        gap = send_times[1] - send_times[0]
        assert gap >= 0.08, f"expected a gap of at least 80ms between retries, got {gap * 1000:.0f}ms"
    finally:
        drainer.cancel()


async def test_send_does_not_sleep_after_the_final_retry():
    """retry_delay only applies *between* retries, so a fail-fast caller fails fast."""
    conn = make_connection()
    session = prime_session(conn)
    drainer = asyncio.create_task(_drain(session))
    try:
        started = asyncio.get_running_loop().time()
        with pytest.raises(TimeoutError):
            await conn.execute(
                WriteHoldingRegisterRequest(register=35, value=20), timeout=0.02, retries=0, retry_delay=5.0
            )
        elapsed = asyncio.get_running_loop().time() - started
        assert elapsed < 0.1, f"expected fast fail (~20ms), took {elapsed * 1000:.0f}ms"
    finally:
        drainer.cancel()


async def test_connection_lost_propagates_without_burning_retries():
    """ConnectionLost IS a TimeoutError; the retry arm must not swallow it."""
    conn = make_connection()
    session = prime_session(conn)
    sending = asyncio.create_task(
        conn.execute(WriteHoldingRegisterRequest(register=35, value=20), timeout=30.0, retries=5)
    )
    queued = await asyncio.wait_for(session.tx_queue.get(), timeout=1.0)
    session.tx_queue.task_done()
    queued.sent.set_result(True)
    await asyncio.sleep(0)  # let the send advance to the response await
    conn._note_lost(session, ConnectionLost("mid-flight drop"))

    with pytest.raises(ConnectionLost):
        await asyncio.wait_for(sending, timeout=1.0)  # well under timeout=30 × 6 tries


# ---------------------------------------------------------------------------
# the stuck-producer safety net
# ---------------------------------------------------------------------------


async def test_frame_sent_timeout_cleans_up_stale_future(monkeypatch):
    """A stuck producer raises a clear TimeoutError and drops the stale future.

    Leaving it registered would let a late send resolve a request nobody is
    waiting for any more.
    """
    monkeypatch.setattr("givenergy_modbus.connection._FRAME_SENT_MIN_TIMEOUT", 0.02)
    conn = make_connection()
    session = prime_session(conn)
    request = WriteHoldingRegisterRequest(register=35, value=20)
    shape = request.expected_response().shape_hash()

    # No producer is running, so `sent` is never resolved → the stuck path.
    with pytest.raises(TimeoutError, match="stuck"):
        await conn.execute(request, timeout=1.0, retries=0)

    assert shape not in session.expected_responses, "stale response future must be cleaned up"


async def test_frame_sent_timeout_drops_the_whole_link(monkeypatch):
    """A wedged producer is a confirmed fault, so the link goes — including other waiters.

    Originally the timeout only evicted the caller's own mapping, so a newer
    same-shaped caller was untouched. Since #356 a frame-sent timeout means the
    producer is confirmed wedged, so the whole link is torn down and the newer
    caller is correctly failed too rather than left waiting on a dead socket.
    """
    monkeypatch.setattr("givenergy_modbus.connection._FRAME_SENT_MIN_TIMEOUT", 0.05)
    conn = make_connection()
    session = prime_session(conn)
    request = WriteHoldingRegisterRequest(register=35, value=20)
    shape = request.expected_response().shape_hash()

    caller_a = asyncio.create_task(conn.execute(request, timeout=1.0, retries=0))
    await asyncio.sleep(0.01)  # let A reach the frame-sent wait

    b_future = asyncio.get_running_loop().create_future()
    session.expected_responses[shape] = b_future

    with pytest.raises(TimeoutError, match="stuck"):
        await caller_a

    assert session.expected_responses.get(shape) is None  # teardown clears the whole map
    assert isinstance(b_future.exception(), ConnectionLost)
    assert session.lost is not None


async def test_discard_identity_guard_preserves_newer_caller_future():
    """One caller's cleanup must not evict a newer same-shaped caller's future.

    Caller A's frame-sent fails with ConnectionLost directly (not via a full
    teardown, so B's registration survives to observe the guard); by then B has
    replaced A's registration. A's cleanup must leave B's future alone.
    """
    conn = make_connection()
    session = prime_session(conn)
    request = WriteHoldingRegisterRequest(register=35, value=20)
    shape = request.expected_response().shape_hash()

    caller_a = asyncio.create_task(conn.execute(request, timeout=30.0, retries=0))
    queued = await asyncio.wait_for(session.tx_queue.get(), timeout=1.0)
    session.tx_queue.task_done()

    b_future = asyncio.get_running_loop().create_future()
    session.expected_responses[shape] = b_future

    queued.sent.set_exception(ConnectionLost("test drop"))
    with pytest.raises(ConnectionLost):
        await asyncio.wait_for(caller_a, timeout=1.0)

    assert session.expected_responses.get(shape) is b_future
    assert not b_future.done()


# ---------------------------------------------------------------------------
# pacing
# ---------------------------------------------------------------------------


def test_pacing_defaults_and_overrides():
    """message_spacing and tx_jitter are the wire-pacing knobs (issue #71)."""
    from givenergy_modbus.connection import GivEnergyConnection, GivEnergyParams

    default = GivEnergyConnection(GivEnergyParams(host="foo", port=4321))
    assert default.message_spacing == 0.25
    assert default.tx_jitter == 0.1

    custom = GivEnergyConnection(GivEnergyParams(host="foo", port=4321), message_spacing=0.5, tx_jitter=0.0)
    assert custom.message_spacing == 0.5
    assert custom.tx_jitter == 0.0


async def test_inter_frame_gap_is_at_least_message_spacing():
    """The gap between two frames never falls below message_spacing.

    The Pacer measures from the end of the previous send, and the jitter sleep
    happens inside the paced block, so jitter only ever lengthens the gap —
    preserving the historic floor.
    """
    conn = make_connection(message_spacing=0.06, tx_jitter=0)
    session = prime_session(conn)
    writes: list[float] = []
    session.writer.write = MagicMock(side_effect=lambda _: writes.append(asyncio.get_running_loop().time()))

    loop = asyncio.get_running_loop()
    frames = [_QueuedFrame(b"frame", 0x32, loop.create_future(), loop.create_future()) for _ in range(3)]
    for frame in frames:
        session.tx_queue.put_nowait(frame)

    producer = asyncio.create_task(conn._produce(session))
    try:
        await asyncio.wait_for(asyncio.gather(*(f.sent for f in frames)), timeout=2.0)
    finally:
        producer.cancel()

    gaps = [b - a for a, b in zip(writes, writes[1:], strict=False)]
    assert all(gap >= 0.06 for gap in gaps), f"a gap fell below message_spacing: {gaps}"


async def test_jitter_lengthens_the_gap():
    """Jitter is additive on top of message_spacing, never subtractive."""
    conn = make_connection(message_spacing=0.02, tx_jitter=0.05)
    session = prime_session(conn)
    writes: list[float] = []
    session.writer.write = MagicMock(side_effect=lambda _: writes.append(asyncio.get_running_loop().time()))

    loop = asyncio.get_running_loop()
    frames = [_QueuedFrame(b"frame", 0x32, loop.create_future(), loop.create_future()) for _ in range(2)]
    for frame in frames:
        session.tx_queue.put_nowait(frame)

    with patch("givenergy_modbus.connection.random.uniform", return_value=0.05):
        producer = asyncio.create_task(conn._produce(session))
        try:
            await asyncio.wait_for(asyncio.gather(*(f.sent for f in frames)), timeout=2.0)
        finally:
            producer.cancel()

    assert writes[1] - writes[0] >= 0.02 + 0.05


async def test_per_unit_spacing_applies_on_top_of_the_connection_gap():
    """A slow device can be paced harder than the link as a whole."""
    conn = make_connection(message_spacing=0, tx_jitter=0)
    conn.set_unit_spacing(0x33, 0.06)
    session = prime_session(conn)
    writes: list[tuple[int, float]] = []

    loop = asyncio.get_running_loop()
    frames = [_QueuedFrame(b"frame", 0x33, loop.create_future(), loop.create_future()) for _ in range(2)]
    session.writer.write = MagicMock(side_effect=lambda _: writes.append((0x33, loop.time())))
    for frame in frames:
        session.tx_queue.put_nowait(frame)

    producer = asyncio.create_task(conn._produce(session))
    try:
        await asyncio.wait_for(asyncio.gather(*(f.sent for f in frames)), timeout=2.0)
    finally:
        producer.cancel()

    assert writes[1][1] - writes[0][1] >= 0.06


# ---------------------------------------------------------------------------
# byte tap
# ---------------------------------------------------------------------------


async def test_producer_emits_tx_frames_to_the_byte_tap():
    """A frame sent while a tap is installed is teed to it.

    The tap call sits inside the write/drain try block; it swallows its own
    errors, so it can never trip the OSError teardown arm.
    """
    conn = make_connection()
    session = prime_session(conn)
    session.writer.is_closing.side_effect = [False, True]  # one loop pass, then exit
    session.closing = True  # quiet DEBUG exit; no teardown side effects in play
    tapped: list[tuple[str, bytes]] = []
    conn.set_byte_tap(lambda direction, data: tapped.append((direction, data)))
    session.tx_queue.put_nowait(_QueuedFrame(b"raw-frame", 0x32))

    await asyncio.wait_for(conn._produce(session), timeout=2.0)

    assert tapped == [("tx", b"raw-frame")]
    session.writer.write.assert_called_once_with(b"raw-frame")


async def test_a_raising_byte_tap_cannot_break_the_link(caplog):
    conn = make_connection()
    session = prime_session(conn)
    session.writer.is_closing.side_effect = [False, True]
    session.closing = True

    def boom(direction, data):
        raise RuntimeError("tap blew up")

    conn.set_byte_tap(boom)
    session.tx_queue.put_nowait(_QueuedFrame(b"raw-frame", 0x32))

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        await asyncio.wait_for(conn._produce(session), timeout=2.0)

    assert "byte tap raised" in caplog.text
    session.writer.write.assert_called_once_with(b"raw-frame")
