"""Client-level behaviour.

The transport this file used to exercise — the socket, the pump tasks, the
transmit queue and the retry loop — moved into
:class:`~givenergy_modbus.connection.GivEnergyConnection`; its tests moved with
it, to ``tests/test_transport.py`` and ``tests/test_connection.py``. What is
left here is what the client itself is responsible for: owning (or not owning) a
connection, ingesting decoded frames into the plant, and attributing a consumed
retry to the right device.
"""

import asyncio
import datetime

import pytest

from givenergy_modbus.client.client import Client
from givenergy_modbus.connection import GivEnergyConnection, GivEnergyParams
from givenergy_modbus.exceptions import CommunicationError, ConnectionLost
from givenergy_modbus.model import TimeSlot
from givenergy_modbus.model.plant import Plant
from givenergy_modbus.pdu.write_registers import WriteHoldingRegisterRequest, WriteHoldingRegisterResponse
from tests.transport import connection_with_pumps, prime_session, stop_pumps


def test_timeslot():
    ts = TimeSlot(datetime.time(4, 5), datetime.time(9, 8))
    assert ts == TimeSlot(start=datetime.time(4, 5), end=datetime.time(9, 8))
    assert ts == TimeSlot(datetime.time(4, 5), datetime.time(9, 8))
    assert ts == TimeSlot.from_components(4, 5, 9, 8)
    assert ts == TimeSlot.from_repr(405, 908)
    assert ts == TimeSlot.from_repr("405", "908")
    assert TimeSlot(datetime.time(0, 2), datetime.time(0, 2)) == TimeSlot.from_repr(2, 2)
    with pytest.raises(ValueError, match="hour must be in 0..23"):
        TimeSlot.from_repr(999999, 999999)
    with pytest.raises(ValueError, match="minute must be in 0..59"):
        TimeSlot.from_repr(999, 888)
    with pytest.raises(ValueError, match="hour must be in 0..23"):
        TimeSlot.from_components(99, 88, 77, 66)
    with pytest.raises(ValueError, match="minute must be in 0..59"):
        TimeSlot.from_components(11, 22, 11, 66)

    ts = TimeSlot(datetime.time(12, 34), datetime.time(23, 45))
    assert ts == TimeSlot(start=datetime.time(12, 34), end=datetime.time(23, 45))
    assert ts == TimeSlot(datetime.time(12, 34), datetime.time(23, 45))
    assert ts == TimeSlot.from_components(12, 34, 23, 45)
    assert ts == TimeSlot.from_repr(1234, 2345)
    assert ts == TimeSlot.from_repr("1234", "2345")
    with pytest.raises(ValueError, match="hour must be in 0..23"):
        assert ts == TimeSlot.from_components(43, 21, 54, 32)
    with pytest.raises(ValueError, match="hour must be in 0..23"):
        assert ts == TimeSlot.from_repr(4321, 5432)
    with pytest.raises(ValueError, match="hour must be in 0..23"):
        assert ts == TimeSlot.from_repr("4321", "5432")


# ---------------------------------------------------------------------------
# connection ownership
# ---------------------------------------------------------------------------


def test_for_host_builds_and_owns_a_connection():
    client = Client.for_host(host="foo", port=4321)
    assert isinstance(client.connection, GivEnergyConnection)
    assert (client.host, client.port) == ("foo", 4321)
    assert client._owns_connection is True


def test_a_supplied_connection_is_not_owned():
    """The shared-connection model: a consumer must not close a link it was lent."""
    connection = GivEnergyConnection(GivEnergyParams(host="foo", port=4321))
    assert Client(connection)._owns_connection is False


async def test_close_leaves_a_borrowed_connection_open():
    """Closing one consumer must not pull the link out from under the others."""
    connection = GivEnergyConnection(GivEnergyParams(host="foo", port=4321))
    session = prime_session(connection)
    first, second = Client(connection), Client(connection)

    await first.close()

    assert connection.connected  # still up for the second consumer
    session.writer.close.assert_not_called()
    await second.close()
    await connection.close()


async def test_close_closes_an_owned_connection():
    client = Client.for_host(host="foo", port=4321)
    session = prime_session(client.connection)

    await client.close()

    assert not client.connected
    session.writer.close.assert_called_once()


async def test_close_stops_ingesting_frames():
    """A closed client must not keep writing into its plant off a shared link."""
    connection = GivEnergyConnection(GivEnergyParams(host="foo", port=4321), message_spacing=0, tx_jitter=0)
    session = connection_with_pumps(connection)
    client = Client(connection)
    frame = WriteHoldingRegisterResponse(inverter_serial_number="", register=35, value=20).encode()

    await client.close()
    session.reader.feed_data(frame)
    await asyncio.sleep(0.05)

    # Plant() pre-seeds an empty cache for 0x32, so check that nothing landed in it.
    assert not any(client.plant.register_caches.values())
    await stop_pumps(session)
    await connection.close()


def test_connected_reflects_the_connection():
    client = Client.for_host(host="foo", port=4321)
    assert client.connected is False
    prime_session(client.connection)
    assert client.connected is True


# ---------------------------------------------------------------------------
# plant wiring
# ---------------------------------------------------------------------------


async def test_decoded_frames_reach_the_plant():
    """Every register response the connection decodes is committed to the plant."""
    client = Client.for_host(host="foo", port=4321, tx_message_wait=0, tx_jitter=0)
    session = connection_with_pumps(client.connection)
    request = WriteHoldingRegisterRequest(register=35, value=20)

    sending = asyncio.create_task(client.send_request_and_await_response(request, timeout=1.0, retries=0))
    await asyncio.sleep(0)
    session.reader.feed_data(WriteHoldingRegisterResponse(inverter_serial_number="", register=35, value=20).encode())

    response = await asyncio.wait_for(sending, timeout=2)
    assert response.register == 35
    assert client.plant.register_caches, "the response never reached the plant"
    await stop_pumps(session)


async def test_retry_count_attributes_a_consumed_retry_to_its_device():
    """The client supplies the per-device attribution the transport can't know (#284)."""
    client = Client.for_host(host="foo", port=4321, tx_message_wait=0, tx_jitter=0)
    session = prime_session(client.connection)
    request = WriteHoldingRegisterRequest(register=35, value=20)
    shape = request.expected_response().shape_hash()

    async def drain_and_respond_on_retry():
        attempt = 0
        while True:
            queued = await session.tx_queue.get()
            session.tx_queue.task_done()
            attempt += 1
            if queued.sent is not None and not queued.sent.done():
                queued.sent.set_result(True)
            if attempt >= 2:  # the first attempt times out; the retry succeeds
                await asyncio.sleep(0)
                future = session.expected_responses.get(shape)
                if future is not None and not future.done():
                    future.set_result(WriteHoldingRegisterResponse(inverter_serial_number="", register=35, value=20))

    drainer = asyncio.create_task(drain_and_respond_on_retry())
    try:
        await client.send_request_and_await_response(request, timeout=0.02, retries=2, retry_delay=0)
    finally:
        drainer.cancel()

    assert client.plant.retry_count == {request.device_address: 1}


async def test_probe_retries_are_not_attributed():
    """Absent-device probes pass warn_timeout=False and must not pollute retry_count (#284)."""
    client = Client.for_host(host="foo", port=4321, tx_message_wait=0, tx_jitter=0)
    session = prime_session(client.connection)

    async def drain():
        while True:
            queued = await session.tx_queue.get()
            session.tx_queue.task_done()
            if queued.sent is not None and not queued.sent.done():
                queued.sent.set_result(True)

    drainer = asyncio.create_task(drain())
    try:
        with pytest.raises(TimeoutError):
            await client.send_request_and_await_response(
                WriteHoldingRegisterRequest(register=35, value=20),
                timeout=0.02,
                retries=1,
                retry_delay=0,
                warn_timeout=False,
            )
    finally:
        drainer.cancel()

    assert client.plant.retry_count == {}


# ---------------------------------------------------------------------------
# plant construction options
# ---------------------------------------------------------------------------


def test_client_forwards_splice_heal_seconds():
    """splice_heal_seconds overrides the plant's value only when explicitly given (#286)."""
    assert Client.for_host(host="foo", port=4321).plant.splice_heal_seconds == 900.0  # Plant default stands
    assert Client.for_host(host="foo", port=4321, splice_heal_seconds=42.0).plant.splice_heal_seconds == 42.0
    # An injected plant's own value is preserved when the param is omitted (not clobbered).
    assert Client.for_host(host="foo", port=4321, plant=Plant(splice_heal_seconds=123.0)).plant.splice_heal_seconds == (
        123.0
    )
    # An explicit param still wins over an injected plant's value.
    assert (
        Client.for_host(
            host="foo", port=4321, plant=Plant(splice_heal_seconds=123.0), splice_heal_seconds=7.0
        ).plant.splice_heal_seconds
        == 7.0
    )


def test_client_forwards_splice_reject_heal_seconds():
    """splice_reject_heal_seconds overrides the plant's value only when given (#299)."""
    assert Client.for_host(host="foo", port=4321).plant.splice_reject_heal_seconds is None  # default: disabled
    assert (
        Client.for_host(host="foo", port=4321, splice_reject_heal_seconds=300.0).plant.splice_reject_heal_seconds
        == 300.0
    )
    injected = Plant(splice_reject_heal_seconds=600.0)
    assert Client.for_host(host="foo", port=4321, plant=injected).plant.splice_reject_heal_seconds == 600.0
    assert (
        Client.for_host(
            host="foo", port=4321, plant=Plant(splice_reject_heal_seconds=600.0), splice_reject_heal_seconds=300.0
        ).plant.splice_reject_heal_seconds
        == 300.0
    )


def test_client_forwards_pacing_to_the_connection():
    """tx_message_wait and tx_jitter reach the connection's pacing knobs (issue #71)."""
    default = Client.for_host(host="foo", port=4321)
    assert default.connection.message_spacing == 0.25
    assert default.connection.tx_jitter == 0.1

    custom = Client.for_host(host="foo", port=4321, tx_message_wait=0.5, tx_jitter=0.0)
    assert custom.connection.message_spacing == 0.5
    assert custom.connection.tx_jitter == 0.0


# ---------------------------------------------------------------------------
# the exception compatibility contract
# ---------------------------------------------------------------------------


def test_connection_lost_is_communication_error_and_timeout():
    """Multiple inheritance is the compat contract (#356): typed for new consumers.

    Still a TimeoutError so legacy `except TimeoutError` paths keep working, and
    now also a ModbusConnectionError so the library's own contract holds.
    """
    from modbus_connection import ModbusConnectionError

    exc = ConnectionLost("connection dropped")
    assert isinstance(exc, CommunicationError)
    assert isinstance(exc, TimeoutError)
    assert isinstance(exc, ModbusConnectionError)


def test_connection_lost_caught_by_legacy_timeout_handler():
    """A consumer catching bare TimeoutError (e.g. a released hass coordinator) must catch it."""
    try:
        raise ConnectionLost("connection dropped")
    except TimeoutError as e:
        assert "connection dropped" in str(e)
