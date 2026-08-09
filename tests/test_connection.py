"""Tests for the modbus-connection transport backend.

Split in two: the Protocol-conformance and error-mapping tests run against a
tiny scripted server, and the end-to-end tests drive a real ``MockPlant``
seeded from a wire capture over a real socket.
"""

import asyncio
from pathlib import Path

import pytest
from modbus_connection import (
    IllegalDataAddressError,
    IllegalFunctionError,
    ModbusConnectionError,
    ModbusTimeoutError,
    ModbusUnit,
)

from givenergy_modbus.connection import (
    MAX_REGISTERS_PER_READ,
    GivEnergyConnection,
    GivEnergyParams,
    GivEnergyUnit,
)
from givenergy_modbus.exceptions import ConnectionLost
from givenergy_modbus.pdu import HeartbeatRequest, ReadInputRegistersRequest, ReadInputRegistersResponse
from givenergy_modbus.testing.mock_plant import MockPlant

CAPTURES = Path(__file__).parent / "fixtures" / "captures"
HYBRID_CAPTURE = CAPTURES / "hybrid_2_bat_a" / "hybrid_gen1_arm449_givbat82_givbat95gen3_60min.log"

pytestmark = pytest.mark.timeout(20)

# Device addresses in the hybrid capture the mock plant serves: 0x31 is the
# inverter, 0x32/0x33 the two battery BMSes. 0x7f is served by nothing, so the
# mock stays silent on it exactly as absent hardware does.
INVERTER = 0x31
BATTERY = 0x32
ABSENT = 0x7F


@pytest.fixture
async def plant_server():
    """A MockPlant serving a two-battery hybrid capture on a real socket."""
    mock = MockPlant.from_capture(HYBRID_CAPTURE)
    host, port = await mock.start()
    try:
        yield mock, host, port
    finally:
        await mock.aclose()


@pytest.fixture
async def connection(plant_server):
    """A connection to the mock plant, paced fast so tests don't crawl."""
    _, host, port = plant_server
    conn = GivEnergyConnection(GivEnergyParams(host=host, port=port), message_spacing=0, tx_jitter=0)
    try:
        yield conn
    finally:
        await conn.close()


def test_unit_satisfies_the_protocol():
    """A GivEnergyUnit is structurally a ModbusUnit — the whole point of the seam."""
    conn = GivEnergyConnection(GivEnergyParams(host="nowhere"))
    unit = conn.for_unit(0x32)
    assert isinstance(unit, ModbusUnit)
    assert isinstance(unit, GivEnergyUnit)
    assert not [name for name in ModbusUnit.__protocol_attrs__ if not hasattr(unit, name)]


def test_params_endpoint_identity():
    """endpoint() gives the same 'same device' identity the library's params do."""
    assert GivEnergyParams(host="Inverter.Local").endpoint == ("tcp", "inverter.local", 8899)
    assert GivEnergyParams(host="a", port=1) != GivEnergyParams(host="a", port=2)


async def test_connects_on_demand_and_reads(connection):
    """No explicit connect(): the first read establishes the link."""
    assert not connection.connected
    unit = connection.for_unit(BATTERY)
    values = await unit.read_input_registers(60, 60)
    assert connection.connected
    assert len(values) == 60
    assert any(values), "the BMS page came back all zeros"


async def test_read_wider_than_the_device_cap_is_split(connection):
    """GivEnergy caps a read at 60 registers; the unit splits a wider request."""
    reads: list[tuple[int, int]] = []
    connection.add_frame_listener(
        lambda pdu: (
            reads.append((pdu.base_register, pdu.register_count))
            if isinstance(pdu, ReadInputRegistersResponse)
            else None
        )
    )
    unit = connection.for_unit(INVERTER)
    values = await unit.read_input_registers(0, 120)
    assert len(values) == 120
    assert [count for _, count in reads] == [MAX_REGISTERS_PER_READ, MAX_REGISTERS_PER_READ]


async def test_absent_bank_raises_illegal_data_address(connection):
    """A device error response is a definitive absence, not a timeout."""
    unit = GivEnergyUnit(connection, INVERTER, timeout=1.0, retries=0)
    with pytest.raises(IllegalDataAddressError):
        # A single-phase hybrid has no three-phase bank at IR(1000).
        await unit.read_input_registers(1000, 60)


async def test_silent_device_raises_modbus_timeout(connection):
    """A device that never answers raises the library's timeout error."""
    unit = GivEnergyUnit(connection, ABSENT, timeout=0.3, retries=0)
    with pytest.raises(ModbusTimeoutError):
        await unit.read_input_registers(60, 60)


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("read_coils", (0, 1)),
        ("read_discrete_inputs", (0, 1)),
        ("write_coil", (0, True)),
        ("write_coils", (0, [True])),
        ("read_exception_status", ()),
        ("report_server_id", ()),
        ("mask_write_register", (0, 0, 0)),
        ("read_write_registers", (0, 1, 0, [1])),
        ("read_fifo_queue", (0,)),
        ("read_device_identification", ()),
        ("read_file_record", (0, 0, 1)),
        ("write_file_record", (0, 0, [1])),
        ("diagnostics", (0,)),
        ("get_comm_event_counter", ()),
        ("get_comm_event_log", ()),
    ],
)
async def test_unsupported_function_codes_raise_illegal_function(method, args):
    """The sixteen function codes this hardware lacks say so, honestly."""
    conn = GivEnergyConnection(GivEnergyParams(host="nowhere"))
    unit = conn.for_unit(0x32)
    with pytest.raises(IllegalFunctionError):
        await getattr(unit, method)(*args)


async def test_multi_register_write_is_refused_but_single_is_served():
    """FC16 doesn't exist here; a one-register FC16 degrades to FC06 rather than failing."""
    conn = GivEnergyConnection(GivEnergyParams(host="nowhere"))
    unit = conn.for_unit(INVERTER)
    with pytest.raises(IllegalFunctionError):
        await unit.write_registers(35, [1, 2])
    # The single-register path reaches the wire (and fails on the absent host),
    # proving it was translated rather than refused outright.
    with pytest.raises(ModbusConnectionError):
        await unit.write_registers(35, [1])


async def test_heartbeat_is_answered(plant_server):
    """Three unanswered heartbeats and the dongle hangs up; so we answer them."""
    _, host, port = plant_server
    conn = GivEnergyConnection(GivEnergyParams(host=host, port=port), message_spacing=0, tx_jitter=0)
    seen: list[object] = []
    conn.add_frame_listener(seen.append)
    tapped: list[bytes] = []
    conn.set_byte_tap(lambda direction, data: tapped.append(data) if direction == "tx" else None)
    try:
        await conn.connect()
        session = conn._client
        # Feed a heartbeat in as though the dongle had sent it.
        request = HeartbeatRequest(data_adapter_serial_number="WF1234G567", data_adapter_type=32)
        session.reader.feed_data(request.encode())
        await asyncio.sleep(0.1)
        assert any(isinstance(pdu, HeartbeatRequest) for pdu in seen)
        assert tapped, "no heartbeat response reached the wire"
    finally:
        await conn.close()


async def test_connection_lost_fires_callbacks_and_flips_connected(plant_server):
    """An unexpected drop clears `connected` and notifies subscribers."""
    _, host, port = plant_server
    conn = GivEnergyConnection(GivEnergyParams(host=host, port=port), message_spacing=0, tx_jitter=0)
    lost = asyncio.Event()
    unit = conn.for_unit(INVERTER)
    unit.on_connection_lost(lost.set)
    try:
        await conn.connect()
        assert conn.connected
        conn._client.reader.feed_eof()
        await asyncio.wait_for(lost.wait(), timeout=2)
        assert not conn.connected
        assert not unit.connected
    finally:
        await conn.close()


async def test_reconnects_after_a_drop(connection):
    """The link is re-established on the next request, with no explicit reconnect."""
    unit = connection.for_unit(INVERTER)
    assert await unit.read_input_registers(0, 60)
    connection._client.reader.feed_eof()
    await asyncio.sleep(0.1)
    assert not connection.connected
    assert await unit.read_input_registers(0, 60)
    assert connection.connected


async def test_in_flight_request_fails_with_a_connection_error(connection):
    """A drop mid-request surfaces as the library's link-is-down error, not a timeout."""
    unit = GivEnergyUnit(connection, ABSENT, timeout=5.0, retries=0)
    await connection.connect()
    reading = asyncio.create_task(unit.read_input_registers(60, 60))
    await asyncio.sleep(0.1)
    connection._client.reader.feed_eof()
    with pytest.raises(ModbusConnectionError):
        await reading


async def test_message_spacing_paces_the_wire(plant_server):
    """The base class's Pacer supplies the inter-frame gap the hardware needs."""
    _, host, port = plant_server
    conn = GivEnergyConnection(GivEnergyParams(host=host, port=port), message_spacing=0.2, tx_jitter=0)
    try:
        unit = conn.for_unit(INVERTER)
        started = asyncio.get_running_loop().time()
        await asyncio.gather(*(unit.read_holding_registers(base, 60) for base in (0, 60, 120)))
        elapsed = asyncio.get_running_loop().time() - started
        # Three frames at a 0.2s gap can't be done in under 0.4s.
        assert elapsed >= 0.4
    finally:
        await conn.close()


async def test_per_unit_spacing_is_settable(connection):
    """set_message_spacing() reaches the connection's pacer."""
    unit = connection.for_unit(0x33)
    unit.set_message_spacing(1.5)
    assert connection._pacer._unit_spacing[0x33] == 1.5
    unit.set_message_spacing(0)
    assert 0x33 not in connection._pacer._unit_spacing


async def test_close_is_not_a_connection_loss(plant_server):
    """Deliberate teardown must not fire the lost callbacks."""
    _, host, port = plant_server
    conn = GivEnergyConnection(GivEnergyParams(host=host, port=port), message_spacing=0, tx_jitter=0)
    fired = []
    conn.on_connection_lost(lambda: fired.append(True))
    await conn.connect()
    await conn.close()
    await asyncio.sleep(0.1)
    assert not fired
    assert not conn.connected


async def test_frame_listener_sees_unsolicited_responses(connection):
    """Every decoded frame reaches the listeners, whether or not we asked for it."""
    seen = []
    unsubscribe = connection.add_frame_listener(seen.append)
    unit = connection.for_unit(INVERTER)
    await unit.read_input_registers(0, 60)
    assert seen
    unsubscribe()
    seen.clear()
    await unit.read_input_registers(0, 60)
    assert not seen


async def test_a_raising_listener_cannot_break_the_link(connection, caplog):
    """A listener is a tee; its failure is logged, never propagated."""

    def boom(pdu):
        raise RuntimeError("listener blew up")

    connection.add_frame_listener(boom)
    unit = connection.for_unit(INVERTER)
    assert await unit.read_input_registers(0, 60)
    assert "frame listener raised" in caplog.text


async def test_connection_error_before_connect(plant_server):
    """Connecting to nothing raises the library's connection error."""
    conn = GivEnergyConnection(GivEnergyParams(host="127.0.0.1", port=1), timeout=1.0)
    unit = conn.for_unit(INVERTER)
    with pytest.raises(ModbusConnectionError):
        await unit.read_input_registers(0, 60)
    await conn.close()


async def test_execute_surfaces_the_raw_pdu(connection):
    """The raw-PDU escape hatch is still there for what the Protocol can't express."""
    response = await connection.execute(
        ReadInputRegistersRequest(base_register=0, register_count=60, device_address=INVERTER),
        timeout=2.0,
        retries=0,
    )
    assert response.base_register == 0
    assert len(response.register_values) == 60


async def test_closed_connection_refuses_work(connection):
    """close() is permanent; disconnect() is the recycle-the-link primitive."""
    unit = connection.for_unit(INVERTER)
    await unit.read_input_registers(0, 60)
    await connection.disconnect()
    assert not connection.connected
    assert await unit.read_input_registers(0, 60)  # reconnects
    await connection.close()
    with pytest.raises(ModbusConnectionError):
        await unit.read_input_registers(0, 60)


async def test_connection_lost_is_both_error_families(connection):
    """ConnectionLost answers to the old names and the library's."""
    err = ConnectionLost("gone")
    assert isinstance(err, ModbusConnectionError)
    assert isinstance(err, TimeoutError)
