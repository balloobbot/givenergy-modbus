"""The model layer driven over the real transport, against a real socket.

``tests/model/test_components.py`` proves the field translation against an
in-memory unit. This proves the whole stack: a ``Component`` planning reads,
issuing them through :class:`~givenergy_modbus.connection.GivEnergyUnit`, over
GivEnergy's Transparent framing, to a ``MockPlant`` replaying a wire capture —
with no library code in between that knows GivEnergy exists.
"""

import asyncio
from pathlib import Path

import pytest
from modbus_connection import ModbusExceptionError

from givenergy_modbus.connection import GivEnergyConnection, GivEnergyParams
from givenergy_modbus.model.battery import BatteryRegisterGetter
from givenergy_modbus.model.components import (
    Battery,
    InverterHolding,
    components_for,
    modelled_fields,
    restrict_to_banks,
)
from givenergy_modbus.pdu import ReadRegistersResponse
from givenergy_modbus.testing.mock_plant import MockPlant, plant_from_capture

CAPTURE = (
    Path(__file__).parent
    / "fixtures"
    / "captures"
    / "hybrid_2_bat_a"
    / "hybrid_gen1_arm449_givbat82_givbat95gen3_60min.log"
)

INVERTER = 0x31
BATTERY = 0x32

# The banks a Gen-1 hybrid actually serves, as detect() would establish them.
HYBRID_HOLDING_BANKS = [(0, 59), (60, 119), (120, 179)]

pytestmark = pytest.mark.timeout(30)


@pytest.fixture
async def connection():
    """A connection to a mock plant replaying a two-battery hybrid capture."""
    mock = MockPlant.from_capture(CAPTURE)
    host, port = await mock.start()
    conn = GivEnergyConnection(GivEnergyParams(host=host, port=port), message_spacing=0, tx_jitter=0)
    try:
        yield conn
    finally:
        await conn.close()
        await mock.aclose()


async def test_a_battery_reads_itself_over_the_wire(connection):
    """One component, one Modbus call, real frames — and the values are right."""
    battery = Battery(connection.for_unit(BATTERY))

    await battery.async_update()

    expected = BatteryRegisterGetter(plant_from_capture(CAPTURE).register_caches[BATTERY]).build()
    assert battery.serial_number == expected["serial_number"]
    assert battery.v_cell_01 == expected["v_cell_01"]
    assert battery.soc == expected["soc"]
    assert battery.soc is not None


async def test_two_batteries_are_two_units_on_one_connection(connection):
    """Device addresses are unit ids; the shared link serialises the two reads."""
    first = Battery(connection.for_unit(BATTERY))
    second = Battery(connection.for_unit(BATTERY + 1))

    await asyncio.gather(first.async_update(), second.async_update())

    assert first.serial_number != second.serial_number
    assert first.soc is not None
    assert second.soc is not None


async def test_an_absent_bank_aborts_the_whole_update(connection):
    """A refused block throws away everything already read — the sharp edge here.

    An inverter component declares every bank any GivEnergy inverter might
    serve, because ``register_ranges`` is a class attribute. A Gen-1 hybrid
    answers three of them and refuses the rest, and one refusal fails the
    update: the fields that *did* come back are discarded with it. Absent banks
    are routine on this hardware — which is why the client's own poll reports
    ``RefreshPartiallySucceeded`` and keeps the partial data.
    """
    inverter = InverterHolding(connection.for_unit(INVERTER))

    with pytest.raises(ModbusExceptionError) as raised:
        await inverter.async_update()

    assert raised.value.block is not None  # which block was refused
    assert inverter.serial_number is None  # HR(0-59) was read, then dropped


async def test_narrowing_to_the_detected_banks_makes_the_read_work(connection):
    """Capability gating is the fix: keep the fields the detected model serves."""
    inverter = InverterHolding(connection.for_unit(INVERTER))
    restrict_to_banks(inverter, HYBRID_HOLDING_BANKS)

    await inverter.async_update()

    assert inverter.serial_number
    assert inverter.battery_charge_limit is not None
    assert modelled_fields(inverter)


async def test_a_narrowed_group_reads_the_client_s_own_request_pattern(connection):
    """The pooled plan reproduces the banks the client polls, and nothing else."""
    unit = connection.for_unit(INVERTER)
    components, group = components_for("inverter", unit)
    holding, input_registers = components
    restrict_to_banks(holding, HYBRID_HOLDING_BANKS)
    restrict_to_banks(input_registers, [(0, 59), (180, 239)])

    # Each answer echoes the shape of the request that asked for it, so the
    # inbound frames are a faithful record of what went out.
    reads: list[tuple[str, int, int]] = []
    connection.add_frame_listener(
        lambda pdu: (
            reads.append((type(pdu).__name__, pdu.base_register, pdu.register_count))
            if isinstance(pdu, ReadRegistersResponse)
            else None
        )
    )
    await group.async_update()

    holding_reads = sorted(base for name, base, _ in reads if "Holding" in name)
    input_reads = sorted(base for name, base, _ in reads if "Input" in name)
    assert holding_reads == [0, 60, 120]
    assert input_reads == [0, 180]
    assert all(count == 60 for _, _, count in reads)
