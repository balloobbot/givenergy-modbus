"""The Component view must agree with the pydantic view, field for field.

Both are generated from the same ``REGISTER_LUT``, so this is really a test of
the *translation*: that ``GivEnergyField`` selects the right registers in the
right order, composes the converters the way ``RegisterGetter.get`` does, and
applies the same bounds guard. It runs against real wire captures rather than
synthetic registers, so every quirk the hardware actually emits is covered.
"""

from pathlib import Path

import pytest
from modbus_connection.mock import MockModbusConnection, MockModbusUnit

from givenergy_modbus.framer import ClientFramer
from givenergy_modbus.model.aio_battery import AioBatteryModuleRegisterGetter
from givenergy_modbus.model.battery import BatteryRegisterGetter
from givenergy_modbus.model.components import (
    MAX_REGISTERS_PER_READ,
    Battery,
    GivEnergyField,
    InverterHolding,
    Meter,
    components_for,
    modelled_fields,
    restrict_to_banks,
)
from givenergy_modbus.model.ems import EmsRegisterGetter
from givenergy_modbus.model.gateway import GatewayV1RegisterGetter, GatewayV2RegisterGetter
from givenergy_modbus.model.hv_bcu import BcuRegisterGetter, BmuRegisterGetter
from givenergy_modbus.model.inverter import SinglePhaseInverterRegisterGetter
from givenergy_modbus.model.inverter_threephase import ThreePhaseInverterRegisterGetter
from givenergy_modbus.model.meter import MeterProductRegisterGetter, MeterRegisterGetter
from givenergy_modbus.model.register import HR, IR
from givenergy_modbus.model.register_cache import RegisterCache
from givenergy_modbus.pdu import ReadRegistersResponse
from givenergy_modbus.testing.mock_plant import _iter_capture_frames, plant_from_capture

CAPTURES = Path(__file__).parent.parent / "fixtures" / "captures"

pytestmark = pytest.mark.timeout(60)

# (capture directory, device address, family, the pydantic getter to agree with).
# Device addresses come from the captures themselves — see the plant topology each
# one records.
CASES = [
    ("hybrid_2_bat_a", 0x31, "inverter", SinglePhaseInverterRegisterGetter),
    ("hybrid_2_bat_a", 0x32, "battery", BatteryRegisterGetter),
    ("hybrid_gen2_1_bat_a", 0x11, "inverter", SinglePhaseInverterRegisterGetter),
    ("hybrid_gen2_1_bat_a", 0x31, "battery", BatteryRegisterGetter),
    ("hybrid_gen2_1_bat_a", 0x01, "meter", MeterRegisterGetter),
    ("aio_a", 0x11, "inverter", SinglePhaseInverterRegisterGetter),
    ("aio_a", 0x50, "aio_battery_module", AioBatteryModuleRegisterGetter),
    ("aio_a", 0x70, "hv_bcu", BcuRegisterGetter),
    ("aio_a", 0x03, "meter", MeterRegisterGetter),
    ("three_phase_hv_a", 0x11, "three_phase_inverter", ThreePhaseInverterRegisterGetter),
    ("three_phase_hv_a", 0x70, "hv_bcu", BcuRegisterGetter),
    ("three_phase_hv_a", 0x50, "hv_bmu", BmuRegisterGetter),
    ("three_phase_hv_a", 0x03, "meter", MeterRegisterGetter),
    ("hybrid_hv_gen3_a", 0x70, "hv_bcu", BcuRegisterGetter),
    ("hybrid_hv_gen3_a", 0x50, "hv_bmu", BmuRegisterGetter),
    ("ems_2_inv_3_bat_a", 0x11, "ems", EmsRegisterGetter),
    ("ems_2_inv_3_bat_a", 0x31, "inverter", SinglePhaseInverterRegisterGetter),
    ("ems_2_inv_3_bat_a", 0x30, "battery", BatteryRegisterGetter),
    ("ems_2_inv_3_bat_a", 0x07, "meter", MeterRegisterGetter),
    ("gateway_2aio_a", 0x11, "gateway_v1", GatewayV1RegisterGetter),
    ("gateway_2aio_a", 0x11, "gateway_v2", GatewayV2RegisterGetter),
    ("gateway_2aio_a", 0x01, "aio_battery_module", AioBatteryModuleRegisterGetter),
]


def _cache(capture: str, address: int) -> RegisterCache:
    """The register cache one capture recorded for one device address."""
    plant = plant_from_capture(*sorted((CAPTURES / capture).glob("*.log")))
    cache = plant.register_caches.get(address)
    assert cache, f"{capture} has no registers for device 0x{address:02x}"
    return cache


def _seed(unit: MockModbusUnit, cache: RegisterCache) -> dict[str, set[int]]:
    """Load a capture's registers into a mock unit; return what each space serves."""
    served: dict[str, set[int]] = {"holding": set(), "input": set()}
    for register, value in cache.items():
        if isinstance(register, HR):
            unit.holding[register.index] = value
            served["holding"].add(register.index)
        elif isinstance(register, IR):
            unit.input[register.index] = value
            served["input"].add(register.index)
    return served


def _addresses(field: GivEnergyField) -> set[int]:
    """The registers a field actually reads, ignoring any hole it spans."""
    return {field.address + offset for offset in field.offsets}


@pytest.mark.parametrize(("capture", "address", "family", "getter"), CASES, ids=lambda v: getattr(v, "__name__", v))
async def test_component_agrees_with_the_pydantic_model(capture, address, family, getter):
    """Every field the capture populated decodes identically through both views."""
    cache = _cache(capture, address)
    unit = MockModbusConnection().for_unit(address)
    served = _seed(unit, cache)

    components, group = components_for(family, unit)
    await group.async_update()

    expected = getter(cache).build()
    compared = 0
    for component in components:
        space = component.register_space
        for name, field in modelled_fields(component).items():
            if not _addresses(field) <= served[space]:
                continue  # the device never answered for these registers
            assert getattr(component, name) == expected[name], f"{family}.{name} disagrees"
            compared += 1
    assert compared, f"{capture} 0x{address:02x} populated no {family} fields"


@pytest.mark.parametrize(("capture", "address", "family", "getter"), CASES, ids=lambda v: getattr(v, "__name__", v))
async def test_reads_stay_within_the_device_read_limit(capture, address, family, getter):
    """No planned block exceeds the 60 registers GivEnergy's read function accepts."""
    unit = MockModbusConnection().for_unit(address)
    _seed(unit, _cache(capture, address))
    _, group = components_for(family, unit)

    await group.async_update()

    assert unit.read_events, "the component planned no reads at all"
    oversized = [event for event in unit.read_events if event.count > MAX_REGISTERS_PER_READ]
    assert not oversized, f"blocks wider than the device accepts: {oversized}"


async def test_a_battery_is_one_read_of_its_own_bank():
    """A BMS is a single block, trimmed to the registers actually modelled.

    The client polls this bank as IR(60,60); the planner trims to IR(60,56)
    because nothing is modelled above IR(115). The device serves either — its
    own traffic includes reads as narrow as one register at an arbitrary base —
    so trimming is free and the narrower read is strictly better.
    """
    unit = MockModbusConnection().for_unit(0x32)
    _seed(unit, _cache("hybrid_2_bat_a", 0x32))
    _, group = components_for("battery", unit)

    await group.async_update()

    assert [(e.register_type, e.address, e.count) for e in unit.read_events] == [("input", 60, 56)]


async def test_a_meter_is_one_trimmed_read():
    """A meter's fields sit in IR(60-88); the client polls IR(60,30), we read IR(60,29)."""
    unit = MockModbusConnection().for_unit(0x03)
    _seed(unit, _cache("aio_a", 0x03))
    _, group = components_for("meter", unit)

    await group.async_update()

    assert [(e.address, e.count) for e in unit.read_events] == [(60, 29)]


async def test_a_split_family_reads_both_register_spaces():
    """An inverter's two components address FC03 and FC04 through one group call."""
    unit = MockModbusConnection().for_unit(0x31)
    _seed(unit, _cache("hybrid_2_bat_a", 0x31))
    _, group = components_for("inverter", unit)

    await group.async_update()

    spaces = {event.register_type for event in unit.read_events}
    assert spaces == {"holding", "input"}


def test_every_declared_field_is_inside_its_readable_ranges():
    """A field outside the map would be unreadable; component_class refuses to build one."""
    for family in ("inverter", "three_phase_inverter", "ems", "battery", "gateway_v2", "hv_bcu"):
        unit = MockModbusConnection().for_unit(1)
        components, _ = components_for(family, unit)
        for component in components:
            readable = {a for low, high in component.register_ranges for a in range(low, high + 1)}
            for name, field in modelled_fields(component).items():
                window = set(range(field.address, field.address + field.count))
                assert window <= readable, f"{family}.{name} reads outside the declared map"


def test_non_contiguous_and_reordered_fields_survive_translation():
    """The two shapes RegisterField cannot express natively are still modelled.

    ``firmware_version`` skips a register; the Gateway's 32-bit totals name their
    high word first at the higher address. Both are offset selections over a
    contiguous window.
    """
    firmware = InverterHolding.declared_fields["firmware_version"]
    assert (firmware.address, firmware.count, firmware.offsets) == (19, 3, (0, 2))

    from givenergy_modbus.model.components import GatewayV2

    total = GatewayV2.declared_fields["e_grid_import_total"]
    assert (total.address, total.count, total.offsets) == (1641, 2, (1, 0))


def test_bounds_suppress_an_implausible_value_but_not_an_unset_bank():
    """The #82 corruption guard travels with the field."""
    voltage = Meter.declared_fields["v_phase_1"]  # deci-volts, bounded to [0, 500]
    assert voltage.decode([2400]) == 240.0
    assert voltage.decode([60000]) is None  # 6000 V is not a thing
    assert voltage.decode([0]) == 0.0  # an all-zero bank is "unset", not out of bounds


def test_restrict_to_banks_narrows_a_component_to_what_a_model_serves():
    """Capability gating: a hybrid that times out on HR(300-359) drops those fields."""
    unit = MockModbusConnection().for_unit(0x11)
    component = InverterHolding(unit)
    assert "export_priority" in component.declared_fields  # lives at HR(311)

    restrict_to_banks(component, [(0, 59), (60, 119), (120, 179)])

    assert component.export_priority is None
    kept = {field.address for field in modelled_fields(component).values()}
    assert kept and max(kept) < 180


async def test_a_restricted_component_reads_only_the_banks_it_kept():
    """Narrowing reshapes the plan, not just the attribute set."""
    unit = MockModbusConnection().for_unit(0x31)
    _seed(unit, _cache("hybrid_2_bat_a", 0x31))
    component = InverterHolding(unit)
    restrict_to_banks(component, [(0, 59)])

    await component.async_update()

    assert all(event.address < 60 for event in unit.read_events), unit.read_events


async def test_the_hardware_serves_arbitrary_bases_and_counts():
    """Why the planner is allowed to trim a block to the fields inside it.

    The client's own poll only ever asks for whole banks, which makes it look as
    though the device answers in fixed pages and a trimmed read would be
    refused. The captures say otherwise: every recorded response echoes the
    shape it was asked for, and the successful ones include single registers at
    arbitrary bases and a range of odd counts. So a narrower block is safe, and
    the planner's trimming is a straightforward win.
    """
    served: set[tuple[int, int]] = set()
    for capture in CAPTURES.iterdir():
        for log in capture.glob("*.log"):
            framer = ClientFramer()
            for frame in _iter_capture_frames(log):
                async for pdu in framer.decode(frame):
                    if isinstance(pdu, ReadRegistersResponse) and not pdu.error:
                        served.add((pdu.base_register, pdu.register_count))

    unaligned = {base for base, _ in served if base % 60}
    assert unaligned >= {1110, 1122, 2044, 2070}, f"expected odd bases to be served, got {sorted(unaligned)}"
    assert {count for _, count in served} >= {1, 5, 20, 21, 30, 54, 60}


def test_meter_product_registers_have_no_component():
    """A documented gap: MR (FC 0x16) is a fourth space the model framework lacks.

    ``modbus_connection.model``'s ``RegisterSpace`` is holding or input, so the
    meter's identification block — serial, factory code, hardware and software
    versions — can only be read through the raw PDU surface.
    """
    from givenergy_modbus.model import components

    modelled = set(components.INPUT_ONLY_FAMILIES) | set(components.SPLIT_FAMILIES)
    assert "meter_product" not in modelled
    assert MeterProductRegisterGetter.REGISTER_LUT, "the fields exist; only the space is missing"


def test_battery_component_is_read_only():
    """Writes stay behind the two-gate safety model in client.commands."""
    assert not any(field.writable for field in Battery.declared_fields.values())
    assert not any(field.writable for field in InverterHolding.declared_fields.values())
