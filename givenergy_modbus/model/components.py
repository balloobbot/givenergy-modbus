"""Every device family as a ``modbus_connection.model`` Component.

The pydantic models in this package answer "given a register cache, what do
these numbers mean?". They do not read anything — :class:`~givenergy_modbus.model.plant.Plant`
is fed by the client's poll loop, which decides which banks to solicit. This
module is the same device knowledge expressed the other way round, in
``modbus-connection``'s declarative vocabulary: a :class:`Component` per device
family that knows its own readable address map and can read *itself* in as few
Modbus calls as the map allows.

Both views are generated from one source — the ``REGISTER_LUT`` on each
``RegisterGetter``. Re-typing 1260 register addresses into a second file would
be the single most dangerous edit anyone could make to this library (see
AGENTS.md, "Critical Caution"), so the LUT stays canonical and the fields are
derived from it at import. What that costs is compile-time attribute typing;
what it buys is that the two views cannot drift, which
``tests/model/test_components.py`` asserts field by field against real captures.

What the translation needs, beyond the stock field factories:

* **A field is a set of addresses, not a span.** A GivEnergy field names its
  registers individually and in its own order — ``uint32(IR1642, IR1641)`` is a
  little-endian pair, ``firmware_version(HR19, HR21)`` skips HR20 entirely.
  ``RegisterField`` is ``(address, count)`` over a contiguous window, so
  :class:`GivEnergyField` keeps the per-register offsets and re-selects the
  words it was given. 16 of 1260 fields need that; the rest are plain spans.
* **Bounds.** 302 fields declare a plausible range and decode to ``None``
  outside it — a corruption guard (#82) with no equivalent in the field
  vocabulary, so it lives on the field here.
* **Converters.** The LUT's ``pre_conv``/``post_conv`` pair composes into one
  ``words -> value`` callable, which covers every converter the library has:
  scaled numbers, enums, bitfields, byte splits, time slots, datetimes,
  serials, fault-code lists.

Two things do not fit at all and are not modelled here:

* **Meter product registers** (``MR``, function code 0x16) are a fourth register
  space. ``modbus_connection.model`` knows holding and input, so
  :class:`~givenergy_modbus.model.meter.MeterProduct` has no component.
* **Writes.** Every field here is read-only on purpose. Writing a GivEnergy
  register is gated twice (``manifest.write_safe_registers`` and
  ``pdu.write_registers.WRITE_SAFE_REGISTERS``) because the wrong address can
  damage hardware. Marking fields ``writable`` would route writes around both
  gates; ``client.commands`` remains the only way to write.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from enum import Enum
from typing import Any

from modbus_connection.model import (
    Component,
    ComponentGroup,
    Range,
    RawField,
    RegisterField,
    RegisterSpace,
    raw_register,
)

from givenergy_modbus.connection import MAX_REGISTERS_PER_READ
from givenergy_modbus.model.aio_battery import AioBatteryModuleRegisterGetter
from givenergy_modbus.model.battery import BatteryRegisterGetter
from givenergy_modbus.model.ems import EmsRegisterGetter
from givenergy_modbus.model.gateway import GatewayV1RegisterGetter, GatewayV2RegisterGetter
from givenergy_modbus.model.hv_bcu import BcuRegisterGetter, BmuRegisterGetter
from givenergy_modbus.model.inverter import SinglePhaseInverterRegisterGetter
from givenergy_modbus.model.inverter_threephase import ThreePhaseInverterRegisterGetter
from givenergy_modbus.model.lv_bcu import LvBcuRegisterGetter
from givenergy_modbus.model.meter import MeterRegisterGetter
from givenergy_modbus.model.register import HR, IR, Register, RegisterDefinition, RegisterGetter

_logger = logging.getLogger(__name__)


class GivEnergyField(RegisterField[Any]):
    """One LUT entry as a component attribute.

    ``RegisterField`` hands ``decode`` the contiguous window ``[address,
    address + count)``; ``offsets`` picks this field's registers out of it, in
    the order the LUT names them. That covers the reordered pairs and the one
    field with a hole in it, at the cost of reading a register or two nobody
    wants — which is free, since they sit inside a page the device serves as a
    unit anyway.
    """

    def __init__(
        self,
        address: int,
        *,
        count: int,
        offsets: Sequence[int],
        decode_value: Callable[[list[int]], Any],
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> None:
        super().__init__(address, count=count)
        self.offsets = tuple(offsets)
        self.decode_value = decode_value
        self.min_value = min_value
        self.max_value = max_value

    def decode(self, words: list[int], scale_exponent: int | None = None) -> Any:
        """Decode this field's registers out of ``words``."""
        selected = [words[offset] for offset in self.offsets]
        value = self.decode_value(selected)
        if value is None or (self.min_value is None and self.max_value is None):
            return value
        if not any(selected):
            # An all-zero raw bank means the hardware never populated these
            # registers (an absent meter slot, say). Bounds don't apply to
            # "unset", and applying them would log on every poll.
            return value
        if (self.min_value is not None and value < self.min_value) or (
            self.max_value is not None and value > self.max_value
        ):
            # Suppressing an out-of-bounds value is more honest than passing an
            # obviously-wrong one downstream — see #82 for the corruption pattern.
            _logger.debug("register value out of bounds: %r not in [%s, %s]", value, self.min_value, self.max_value)
            return None
        return value


def _decoder(definition: RegisterDefinition) -> Callable[[list[int]], Any]:
    """Compose a LUT entry's pre- and post-converters into one words -> value call.

    Mirrors ``RegisterGetter.get`` exactly, including its leniency: an enum
    post-converter handed a value the firmware invented decodes to ``None``
    rather than aborting the whole read.
    """
    pre_conv, post_conv = definition.pre_conv, definition.post_conv

    def decode(words: list[int]) -> Any:
        if isinstance(pre_conv, tuple):
            value = pre_conv[0](*words, *pre_conv[1:])
        elif pre_conv is not None:
            value = pre_conv(*words)
        else:
            value = list(words)
        if post_conv is None:
            return value
        if isinstance(post_conv, tuple):
            return post_conv[0](value, *post_conv[1:])
        if isinstance(post_conv, type) and issubclass(post_conv, Enum):
            try:
                return post_conv(value)
            except (ValueError, TypeError):
                _logger.debug("%r is not a valid %s", value, post_conv.__name__)
                return None
        return post_conv(value)

    return decode


def _field(definition: RegisterDefinition) -> GivEnergyField:
    """Build the component field backing one LUT entry."""
    addresses = [register.index for register in definition.registers]
    base = min(addresses)
    return GivEnergyField(
        base,
        count=max(addresses) - base + 1,
        offsets=[address - base for address in addresses],
        decode_value=_decoder(definition),
        min_value=definition.min_value,
        max_value=definition.max_value,
    )


def _register_class(space: RegisterSpace) -> type[Register]:
    return HR if space == "holding" else IR


def component_class(
    name: str,
    getter: type[RegisterGetter],
    *,
    space: RegisterSpace,
    ranges: tuple[Range, ...],
    doc: str,
) -> type[Component]:
    """Build the Component for one register space of one device family.

    Only the LUT entries that live in ``space`` become fields — a family whose
    registers span both spaces becomes two components, pooled by
    :func:`components_for` (see :class:`Component`'s single ``register_space``).

    Raises ``ValueError`` if a field's addresses fall outside ``ranges``, which
    would leave it unreadable.
    """
    register_class = _register_class(space)
    namespace: dict[str, Any] = {
        "__doc__": doc,
        "register_space": space,
        "register_ranges": ranges,
        # GivEnergy's Transparent read function caps a request at 60 registers,
        # well under the Modbus ceiling of 125.
        "max_span": MAX_REGISTERS_PER_READ,
    }
    readable = {address for low, high in ranges for address in range(low, high + 1)}
    covered: set[int] = set()
    for field_name, definition in getter.REGISTER_LUT.items():
        if not all(isinstance(register, register_class) for register in definition.registers):
            continue
        field = _field(definition)
        window = set(range(field.address, field.address + field.count))
        if not window <= readable:
            raise ValueError(
                f"{name}.{field_name} reads {sorted(window - readable)}, outside the declared readable ranges {ranges}"
            )
        namespace[field_name] = field
        covered |= window
    namespace.update(_page_anchors(ranges, covered))
    return type(name, (Component,), namespace)


def _page_anchors(ranges: tuple[Range, ...], covered: set[int]) -> dict[str, RawField]:
    """Placeholder fields pinning each block to the whole page the device serves.

    GivEnergy answers a read as a page: the dongle's own traffic only ever asks
    for a bank's full width from its origin. The planner sizes a block to the
    fields in it, so a page whose first modelled register is not its first
    register would be read from the wrong base — ``HR(242, 58)`` where the
    hardware expects ``HR(240, 60)``.

    ``register_ranges`` is the natural place to say "this device answers in
    fixed pages", but it only declares which addresses are *readable*, not where
    a block may start. Until it does, an unused raw field at each end of a range
    pulls the block out to the page boundary. They read as ``None`` and are
    filtered out of a component's modelled fields by their leading underscore.
    """
    anchors: dict[str, RawField] = {}
    for low, high in ranges:
        for address in (low, high):
            if address not in covered:
                anchors[f"_page_anchor_{address}"] = raw_register(address)
    return anchors


# ---------------------------------------------------------------------------
# Readable address maps
#
# Stated as data rather than derived from the field addresses, because they are
# a claim about the *hardware*: these are the banks the device answers, in the
# page-aligned shape it answers them in. Where a family is polled by the client,
# the ranges match the banks in ``manifest`` and ``client._refresh_banks``
# exactly, so a component's read plan reproduces the library's own request
# pattern. Where it is not — HR(180-239), HR(4107+), IR(240-248) — the range
# comes from the LUT's own belief about what lives there.
#
# ``tests/model/test_components.py`` asserts both halves of that: every field is
# covered, and the plan a component builds is the request pattern the client
# already makes.
# ---------------------------------------------------------------------------

_INVERTER_HOLDING: tuple[Range, ...] = (
    (0, 59),  # identity, firmware, serial
    (60, 119),  # charge/discharge configuration
    (120, 179),  # battery and grid limits
    (180, 239),  # command registers; written, never polled
    (240, 299),  # extended charge/discharge slots 3-10
    (300, 359),  # AC-output configuration (AC-coupled and All-in-One only)
    (499, 510),  # HV cabinet topology
    (540, 599),  # Smart Load scheduling slots
    (4107, 4114),  # installer-tier registers
    (4141, 4142),
    (20000, 20051),  # peak shaving / valley filling
)

_INVERTER_INPUT: tuple[Range, ...] = (
    (0, 59),  # live electrical measurements
    (180, 239),  # battery and BMS rollup
    (240, 248),
)

_THREE_PHASE_HOLDING: tuple[Range, ...] = (
    *_INVERTER_HOLDING,
    (1000, 1059),  # three-phase configuration
    (1060, 1119),
    (1120, 1124),
)

_THREE_PHASE_INPUT: tuple[Range, ...] = (
    *_INVERTER_INPUT,
    *((base, min(base + 59, 1413)) for base in range(1000, 1414, 60)),
)

_EMS_HOLDING: tuple[Range, ...] = ((0, 59), (2040, 2075))
_EMS_INPUT: tuple[Range, ...] = ((0, 59), (2040, 2094))

# The BMS page every LV battery, AIO module and HV BMU serves.
_BMS_INPUT: tuple[Range, ...] = ((60, 119),)
_HV_BCU_INPUT: tuple[Range, ...] = ((60, 119), (120, 179))
_METER_INPUT: tuple[Range, ...] = ((60, 89),)
_GATEWAY_INPUT: tuple[Range, ...] = tuple((base, min(base + 59, 1859)) for base in range(1600, 1860, 60))


# ---------------------------------------------------------------------------
# The components
# ---------------------------------------------------------------------------

InverterHolding = component_class(
    "InverterHolding",
    SinglePhaseInverterRegisterGetter,
    space="holding",
    ranges=_INVERTER_HOLDING,
    doc="Single-phase inverter configuration and identity (FC03).",
)

InverterInput = component_class(
    "InverterInput",
    SinglePhaseInverterRegisterGetter,
    space="input",
    ranges=_INVERTER_INPUT,
    doc="Single-phase inverter live measurements (FC04).",
)

ThreePhaseInverterHolding = component_class(
    "ThreePhaseInverterHolding",
    ThreePhaseInverterRegisterGetter,
    space="holding",
    ranges=_THREE_PHASE_HOLDING,
    doc="Three-phase inverter configuration and identity (FC03).",
)

ThreePhaseInverterInput = component_class(
    "ThreePhaseInverterInput",
    ThreePhaseInverterRegisterGetter,
    space="input",
    ranges=_THREE_PHASE_INPUT,
    doc="Three-phase inverter live measurements, including per-phase data (FC04).",
)

EmsHolding = component_class(
    "EmsHolding",
    EmsRegisterGetter,
    space="holding",
    ranges=_EMS_HOLDING,
    doc="EMS plant-controller configuration (FC03).",
)

EmsInput = component_class(
    "EmsInput",
    EmsRegisterGetter,
    space="input",
    ranges=_EMS_INPUT,
    doc="EMS plant-level rollup across the inverters it manages (FC04).",
)

Battery = component_class(
    "Battery",
    BatteryRegisterGetter,
    space="input",
    ranges=_BMS_INPUT,
    doc="An LV battery pack's BMS, at its own device address (FC04).",
)

AioBatteryModule = component_class(
    "AioBatteryModule",
    AioBatteryModuleRegisterGetter,
    space="input",
    ranges=_BMS_INPUT,
    doc="One battery module inside an All-in-One (FC04).",
)

HvBcu = component_class(
    "HvBcu",
    BcuRegisterGetter,
    space="input",
    ranges=_HV_BCU_INPUT,
    doc="An HV stack's battery control unit (FC04).",
)

HvBmu = component_class(
    "HvBmu",
    BmuRegisterGetter,
    space="input",
    ranges=_BMS_INPUT,
    doc="One battery module unit inside an HV stack (FC04).",
)

LvBcu = component_class(
    "LvBcu",
    LvBcuRegisterGetter,
    space="input",
    ranges=_BMS_INPUT,
    doc="The LV battery control unit's summary registers (FC04).",
)

Meter = component_class(
    "Meter",
    MeterRegisterGetter,
    space="input",
    ranges=_METER_INPUT,
    doc="An external energy meter's measurements (FC04).",
)

GatewayV1 = component_class(
    "GatewayV1",
    GatewayV1RegisterGetter,
    space="input",
    ranges=_GATEWAY_INPUT,
    doc="A Gen-1 gateway's rollup across the All-in-Ones behind it (FC04).",
)

GatewayV2 = component_class(
    "GatewayV2",
    GatewayV2RegisterGetter,
    space="input",
    ranges=_GATEWAY_INPUT,
    doc="A Gen-2 gateway's rollup across the All-in-Ones behind it (FC04).",
)


#: Families whose registers span both spaces, and so need two components.
SPLIT_FAMILIES: dict[str, tuple[type[Component], type[Component]]] = {
    "inverter": (InverterHolding, InverterInput),
    "three_phase_inverter": (ThreePhaseInverterHolding, ThreePhaseInverterInput),
    "ems": (EmsHolding, EmsInput),
}

#: Families that live entirely in the input space.
INPUT_ONLY_FAMILIES: dict[str, type[Component]] = {
    "battery": Battery,
    "aio_battery_module": AioBatteryModule,
    "hv_bcu": HvBcu,
    "hv_bmu": HvBmu,
    "lv_bcu": LvBcu,
    "meter": Meter,
    "gateway_v1": GatewayV1,
    "gateway_v2": GatewayV2,
}


def modelled_fields(component: Component) -> dict[str, GivEnergyField]:
    """A component's live device fields, without the page anchors.

    Reads ``_register_fields`` rather than ``declared_fields`` because the
    public mapping is the *class's* declared layout and ``restrict_fields``
    doesn't narrow it — so after capability gating it still lists fields the
    instance no longer has.
    """
    return {name: field for name, field in component._register_fields.items() if isinstance(field, GivEnergyField)}


def components_for(family: str, unit: Any) -> tuple[list[Component], ComponentGroup]:
    """Build ``family``'s components on ``unit`` and the group that reads them.

    Returns the components and a :class:`ComponentGroup` that pools their reads.
    A split family's two components address different register spaces, so the
    group plans each space's blocks separately but drives both from one call.
    """
    if family in SPLIT_FAMILIES:
        components = [klass(unit) for klass in SPLIT_FAMILIES[family]]
    elif family in INPUT_ONLY_FAMILIES:
        components = [INPUT_ONLY_FAMILIES[family](unit)]
    else:
        raise KeyError(f"unknown device family {family!r}")
    return components, ComponentGroup(unit, components)


def restrict_to_banks(component: Component, banks: Iterable[Range]) -> None:
    """Narrow ``component`` to the fields inside ``banks``, and reshape its plan.

    Which banks a GivEnergy device serves depends on its model and firmware —
    an AC-coupled inverter answers HR(300-359) and a hybrid times out on it — so
    the readable map is decided at detect time, not at class-definition time.
    ``register_ranges`` is a class attribute, so capability gating happens here
    instead: keep the fields the detected device actually serves and let
    ``restrict_fields`` recompute the ranges around them.
    """
    served = {address for low, high in banks for address in range(low, high + 1)}
    keep = [
        name
        for name, field in component.declared_fields.items()
        if set(range(field.address, field.address + field.count)) <= served
    ]
    component.restrict_fields(keep)


__all__ = [
    "INPUT_ONLY_FAMILIES",
    "MAX_REGISTERS_PER_READ",
    "SPLIT_FAMILIES",
    "AioBatteryModule",
    "Battery",
    "EmsHolding",
    "EmsInput",
    "GatewayV1",
    "GatewayV2",
    "GivEnergyField",
    "HvBcu",
    "HvBmu",
    "InverterHolding",
    "InverterInput",
    "LvBcu",
    "Meter",
    "ThreePhaseInverterHolding",
    "ThreePhaseInverterInput",
    "component_class",
    "components_for",
    "modelled_fields",
    "restrict_to_banks",
]
