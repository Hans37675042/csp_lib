from csp_lib.equipment.core import pipeline
from csp_lib.equipment.core.point import PointMetadata, RangeValidator
from csp_lib.equipment.alarm import (
    AlarmDefinition,
    BitMaskAlarmEvaluator,
    ThresholdAlarmEvaluator,
    ThresholdCondition,
    Operator,
    AlarmLevel
)

from csp_lib.equipment.core import ReadPoint, WritePoint, RoundTransform, ScaleTransform
from csp_lib.modbus import Float32, ModbusTcpConfig, PymodbusTcpClient, UInt16, FunctionCode



# ============================================================
# Step 1: Define Read Points (設備讀取點位)
# ============================================================

# Active power: register 5000, Float32, scale ×0.1 then round to 1 decimal
active_power = ReadPoint(
    name="active_power",
    address=5000,
    data_type=Float32(),
    pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
    metadata=PointMetadata(unit="kW", description="Active power output"),
    function_code=FunctionCode.READ_INPUT_REGISTERS,
)

# Battery SOC: register 5034, UInt16, scale ×0.1 to get percentage
soc = ReadPoint(
    name="soc",
    address=5034,
    data_type=UInt16(),
    pipeline=pipeline(ScaleTransform(0.1)),
    metadata=PointMetadata(unit="%", description="Battery state of charge"),
    function_code=FunctionCode.READ_INPUT_REGISTERS,
)

# Fault code: register 5100, UInt16, raw bitmask
fault_code = ReadPoint(
    name="fault_code",
    address=5100,
    data_type=UInt16(),
    metadata=PointMetadata(
        description="Fault code bitmask",
        value_map={0: "Normal", 1: "Over-temperature", 2: "Over-current", 4: "DC fault"},
    ),
    function_code=FunctionCode.READ_INPUT_REGISTERS,
)

# ============================================================
# Step 2: Define Write Points (設備寫入點位)
# ============================================================

# Active power setpoint: register 6000, Float32, range -100 ~ 100 kW
p_set = WritePoint(
    name="p_set",
    address=6000,
    data_type=Float32(),
    validator=RangeValidator(min_value=-100.0, max_value=100.0),
    metadata=PointMetadata(unit="kW", description="Active power setpoint"),
)

# Reactive power setpoint: register 6002, Float32, range -50 ~ 50 kVar
q_set = WritePoint(
    name="q_set",
    address=6002,
    data_type=Float32(),
    validator=RangeValidator(min_value=-50.0, max_value=50.0),
    metadata=PointMetadata(unit="kVar", description="Reactive power setpoint"),
)

switch = WritePoint(
    name="BMS_on_off",
    address=6004,
    data_type=UInt16(),
    validator=RangeValidator(min_value=0, max_value=1),
    metadata=PointMetadata(description="BMS on/off switch (0=off, 1=on)"),
)

# ============================================================
# Step 3: Define Alarms (告警定義)
# ============================================================

# Bitmask alarm: check fault_code register bits
fault_evaluator = BitMaskAlarmEvaluator(
    point_name="fault_code",
    bit_alarms={
        0: AlarmDefinition(
            code="OVER_TEMP", name="Over-temperature", level=AlarmLevel.WARNING, description="Over-temperature"
        ),
        1: AlarmDefinition(code="OVER_CURR", name="Over-current", level=AlarmLevel.ALARM, description="Over-current"),
        2: AlarmDefinition(code="DC_FAULT", name="DC bus fault", level=AlarmLevel.ALARM, description="DC bus fault"),
    },
)

# Threshold alarm: SOC too low
soc_evaluator = ThresholdAlarmEvaluator(
    point_name="soc",
    conditions=[
        ThresholdCondition(
            alarm=AlarmDefinition(
                code="SOC_LOW", name="SOC Low", level=AlarmLevel.WARNING, description="SOC below 10%"
            ),
            operator=Operator.LT,
            value=10.0,
        ),
        ThresholdCondition(
            alarm=AlarmDefinition(
                code="SOC_HIGH", name="SOC High", level=AlarmLevel.WARNING, description="SOC above 90%"
            ),
            operator=Operator.GT,
            value=90.0,
        ),
    ],
)

pcs_always_points = [active_power, soc, fault_code]
pcs_write_points = [p_set, q_set, switch]
pcs_alarm_evaluators = [fault_evaluator, soc_evaluator]