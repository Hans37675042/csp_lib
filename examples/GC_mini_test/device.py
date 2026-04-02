from csp_lib.equipment.core.point import PointMetadata, RangeValidator
from csp_lib.equipment.alarm import (
    AlarmDefinition,
    BitMaskAlarmEvaluator,
    ThresholdAlarmEvaluator,
    ThresholdCondition,
    Operator,
    AlarmLevel
)
from csp_lib.equipment.core.transform import BitExtractTransform
from csp_lib.equipment.core.pipeline import ProcessingPipeline
from csp_lib.equipment.core import pipeline, ReadPoint, WritePoint, RoundTransform, ScaleTransform
from csp_lib.equipment.device import AsyncModbusDevice

from csp_lib.modbus import Float32, UInt16, UInt32, FunctionCode

from csp_lib.integration import CommandStep, StepCheck, SystemCommand


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
    address=5002,
    data_type=UInt16(),
    pipeline=pipeline(ScaleTransform(0.1)),
    metadata=PointMetadata(unit="%", description="Battery state of charge"),
    function_code=FunctionCode.READ_INPUT_REGISTERS,
)

bms_on = ReadPoint(
    name="bms_on",
    address=5004,
    data_type=UInt16(),
    metadata=PointMetadata(description="BMS 開機狀態"),
    function_code=FunctionCode.READ_INPUT_REGISTERS,
    pipeline=ProcessingPipeline(steps=[BitExtractTransform(bit_offset=0)]),
    # 輸出: bool
)

pcs_on = ReadPoint(
    name="pcs_on",
    address=5004,
    data_type=UInt16(),
    metadata=PointMetadata(description="PCS 開機狀態"),
    function_code=FunctionCode.READ_INPUT_REGISTERS,
    pipeline=ProcessingPipeline(steps=[BitExtractTransform(bit_offset=1)]),
    # 輸出: bool
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

BMS_switch = WritePoint(
    name="BMS_on_off",
    address=6004,
    data_type=UInt16(),
    validator=RangeValidator(min_value=0, max_value=1),
    metadata=PointMetadata(description="BMS on/off switch (0=off, 1=on)"),
)

PCS_switch = WritePoint(
    name="PCS_on_off",
    address=6006,
    data_type=UInt16(),
    validator=RangeValidator(min_value=0, max_value=1),
    metadata=PointMetadata(description="PCS on/off switch (0=off, 1=on)"),
)

HeartBeat = WritePoint(
    name="heartbeat",
    address=6008,
    data_type=Float32(),
    validator=RangeValidator(min_value=0, max_value=255),
    metadata=PointMetadata(description="Heartbeat signal"),
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

pcs_always_points = [active_power, soc, bms_on, pcs_on, fault_code]
pcs_write_points = [p_set, q_set, BMS_switch, PCS_switch, HeartBeat]
pcs_alarm_evaluators = [fault_evaluator, soc_evaluator]

class PCSDevice(AsyncModbusDevice):
    ACTIONS: dict[str, str] = {
        "dummy_action": "_action_dummy",  # 範例用的虛擬指令
        "pcs_power_on": "_action_pcs_power_on",
        "bms_power_on": "_action_bms_power_on",
        "power_off": "_action_power_off",
    }

    @property
    def is_bms_on(self) -> bool:
        return bool(self.latest_values.get("bms_on", 0))
    
    @property
    def is_bms_off(self) -> bool:
        return not self.is_bms_on

    @property
    def is_pcs_on(self) -> bool:
        return bool(self.latest_values.get("pcs_on", 0))
    
    @property
    def is_pcs_off(self) -> bool:
        return not self.is_pcs_on

    async def _action_dummy(self):
        None

    async def _action_pcs_power_on(self):
        await self.write("PCS_on_off", 1, verify=True)
        
    async def _action_bms_power_on(self):
        await self.write("BMS_on_off", 1, verify=True)

    async def _action_power_off(self):
        await self.write("PCS_on_off", 0, verify=True)
        await self.write("BMS_on_off", 0, verify=True)

startup_cmd = SystemCommand(
    name="startup_sequence",
    description="完整啟動: 待機 -> 驗證 -> 充電",
    steps=[
        CommandStep(
            action="dummy_action",
            trait="pcs",
            description="檢查 PCS 響應",
            check_after=StepCheck(
                trait="pcs",
                check="is_responsive",
                timeout=5.0,
                poll_interval=0.5,
            )
        ),
        CommandStep(
            action="power_off",
            trait="pcs",
            description="關閉PCS",
            delay_before=0.5,
            check_after=StepCheck(
                trait="pcs",
                check="is_pcs_off",
                timeout=3.0,
                poll_interval=0.5,
            )
        ),
        CommandStep(
            action="dummy_action",
            trait="pcs",
            description="關閉BMS",
            delay_before=0.5,
            check_after=StepCheck(
                trait="pcs",
                check="is_bms_off",
                timeout=3.0,
                poll_interval=0.5,
            )
        ),
        CommandStep(
            action="bms_power_on",
            trait="pcs",
            description="開啟BMS",
            delay_before=0.5,
            check_after=StepCheck(
                trait="pcs",
                check="is_bms_on",
                timeout=10.0,
                poll_interval=0.5,
            )
        ),
        CommandStep(
            action="pcs_power_on",
            trait="pcs",
            description="開啟PCS",
            delay_before=0.5,
            check_after=StepCheck(
                trait="pcs",
                check="is_pcs_on",
                timeout=10.0,
                poll_interval=0.5,
            )
        )
    ],
)