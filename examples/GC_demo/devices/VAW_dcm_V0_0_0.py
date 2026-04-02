from csp_lib.modbus import ByteOrder, FunctionCode, Int16, Int32, UInt32
from csp_lib.equipment.core import ReadPoint, pipeline, ScaleTransform, RoundTransform
from csp_lib.equipment.core.point import PointMetadata

# VAW DC Energy Meter - point definitions (no alarm definitions)
# Source: Referance.py VAW class (lines 792-807)
#
# Note: The original exp_kwh/imp_kwh use uint48/int48 (3 registers each).
# UInt32 is used here (reads 2 registers), covering up to ~429M × 0.1 kWh.
# For small-scale BESS sites this is sufficient.
# Original post_read divided raw value by 10; incorporated as ScaleTransform(0.1).

VAW_dcm_read_points = [
    ReadPoint(
        name="exp_kwh",
        address=0x0000,
        data_type=UInt32(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="kWh", description="輸出累積電量"),
    ),
    ReadPoint(
        name="imp_kwh",
        address=0x0003,
        data_type=Int32(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="kWh", description="輸入累積電量"),
    ),
    ReadPoint(
        name="p",
        address=0x001E,
        data_type=Int32(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.01), RoundTransform(2)),
        metadata=PointMetadata(unit="kW", description="直流功率"),
    ),
    ReadPoint(
        name="v",
        address=0x002A,
        data_type=Int16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(unit="V", description="直流電壓"),
    ),
    ReadPoint(
        name="i",
        address=0x002B,
        data_type=Int16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.01), RoundTransform(2)),
        metadata=PointMetadata(unit="A", description="直流電流"),
    ),
]
