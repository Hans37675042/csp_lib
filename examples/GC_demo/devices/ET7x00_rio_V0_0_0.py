from csp_lib.modbus import ByteOrder, FunctionCode, UInt16
from csp_lib.equipment.core import ReadPoint, pipeline, ScaleTransform, RoundTransform
from csp_lib.equipment.core.point import PointMetadata

# ET7x00 Remote I/O - point definitions (no alarm definitions)
# Source: Referance.py ET7x00_RIO class (lines 1584-1611)
#
# Original post_read mapped (switch_on=1, switch_off=0) → switch_state=1 (open)
#                            (switch_on=0, switch_off=1) → switch_state=0 (close)
#                            otherwise                   → switch_state=2 (unknown)
# The new API stores both raw points; GC layer derives switch_state if needed.

ET7x00_rio_read_points = [
    ReadPoint(
        name="switch_on",
        address=0x0000,
        data_type=UInt16(),
        function_code=FunctionCode.READ_DISCRETE_INPUTS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="開關 ON 狀態 (1=open)"),
    ),
    ReadPoint(
        name="switch_off",
        address=0x0001,
        data_type=UInt16(),
        function_code=FunctionCode.READ_DISCRETE_INPUTS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="開關 OFF 狀態 (1=close)"),
    ),
]
