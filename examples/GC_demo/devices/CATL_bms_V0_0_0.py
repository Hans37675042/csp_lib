from csp_lib.modbus import ByteOrder, FunctionCode, UInt16, UInt32
from csp_lib.equipment.core import ReadPoint, WritePoint, pipeline, ScaleTransform, RoundTransform
from csp_lib.equipment.core.point import PointMetadata, RangeValidator, EnumValidator

# CATL BMS - point definitions (no alarm definitions)
# Source: Referance.py CATL_BMS class (lines 827-1353)
#
# Dynamic cell/temperature/balance points are generated via factory functions:
#   catl_bms_cell_points(module_num, cell_num)
#   catl_bms_temp_points(module_num, cell_num)
#   catl_bms_balance_points(module_num, cell_num)
#
# Key points for GC control:
#   soc, v, i, p               → strategy context
#   BMS_power_on, BMS_status   → device readiness check
#   BMS_heartbeat (write)      → SystemController HeartbeatService (INCREMENT 0-15)
#   BMS_on_off (write)         → MongoDB eqpt_control
#   BMS_fault_clear (write)    → MongoDB reset signal

# ─── Static Read Points ───────────────────────────────────────────────────────

CATL_bms_read_points = [
    # ── System-level status (address 0x0020–0x0044) ──
    ReadPoint(
        name="v",
        address=0x0020,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="V", description="系統電壓"),
    ),
    ReadPoint(
        name="i",
        address=0x0021,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        # raw: 0–40000, offset: -20000 → -20000 to +20000 A
        pipeline=pipeline(ScaleTransform(1, offset=-20000), RoundTransform(0)),
        metadata=PointMetadata(unit="A", description="系統電流"),
    ),
    ReadPoint(
        name="soc",
        address=0x0022,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="%", description="荷電狀態"),
    ),
    ReadPoint(
        name="soh",
        address=0x0023,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="%", description="健康狀態"),
    ),
    ReadPoint(
        name="v_max_cell",
        address=0x0024,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.001), RoundTransform(3)),
        metadata=PointMetadata(unit="V", description="最高單體電壓"),
    ),
    ReadPoint(
        name="v_min_cell",
        address=0x0025,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.001), RoundTransform(3)),
        metadata=PointMetadata(unit="V", description="最低單體電壓"),
    ),
    ReadPoint(
        name="v_avg_cell",
        address=0x0026,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.001), RoundTransform(3)),
        metadata=PointMetadata(unit="V", description="平均單體電壓"),
    ),
    ReadPoint(
        name="temp_max_cell",
        address=0x0027,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)),
        metadata=PointMetadata(unit="℃", description="最高單體溫度"),
    ),
    ReadPoint(
        name="temp_min_cell",
        address=0x0028,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)),
        metadata=PointMetadata(unit="℃", description="最低單體溫度"),
    ),
    ReadPoint(
        name="temp_avg_cell",
        address=0x0029,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)),
        metadata=PointMetadata(unit="℃", description="平均單體溫度"),
    ),
    ReadPoint(
        name="i_max_charge",
        address=0x002a,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-20000), RoundTransform(0)),
        metadata=PointMetadata(unit="A", description="最大充電電流"),
    ),
    ReadPoint(
        name="i_max_discharge",
        address=0x002b,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-20000), RoundTransform(0)),
        metadata=PointMetadata(unit="A", description="最大放電電流"),
    ),
    ReadPoint(
        name="p_max_charge",
        address=0x002c,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-20000), RoundTransform(0)),
        metadata=PointMetadata(unit="kW", description="最大充電功率"),
    ),
    ReadPoint(
        name="p_max_discharge",
        address=0x002d,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-20000), RoundTransform(0)),
        metadata=PointMetadata(unit="kW", description="最大放電功率"),
    ),
    ReadPoint(
        name="p",
        address=0x002e,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-20000), RoundTransform(0)),
        metadata=PointMetadata(unit="kW", description="系統功率"),
    ),
    ReadPoint(
        name="soe_charge",
        address=0x002f,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="kWh", description="可充電電量"),
    ),
    ReadPoint(
        name="soe_discharge",
        address=0x0030,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="kWh", description="可放電電量"),
    ),
    ReadPoint(
        name="sys_charge_remain_energy",
        address=0x0031,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(unit="kWh", description="系統剩餘充電量"),
    ),
    ReadPoint(
        name="sys_discharge_remain_energy",
        address=0x0032,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(unit="kWh", description="系統剩餘放電量"),
    ),
    ReadPoint(
        name="v_max_charge",
        address=0x0033,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="V", description="最大充電電壓"),
    ),
    ReadPoint(
        name="v_min_discharge",
        address=0x0034,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)),
        metadata=PointMetadata(unit="V", description="最小放電電壓"),
    ),
    ReadPoint(
        name="ins_det_fun_status",
        address=0x0035,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="絕緣檢測功能狀態"),
    ),
    ReadPoint(
        name="pos_ins_gnd",
        address=0x0036,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(unit="kΩ", description="正極對地絕緣電阻"),
    ),
    ReadPoint(
        name="neg_ins_gnd",
        address=0x0037,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(unit="kΩ", description="負極對地絕緣電阻"),
    ),
    ReadPoint(
        name="sys_avg_temp_environment_MBMU1",
        address=0x0038,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)),
        metadata=PointMetadata(unit="℃", description="MBMU1 環境溫度"),
    ),
    ReadPoint(
        name="sys_avg_temp_environment_MBMU2",
        address=0x0039,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)),
        metadata=PointMetadata(unit="℃", description="MBMU2 環境溫度"),
    ),
    ReadPoint(
        name="sys_ambient_humi_MBMU1",
        address=0x003e,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(unit="%", description="MBMU1 環境濕度"),
    ),
    ReadPoint(
        name="sys_ambient_humi_MBMU2",
        address=0x003f,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(unit="%", description="MBMU2 環境濕度"),
    ),
    ReadPoint(
        name="sys_soc_maintenance_req_status",
        address=0x0044,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="SOC 維護需求狀態"),
    ),
    # ── TMS thermal management (0x0060–0x0083) ──
    ReadPoint(name="BMS_TMS1_mode", address=0x0060, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS1 模式")),
    ReadPoint(name="TMS1_temp_set_by_bms", address=0x0061, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS1 BMS設定溫度")),
    ReadPoint(name="TMS1_real_mode", address=0x0062, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS1 實際模式")),
    ReadPoint(name="Rack_inlet_temp_TMS1_Outlet", address=0x0063, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS1 機架入口/出口溫度")),
    ReadPoint(name="Rack_outlet_temp_TMS1_inlet", address=0x0064, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS1 機架出口/入口溫度")),
    ReadPoint(name="TMS1_environment_temp", address=0x0065, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS1 環境溫度")),
    ReadPoint(name="TMS1_fault_code", address=0x0066, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS1 故障碼")),
    ReadPoint(name="TMS1_fault_level", address=0x0067, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS1 故障等級")),
    ReadPoint(name="TMS1_cooling_mode_protection", address=0x0068, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS1 冷卻模式保護")),
    ReadPoint(name="BMS_TMS2_mode", address=0x0069, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS2 模式")),
    ReadPoint(name="TMS2_temp_set_by_bms", address=0x006a, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS2 BMS設定溫度")),
    ReadPoint(name="TMS2_real_mode", address=0x006b, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS2 實際模式")),
    ReadPoint(name="Rack_inlet_temp_TMS2_Outlet", address=0x006c, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS2 機架入口/出口溫度")),
    ReadPoint(name="Rack_outlet_temp_TMS2_inlet", address=0x006d, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS2 機架出口/入口溫度")),
    ReadPoint(name="TMS2_environment_temp", address=0x006e, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS2 環境溫度")),
    ReadPoint(name="TMS2_fault_code", address=0x006f, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS2 故障碼")),
    ReadPoint(name="TMS2_fault_level", address=0x0070, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS2 故障等級")),
    ReadPoint(name="TMS2_cooling_mode_protection", address=0x0071, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS2 冷卻模式保護")),
    ReadPoint(name="BMS_TMS3_mode", address=0x0072, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS3 模式")),
    ReadPoint(name="TMS3_temp_set_by_bms", address=0x0073, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS3 BMS設定溫度")),
    ReadPoint(name="TMS3_real_mode", address=0x0074, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS3 實際模式")),
    ReadPoint(name="Rack_inlet_temp_TMS3_Outlet", address=0x0075, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS3 機架入口/出口溫度")),
    ReadPoint(name="Rack_outlet_temp_TMS3_inlet", address=0x0076, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS3 機架出口/入口溫度")),
    ReadPoint(name="TMS3_environment_temp", address=0x0077, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS3 環境溫度")),
    ReadPoint(name="TMS3_fault_code", address=0x0078, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS3 故障碼")),
    ReadPoint(name="TMS3_fault_level", address=0x0079, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS3 故障等級")),
    ReadPoint(name="TMS3_cooling_mode_protection", address=0x007a, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS3 冷卻模式保護")),
    ReadPoint(name="BMS_TMS4_mode", address=0x007b, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS4 模式")),
    ReadPoint(name="TMS4_temp_set_by_bms", address=0x007c, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS4 BMS設定溫度")),
    ReadPoint(name="TMS4_real_mode", address=0x007d, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS4 實際模式")),
    ReadPoint(name="Rack_inlet_temp_TMS4_Outlet", address=0x007e, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS4 機架入口/出口溫度")),
    ReadPoint(name="Rack_outlet_temp_TMS4_inlet", address=0x007f, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS4 機架出口/入口溫度")),
    ReadPoint(name="TMS4_environment_temp", address=0x0080, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="TMS4 環境溫度")),
    ReadPoint(name="TMS4_fault_code", address=0x0081, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS4 故障碼")),
    ReadPoint(name="TMS4_fault_level", address=0x0082, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS4 故障等級")),
    ReadPoint(name="TMS4_cooling_mode_protection", address=0x0083, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="TMS4 冷卻模式保護")),
    # ── BMS command interface status (0x0300–0x0306) ──
    ReadPoint(
        name="BMS_heartbeat",
        address=0x0300,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="BMS 心跳計數 (0-15)"),
    ),
    ReadPoint(
        name="BMS_power_on",
        address=0x0301,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="BMS 上電狀態 (1=ready)"),
    ),
    ReadPoint(
        name="BMS_status",
        address=0x0302,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="BMS 系統狀態"),
    ),
    ReadPoint(
        name="number_connected_hv_bms",
        address=0x0304,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="已連接高壓 BMS 數量"),
    ),
    ReadPoint(
        name="sys_step_charge_mode",
        address=0x0305,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="步進充電模式"),
    ),
    ReadPoint(
        name="number_of_racks",
        address=0x0306,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="機架數量"),
    ),
    # ── EMS command interface (0x0380–0x038f) — read-back monitoring ──
    ReadPoint(
        name="ems_heartbeat",
        address=0x0380,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="EMS 心跳回讀 (0-15)"),
    ),
    ReadPoint(
        name="ems_cmd",
        address=0x0381,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="EMS 命令"),
    ),
    ReadPoint(
        name="fault_clear_cmd",
        address=0x038c,
        data_type=UInt16(),
        function_code=FunctionCode.READ_HOLDING_REGISTERS,
        byte_order=ByteOrder.BIG_ENDIAN,
        pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
        metadata=PointMetadata(description="故障清除命令"),
    ),
    # ── Rack status (0x0410–0x0456) ──
    ReadPoint(name="preChg_relay_rack", address=0x0410, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="預充繼電器狀態")),
    ReadPoint(name="relay_pos_rack", address=0x0411, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="正極繼電器狀態")),
    ReadPoint(name="relay_neg_rack", address=0x0412, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="負極繼電器狀態")),
    ReadPoint(name="HV_online_rack", address=0x0413, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="高壓在線狀態")),
    ReadPoint(name="bat_rack_maintenance", address=0x0414, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1), RoundTransform(0)), metadata=PointMetadata(description="電池維護狀態")),
    ReadPoint(name="v_rack", address=0x0420, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)), metadata=PointMetadata(unit="V", description="機架電壓")),
    ReadPoint(name="i_rack", address=0x0422, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1, offset=-2000), RoundTransform(1)), metadata=PointMetadata(unit="A", description="機架電流")),
    ReadPoint(name="soc_rack", address=0x0423, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)), metadata=PointMetadata(unit="%", description="機架 SOC")),
    ReadPoint(name="soh_rack", address=0x0424, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)), metadata=PointMetadata(unit="%", description="機架 SOH")),
    ReadPoint(name="cv_max_rack", address=0x0425, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.001), RoundTransform(3)), metadata=PointMetadata(unit="V", description="機架最高單體電壓")),
    ReadPoint(name="cv_min_rack", address=0x0426, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.001), RoundTransform(3)), metadata=PointMetadata(unit="V", description="機架最低單體電壓")),
    ReadPoint(name="cv_avg_rack", address=0x0427, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.001), RoundTransform(3)), metadata=PointMetadata(unit="V", description="機架平均單體電壓")),
    ReadPoint(name="ct_max_rack", address=0x0428, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="機架最高單體溫度")),
    ReadPoint(name="ct_min_rack", address=0x0429, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="機架最低單體溫度")),
    ReadPoint(name="ct_avg_rack", address=0x042a, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(1, offset=-50), RoundTransform(0)), metadata=PointMetadata(unit="℃", description="機架平均單體溫度")),
    ReadPoint(name="p_rack", address=0x042f, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1, offset=-2000), RoundTransform(1)), metadata=PointMetadata(unit="kW", description="機架功率")),
    ReadPoint(name="chg_SOE_rack", address=0x0434, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)), metadata=PointMetadata(unit="kWh", description="機架可充電量")),
    ReadPoint(name="dischg_SOE_rack", address=0x0435, data_type=UInt16(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)), metadata=PointMetadata(unit="kWh", description="機架可放電量")),
    ReadPoint(name="kwh_chg_rack", address=0x0454, data_type=UInt32(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)), metadata=PointMetadata(unit="kWh", description="機架累積充電量")),
    ReadPoint(name="kwh_dischg_rack", address=0x0456, data_type=UInt32(), function_code=FunctionCode.READ_HOLDING_REGISTERS, byte_order=ByteOrder.BIG_ENDIAN, pipeline=pipeline(ScaleTransform(0.1), RoundTransform(1)), metadata=PointMetadata(unit="kWh", description="機架累積放電量")),
]

# ─── Write Points ─────────────────────────────────────────────────────────────

CATL_bms_write_points = [
    # BMS_heartbeat: EMS→BMS watchdog (0-15 cycle), managed by HeartbeatService
    WritePoint(
        name="BMS_heartbeat",
        address=0x0380,
        data_type=UInt16(),
        function_code=FunctionCode.WRITE_SINGLE_REGISTER,
        validator=RangeValidator(min_value=0, max_value=15),
    ),
    # BMS_on_off: 2=ON (充放電使能), 3=OFF
    WritePoint(
        name="BMS_on_off",
        address=0x0381,
        data_type=UInt16(),
        function_code=FunctionCode.WRITE_SINGLE_REGISTER,
        validator=EnumValidator((0, 1, 2, 3)),
    ),
    # BMS_fault_clear: 1=clear fault, 0=reset
    WritePoint(
        name="BMS_fault_clear",
        address=0x038c,
        data_type=UInt16(),
        function_code=FunctionCode.WRITE_SINGLE_REGISTER,
        validator=EnumValidator((0, 1)),
    ),
]


# ─── Dynamic Points Factory Functions ─────────────────────────────────────────

def catl_bms_cell_points(module_num: int, cell_num: int) -> list[ReadPoint]:
    """Generate per-cell voltage ReadPoints (v_cell_{module}_{cell})."""
    return [
        ReadPoint(
            name=f"v_cell_{module}_{cell}",
            address=0x0480 + module * cell_num + cell,
            data_type=UInt16(),
            function_code=FunctionCode.READ_HOLDING_REGISTERS,
            byte_order=ByteOrder.BIG_ENDIAN,
            pipeline=pipeline(ScaleTransform(0.001), RoundTransform(3)),
            metadata=PointMetadata(unit="V", description=f"模組{module} 電芯{cell} 電壓"),
        )
        for module in range(module_num)
        for cell in range(cell_num)
    ]


def catl_bms_temp_points(module_num: int, cell_num: int) -> list[ReadPoint]:
    """Generate per-cell temperature ReadPoints (raw register, 2 temps packed per register)."""
    temp_cell_calculate = (cell_num + 1) // 2  # ceiling division
    return [
        ReadPoint(
            name=f"temp_cell_{module}_{cell}",
            address=0x06C0 + module * temp_cell_calculate + cell,
            data_type=UInt16(),
            function_code=FunctionCode.READ_HOLDING_REGISTERS,
            byte_order=ByteOrder.BIG_ENDIAN,
            pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
            # Note: each register packs 2 temperatures (high byte / low byte, offset -50).
            # Unpack at application layer: low=(raw & 0xFF)-50, high=(raw>>8 & 0xFF)-50
            metadata=PointMetadata(description=f"模組{module} 溫度暫存器{cell} (雙溫打包)"),
        )
        for module in range(module_num)
        for cell in range(temp_cell_calculate)
    ]


def catl_bms_balance_points(module_num: int, cell_num: int) -> list[ReadPoint]:
    """Generate per-cell balance status ReadPoints."""
    balance_cell_calculate = (cell_num + 15) // 16  # ceiling division by 16
    return [
        ReadPoint(
            name=f"balance_cell_{module}_{cell}",
            address=0x0790 + module * balance_cell_calculate + cell,
            data_type=UInt16(),
            function_code=FunctionCode.READ_HOLDING_REGISTERS,
            byte_order=ByteOrder.BIG_ENDIAN,
            pipeline=pipeline(ScaleTransform(1), RoundTransform(0)),
            metadata=PointMetadata(description=f"模組{module} 均衡狀態暫存器{cell}"),
        )
        for module in range(module_num)
        for cell in range(balance_cell_calculate)
    ]
