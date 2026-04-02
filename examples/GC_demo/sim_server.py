"""
GC Modbus Simulation Server

模擬 demo_GC 所有設備，供 GC 在無實體設備時進行整合測試。
點位地址、型別、byte_order 完全對應各設備檔案定義。
initial_value 為 raw register 值（ScaleTransform 前的整數）。

伺服器位址：0.0.0.0:5020

─── GC 測試環境變數（搭配 gc_main.py）────────────────────────────
  PCS_HOST=127.0.0.1   BMS_HOST=127.0.0.1   TOTM_HOST=127.0.0.1
  ACM_HOST=127.0.0.1   RIO_HOST=127.0.0.1
  GC_SIM_PORT=5020     # 若 gc_main.py 支援此變數

  Slave ID 映射（模擬環境使用唯一 unit_id）：
    pcs_01: 192.168.1.60 slave 1 → 127.0.0.1:5020 slave 1
    bms_01: 192.168.1.70 slave 1 → 127.0.0.1:5020 slave 2
    totm_01: 192.168.1.50 slave 1 → 127.0.0.1:5020 slave 3
    acm_01: 192.168.1.81 slave 1 → 127.0.0.1:5020 slave 4
    acm_02: 192.168.1.81 slave 2 → 127.0.0.1:5020 slave 5
    acm_03: 192.168.1.81 slave 3 → 127.0.0.1:5020 slave 6
    dcm_01: 192.168.1.81 slave 4 → 127.0.0.1:5020 slave 7
    rio_01: 192.168.1.80 slave 1 → 127.0.0.1:5020 slave 8
──────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
from typing import Any

from csp_lib.modbus import ByteOrder, Int16, Int32, UInt16, UInt32
from csp_lib.modbus_server import (
    BaseDeviceSimulator,
    ServerConfig,
    SimulatedDeviceConfig,
    SimulatedPoint,
    SimulationServer,
)

_BE = ByteOrder.BIG_ENDIAN
_LE = ByteOrder.LITTLE_ENDIAN


def _pt(
    name: str,
    address: int,
    dtype: Any,
    initial_raw: int = 0,
    writable: bool = False,
    byte_order: ByteOrder = _BE,
) -> SimulatedPoint:
    """建立 SimulatedPoint 的快捷函式。initial_raw 為 raw register 值。"""
    return SimulatedPoint(
        name=name,
        address=address,
        data_type=dtype,
        initial_value=initial_raw,
        writable=writable,
        byte_order=byte_order,
    )


# ─── SNPOWER PCS (unit_id=1) ──────────────────────────────────────────────────
# ReadPoint 對應 devices/SNPOWER_pcs_V0_0_0.py
# raw 初始值：380V line→3800, 60Hz(Int32,×0.001)→60000, pf→1000, mode1 bit1 set→2

def build_pcs_config() -> SimulatedDeviceConfig:
    u16, i16, i32, u32 = UInt16(), Int16(), Int32(), UInt32()
    points = [
        # ── 電氣量 (addr 0–21) ──
        _pt("vl_ab",    0,  i16,  3800),
        _pt("vl_bc",    1,  i16,  3800),
        _pt("vl_ca",    2,  i16,  3800),
        _pt("f",        3,  i32,  60000),   # 60Hz × 1000 (scale=0.001)
        _pt("i_a",      5,  i16,  0),
        _pt("i_b",      6,  i16,  0),
        _pt("i_c",      7,  u16,  0),
        _pt("p",        8,  i32,  0),       # kW × 10 (scale=0.1)
        _pt("q",        10, i32,  0),
        _pt("s",        12, i32,  0),
        _pt("pf",       14, i16,  1000),    # 1.0 × 1000 (scale=0.001)
        _pt("v_battery",    15, i16,  1000),  # 1000 Vdc
        _pt("i_battery",    16, i16,  0),
        _pt("p_battery",    17, i32,  0),
        _pt("temp_IGBT",    19, i16,  250),   # 25°C × 10 (scale=0.1)
        _pt("temp_area",    20, i16,  250),
        _pt("available_kva", 21, u32, 24000), # 2400kVA × 10 (scale=0.1)
        # ── 告警暫存器 ──
        _pt("alarm1",   750, u16, 0),
        _pt("alarm2",   751, u16, 0),
        _pt("alarm3",   753, u16, 0),
        _pt("alarm4",   755, u16, 0),
        _pt("alarm5",   757, u16, 0),
        _pt("alarm6",   759, u16, 0),
        _pt("alarm7",   760, u16, 0),
        _pt("alarm8",   761, u16, 0),
        _pt("alarm9",   767, u16, 0),
        _pt("alarm10",  769, u16, 0),
        _pt("alarm11",  771, u16, 0),
        _pt("alarm12",  772, u16, 0),
        _pt("alarm13",  950, u16, 0),
        _pt("alarm14",  951, u16, 0),
        _pt("alarm15",  952, u16, 0),
        # ── 運轉狀態 ── mode1 bit1=1 → pcs_status=1（GC 策略保護邏輯需要）
        _pt("mode1", 850, u16, 2),
        _pt("mode2", 852, u16, 0),
        _pt("mode3", 855, u16, 0),
        _pt("mode4", 1050, u16, 0),
        _pt("mode5", 1051, u16, 0),
        # ── 累積電量 ──
        _pt("tot_charge_kwh",      1150, u32, 0),
        _pt("tot_discharge_kwh",   1152, u32, 0),
        _pt("daily_charge_kwh",    1154, u32, 0),
        _pt("daily_discharge_kwh", 1156, u32, 0),
        _pt("tot_charge_time",     1158, u32, 0),
        _pt("tot_discharge_time",  1160, u32, 0),
        _pt("daily_charge_time",   1162, u32, 0),
        _pt("daily_discharge_time",1164, u32, 0),
        # ── 寫入點（GC 發送 PQ 指令）──
        _pt("PQ_p_ref",  13200, i16, 0, writable=True),
        _pt("PQ_q_ref",  13201, i16, 0, writable=True),
        _pt("PCS_on_off", 5500, u16, 0, writable=True),
    ]
    return SimulatedDeviceConfig(device_id="pcs_sim", unit_id=1, points=tuple(points))


class PcsSimulator(BaseDeviceSimulator):
    """PCS 模擬器：接收 PQ_p_ref / PQ_q_ref 寫入並即時回饋至 p / q 讀取暫存器。"""

    def on_write(self, name: str, old_value: Any, new_value: Any) -> None:
        super().on_write(name, old_value, new_value)
        if name == "PQ_p_ref":
            # GC 寫入 raw（scale=0.1），同步至 p(addr=8) 讓 GC 讀回確認
            self.set_value("p", new_value)
        elif name == "PQ_q_ref":
            self.set_value("q", new_value)

    async def update(self) -> None:
        pass


# ─── CATL BMS (unit_id=2) ────────────────────────────────────────────────────
# ReadPoint 對應 devices/CATL_bms_V0_0_0.py
# offset=-20000 類：raw = physical + 20000（0A → 20000）
# offset=-50 類（溫度）：raw = physical + 50（25°C → 75）
# offset=-2000 scale=0.1 類（機架電流/功率）：raw = (physical + 2000) / 0.1

def build_bms_config() -> SimulatedDeviceConfig:
    u16, u32 = UInt16(), UInt32()
    points = [
        # ── 系統電氣量 (0x0020–0x0044) ──
        _pt("v",       0x0020, u16, 10000),  # 1000V × 10 (scale=0.1)
        _pt("i",       0x0021, u16, 20000),  # 0A (offset -20000 → raw=20000)
        _pt("soc",     0x0022, u16, 500),    # 50% × 10 (scale=0.1)
        _pt("soh",     0x0023, u16, 1000),   # 100%
        _pt("v_max_cell", 0x0024, u16, 3400),  # 3.4V × 1000 (scale=0.001)
        _pt("v_min_cell", 0x0025, u16, 3300),  # 3.3V
        _pt("v_avg_cell", 0x0026, u16, 3350),  # 3.35V
        _pt("temp_max_cell", 0x0027, u16, 75), # 25°C + 50
        _pt("temp_min_cell", 0x0028, u16, 70), # 20°C + 50
        _pt("temp_avg_cell", 0x0029, u16, 72), # 22°C + 50
        _pt("i_max_charge",    0x002a, u16, 20500),  # 500A + 20000
        _pt("i_max_discharge", 0x002b, u16, 20500),
        _pt("p_max_charge",    0x002c, u16, 20500),  # 500kW + 20000
        _pt("p_max_discharge", 0x002d, u16, 20500),
        _pt("p",               0x002e, u16, 20000),  # 0kW (offset -20000)
        _pt("soe_charge",    0x002f, u16, 2000),  # 200kWh × 10 (scale=0.1)
        _pt("soe_discharge", 0x0030, u16, 2000),
        _pt("sys_charge_remain_energy",    0x0031, u16, 200),
        _pt("sys_discharge_remain_energy", 0x0032, u16, 200),
        _pt("v_max_charge",   0x0033, u16, 10500),  # 1050V
        _pt("v_min_discharge",0x0034, u16, 9500),   # 950V
        _pt("ins_det_fun_status", 0x0035, u16, 0),
        _pt("pos_ins_gnd",    0x0036, u16, 1000),
        _pt("neg_ins_gnd",    0x0037, u16, 1000),
        _pt("sys_avg_temp_environment_MBMU1", 0x0038, u16, 75),  # 25°C
        _pt("sys_avg_temp_environment_MBMU2", 0x0039, u16, 75),
        _pt("sys_ambient_humi_MBMU1", 0x003e, u16, 60),
        _pt("sys_ambient_humi_MBMU2", 0x003f, u16, 60),
        _pt("sys_soc_maintenance_req_status", 0x0044, u16, 0),
        # ── TMS 熱管理 (0x0060–0x0083)，溫度 raw = physical + 50 ──
        _pt("BMS_TMS1_mode", 0x0060, u16, 0),
        _pt("TMS1_temp_set_by_bms", 0x0061, u16, 75),
        _pt("TMS1_real_mode", 0x0062, u16, 0),
        _pt("Rack_inlet_temp_TMS1_Outlet", 0x0063, u16, 75),
        _pt("Rack_outlet_temp_TMS1_inlet", 0x0064, u16, 75),
        _pt("TMS1_environment_temp", 0x0065, u16, 75),
        _pt("TMS1_fault_code",  0x0066, u16, 0),
        _pt("TMS1_fault_level", 0x0067, u16, 0),
        _pt("TMS1_cooling_mode_protection", 0x0068, u16, 0),
        _pt("BMS_TMS2_mode", 0x0069, u16, 0),
        _pt("TMS2_temp_set_by_bms", 0x006a, u16, 75),
        _pt("TMS2_real_mode", 0x006b, u16, 0),
        _pt("Rack_inlet_temp_TMS2_Outlet", 0x006c, u16, 75),
        _pt("Rack_outlet_temp_TMS2_inlet", 0x006d, u16, 75),
        _pt("TMS2_environment_temp", 0x006e, u16, 75),
        _pt("TMS2_fault_code",  0x006f, u16, 0),
        _pt("TMS2_fault_level", 0x0070, u16, 0),
        _pt("TMS2_cooling_mode_protection", 0x0071, u16, 0),
        _pt("BMS_TMS3_mode", 0x0072, u16, 0),
        _pt("TMS3_temp_set_by_bms", 0x0073, u16, 75),
        _pt("TMS3_real_mode", 0x0074, u16, 0),
        _pt("Rack_inlet_temp_TMS3_Outlet", 0x0075, u16, 75),
        _pt("Rack_outlet_temp_TMS3_inlet", 0x0076, u16, 75),
        _pt("TMS3_environment_temp", 0x0077, u16, 75),
        _pt("TMS3_fault_code",  0x0078, u16, 0),
        _pt("TMS3_fault_level", 0x0079, u16, 0),
        _pt("TMS3_cooling_mode_protection", 0x007a, u16, 0),
        _pt("BMS_TMS4_mode", 0x007b, u16, 0),
        _pt("TMS4_temp_set_by_bms", 0x007c, u16, 75),
        _pt("TMS4_real_mode", 0x007d, u16, 0),
        _pt("Rack_inlet_temp_TMS4_Outlet", 0x007e, u16, 75),
        _pt("Rack_outlet_temp_TMS4_inlet", 0x007f, u16, 75),
        _pt("TMS4_environment_temp", 0x0080, u16, 75),
        _pt("TMS4_fault_code",  0x0081, u16, 0),
        _pt("TMS4_fault_level", 0x0082, u16, 0),
        _pt("TMS4_cooling_mode_protection", 0x0083, u16, 0),
        # ── BMS 命令介面狀態 (0x0300–0x0306) ──
        _pt("BMS_heartbeat", 0x0300, u16, 0),
        _pt("BMS_power_on",  0x0301, u16, 1),   # 1 = ready（GC 策略必須為 1）
        _pt("BMS_status",    0x0302, u16, 1),
        _pt("number_connected_hv_bms", 0x0304, u16, 1),
        _pt("sys_step_charge_mode",    0x0305, u16, 0),
        _pt("number_of_racks",         0x0306, u16, 1),
        # ── EMS 命令介面讀回 (0x0380–0x038c) ── 兼作 HeartbeatService 寫入目標
        _pt("ems_heartbeat",  0x0380, u16, 0, writable=True),
        _pt("ems_cmd",        0x0381, u16, 0, writable=True),
        _pt("fault_clear_cmd",0x038c, u16, 0, writable=True),
        # ── 機架狀態 (0x0410–0x0456) ──
        _pt("preChg_relay_rack",  0x0410, u16, 0),
        _pt("relay_pos_rack",     0x0411, u16, 1),
        _pt("relay_neg_rack",     0x0412, u16, 1),
        _pt("HV_online_rack",     0x0413, u16, 1),
        _pt("bat_rack_maintenance", 0x0414, u16, 0),
        _pt("v_rack",    0x0420, u16, 10000),  # 1000V
        _pt("i_rack",    0x0422, u16, 20000),  # 0A (offset=-2000, scale=0.1 → raw=(0+2000)/0.1=20000)
        _pt("soc_rack",  0x0423, u16, 500),    # 50%
        _pt("soh_rack",  0x0424, u16, 1000),   # 100%
        _pt("cv_max_rack", 0x0425, u16, 3400),
        _pt("cv_min_rack", 0x0426, u16, 3300),
        _pt("cv_avg_rack", 0x0427, u16, 3350),
        _pt("ct_max_rack", 0x0428, u16, 75),
        _pt("ct_min_rack", 0x0429, u16, 70),
        _pt("ct_avg_rack", 0x042a, u16, 72),
        _pt("p_rack",    0x042f, u16, 20000),  # 0kW
        _pt("chg_SOE_rack",   0x0434, u16, 2000),
        _pt("dischg_SOE_rack",0x0435, u16, 2000),
        _pt("kwh_chg_rack",   0x0454, u32, 0),
        _pt("kwh_dischg_rack",0x0456, u32, 0),
    ]
    return SimulatedDeviceConfig(device_id="bms_sim", unit_id=2, points=tuple(points))


class BmsSimulator(BaseDeviceSimulator):
    """BMS 模擬器：回應 BMS_heartbeat / BMS_on_off / BMS_fault_clear 寫入。"""

    def on_write(self, name: str, old_value: Any, new_value: Any) -> None:
        super().on_write(name, old_value, new_value)
        if name == "ems_heartbeat":
            # EMS 心跳寫入，同步回 BMS_heartbeat 讀取點讓 GC 可驗證
            self.set_value("BMS_heartbeat", new_value)
        elif name == "ems_cmd":
            # BMS_on_off: 2=ON → BMS_power_on=1, 3=OFF → BMS_power_on=0
            cmd = int(new_value)
            if cmd == 2:
                self.set_value("BMS_power_on", 1)
            elif cmd == 3:
                self.set_value("BMS_power_on", 0)
        elif name == "fault_clear_cmd":
            # fault clear 寫入後立即清 0（模擬 BMS 處理完成）
            if int(new_value) == 1:
                self.set_value("fault_clear_cmd", 0)

    async def update(self) -> None:
        pass


# ─── PM335 Total Power Meter (unit_id=3) ─────────────────────────────────────
# ReadPoint 對應 devices/PM335_pm_V0_0_0.py，全部使用 LITTLE_ENDIAN
# 模擬 300kW 總負載：p raw=300000（scale=0.001）

def build_totm_config() -> SimulatedDeviceConfig:
    i32, u32 = Int32(), UInt32()
    points = [
        # ── 功率 ──
        _pt("p",    13696, i32, 300000, byte_order=_LE),  # 300kW (scale=0.001)
        # ── 各相電壓 (scale=0.1) ──
        _pt("v_a",  13952, u32, 2200, byte_order=_LE),   # 220V
        _pt("v_b",  13954, u32, 2200, byte_order=_LE),
        _pt("v_c",  13956, u32, 2200, byte_order=_LE),
        # ── 各相電流 (scale=0.01) ──
        _pt("i_a",  13958, u32, 45500, byte_order=_LE),  # 455A (≈300kW/3/220V)
        _pt("i_b",  13960, u32, 45500, byte_order=_LE),
        _pt("i_c",  13962, u32, 45500, byte_order=_LE),
        # ── 各相功率 (scale=0.001) ──
        _pt("p_a",  13964, i32, 100000, byte_order=_LE), # 100kW
        _pt("p_b",  13966, i32, 100000, byte_order=_LE),
        _pt("p_c",  13968, i32, 100000, byte_order=_LE),
        # ── 各相虛功/視在功率/功率因數 ──
        _pt("q_a",  13970, i32, 0,      byte_order=_LE),
        _pt("q_b",  13972, i32, 0,      byte_order=_LE),
        _pt("q_c",  13974, i32, 0,      byte_order=_LE),
        _pt("s_a",  13976, i32, 100000, byte_order=_LE), # 100kVA
        _pt("s_b",  13978, i32, 100000, byte_order=_LE),
        _pt("s_c",  13980, i32, 100000, byte_order=_LE),
        _pt("pf_a", 13982, i32, 1000,   byte_order=_LE), # 1.0
        _pt("pf_b", 13984, i32, 1000,   byte_order=_LE),
        _pt("pf_c", 13986, i32, 1000,   byte_order=_LE),
        # ── 線電壓 (scale=0.1) ──
        _pt("vl_ab",14012, u32, 3800,   byte_order=_LE), # 380V
        _pt("vl_bc",14014, u32, 3800,   byte_order=_LE),
        _pt("vl_ca",14016, u32, 3800,   byte_order=_LE),
        # ── 總虛功/視在功率/功率因數 ──
        _pt("q",    14338, i32, 0,      byte_order=_LE),
        _pt("s",    14340, u32, 300000, byte_order=_LE), # 300kVA
        _pt("pf",   14342, i32, 1000,   byte_order=_LE),
        # ── 平均電壓/電流/頻率 ──
        _pt("v",    14356, u32, 2200,   byte_order=_LE), # 220V
        _pt("vl",   14358, u32, 3800,   byte_order=_LE), # 380V
        _pt("i",    14360, u32, 45500,  byte_order=_LE), # 455A
        _pt("f",    14468, u32, 6000,   byte_order=_LE), # 60Hz (scale=0.01)
        # ── 電量累積 (Read Input Registers, 但 datablock 統一，raw=0) ──
        _pt("imp_kwh",  14720, u32, 0,  byte_order=_LE),
        _pt("exp_kwh",  14722, u32, 0,  byte_order=_LE),
        _pt("tot_kwh",  14726, u32, 0,  byte_order=_LE),
        _pt("imp_kvarh",14728, u32, 0,  byte_order=_LE),
        _pt("exp_kvarh",14730, u32, 0,  byte_order=_LE),
        _pt("tot_kvarh",14734, u32, 0,  byte_order=_LE),
        _pt("imp_kvah", 14742, u32, 0,  byte_order=_LE),
        _pt("exp_kvah", 14744, u32, 0,  byte_order=_LE),
        _pt("tot_kvah", 14736, u32, 0,  byte_order=_LE),
        # ── 需量 (scale=0.001) ──
        _pt("ins_kw",   14610, u32, 300000, byte_order=_LE), # 300kW
        _pt("ins_kvar", 14612, u32, 0,      byte_order=_LE),
        _pt("ins_kva",  14614, u32, 300000, byte_order=_LE),
        _pt("acc_kw",   14622, u32, 300000, byte_order=_LE),
        _pt("acc_kvar", 14624, u32, 0,      byte_order=_LE),
    ]
    return SimulatedDeviceConfig(device_id="totm_sim", unit_id=3, points=tuple(points))


# ─── SE4900 Branch Power Meter (unit_ids=4,5,6) ───────────────────────────────
# ReadPoint 對應 devices/SE4900_pm_V0_0_0.py，全部 BIG_ENDIAN
# 各 ACM 模擬不同分支負載：acm1=100kW, acm2=80kW, acm3=120kW

_ACM_P_RAW = {4: 100000, 5: 80000, 6: 120000}  # unit_id → p raw (scale=0.001)

def build_acm_config(unit_id: int) -> SimulatedDeviceConfig:
    u16, u32 = UInt16(), UInt32()
    p_raw = _ACM_P_RAW.get(unit_id, 100000)
    p_phase_raw = p_raw // 3
    points = [
        _pt("acc_kwh", 0x0000, u32, 0),
        _pt("v",       0x0002, u32, 2200),  # 220V (scale=0.1)
        _pt("i",       0x0004, u32, 0),
        _pt("p",       0x0006, u32, p_raw),
        _pt("v_a",     0x0008, u32, 2200),
        _pt("v_b",     0x000A, u32, 2200),
        _pt("v_c",     0x000C, u32, 2200),
        _pt("i_a",     0x000E, u32, 0),
        _pt("i_b",     0x0010, u32, 0),
        _pt("i_c",     0x0012, u32, 0),
        _pt("p_a",     0x0020, u16, p_phase_raw),
        _pt("p_b",     0x0021, u16, p_phase_raw),
        _pt("p_c",     0x0022, u16, p_phase_raw),
    ]
    return SimulatedDeviceConfig(
        device_id=f"acm_{unit_id - 3:02d}_sim",
        unit_id=unit_id,
        points=tuple(points),
    )


# ─── VAW DC Meter (unit_id=7) ────────────────────────────────────────────────
# ReadPoint 對應 devices/VAW_dcm_V0_0_0.py，全部 BIG_ENDIAN

def build_dcm_config() -> SimulatedDeviceConfig:
    i16, i32, u32 = Int16(), Int32(), UInt32()
    points = [
        _pt("exp_kwh",0x0000, u32, 0),
        _pt("imp_kwh",0x0003, i32, 0),
        _pt("p",      0x001E, i32, 0),      # 0kW (scale=0.01)
        _pt("v",      0x002A, i16, 1000),   # 1000V DC (scale=1)
        _pt("i",      0x002B, i16, 0),
    ]
    return SimulatedDeviceConfig(device_id="dcm_sim", unit_id=7, points=tuple(points))


# ─── ET7x00 Remote I/O (unit_id=8) ───────────────────────────────────────────
# ReadPoint 對應 devices/ET7x00_rio_V0_0_0.py，FC02（server 統一 datablock）
# switch_on=1, switch_off=0 → 開關為 open 狀態

def build_rio_config() -> SimulatedDeviceConfig:
    u16 = UInt16()
    points = [
        _pt("switch_on",  0x0000, u16, 1),  # 1 = open（開關 ON 狀態）
        _pt("switch_off", 0x0001, u16, 0),
    ]
    return SimulatedDeviceConfig(device_id="rio_sim", unit_id=8, points=tuple(points))


# ─── 靜態模擬器基類 ────────────────────────────────────────────────────────────

class StaticSimulator(BaseDeviceSimulator):
    """靜態模擬器：不執行任何動態更新，僅維持初始值。"""

    async def update(self) -> None:
        pass


# ─── 主程式 ──────────────────────────────────────────────────────────────────

async def main() -> None:
    server = SimulationServer(ServerConfig(host="0.0.0.0", port=5020, tick_interval=1.0))

    server.add_simulator(PcsSimulator(build_pcs_config()))
    server.add_simulator(BmsSimulator(build_bms_config()))
    server.add_simulator(StaticSimulator(build_totm_config()))
    server.add_simulator(StaticSimulator(build_acm_config(4)))
    server.add_simulator(StaticSimulator(build_acm_config(5)))
    server.add_simulator(StaticSimulator(build_acm_config(6)))
    server.add_simulator(StaticSimulator(build_dcm_config()))
    server.add_simulator(StaticSimulator(build_rio_config()))

    async with server:
        print("=" * 56)
        print("GC Simulation Server running on 0.0.0.0:5020")
        print()
        print("  unit_id  device")
        print("  ───────  ──────────────────")
        print("     1     PCS  (SNPOWER pcs_01)")
        print("     2     BMS  (CATL bms_01)")
        print("     3     TOTM (PM335 totm_01)")
        print("     4     ACM1 (SE4900 acm_01)")
        print("     5     ACM2 (SE4900 acm_02)")
        print("     6     ACM3 (SE4900 acm_03)")
        print("     7     DCM  (VAW dcm_01)")
        print("     8     RIO  (ET7x00 rio_01)")
        print("=" * 56)
        await server.serve()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSimulation server stopped.")
