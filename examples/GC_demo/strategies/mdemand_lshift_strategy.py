"""
MDemand_LShiftStrategy — 需量管理 + 負載轉移策略

最小化重構自 Referance.py MDemand_LShiftStrategy，
適配 csp_lib v0.4.2 Strategy 介面。

主要修改：
  1. 移除 Singleton 模式 (threading.Lock, __new__)
  2. execute(last_command) → execute(context: StrategyContext)
  3. soc / current_load / last_command 從 context 讀取
  4. 新增 execution_config property
  5. MDemandLShiftParam 合併業務參數與系統限制參數
  6. 計算邏輯 (cal_*, chk_*, update_var) 完全保留
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from csp_lib import logger
from csp_lib.controller.core import Command, ExecutionConfig, ExecutionMode, StrategyContext
from csp_lib.controller.core.strategy import Strategy


# ─── Parameter Dataclass ─────────────────────────────────────────────────────

@dataclass
class MDemandLShiftParam:
    """MDemand_LShift 策略完整參數（業務參數 + 系統限制參數）。

    business params:
        contract_capacity   契約容量 (kW)
        PeakLoad_reserve    尖峰保留容量 (kW)
        dead_zone_soc       SOC 死區 (%)，用於遲滯控制
        NationalHoliday     國定假日日期列表 [datetime.date]
        summer_period       夏月範圍 {"start": datetime, "end": datetime}
        day_X_priority_list 各星期優先級清單 (0-6)
        sum/not_sum_X_dem_soc / peak_soc  各日期 SOC 保留值
        sum/not_sum_X_peak/half_peak_period  各日期時段設定

    system params (set once at startup, can be updated via set_parameter):
        min_soc   最低允許 SOC (%)
        max_soc   最高允許 SOC (%)
        min_p     最低允許功率 (kW, 負值=充電上限)
        max_p     最高允許功率 (kW, 正值=放電上限)
        bess_capacity   電池總容量 (kWh)
        gc_interval     GC 執行週期 (秒)
    """
    # ── 業務參數 ──
    contract_capacity: float = 0.0
    PeakLoad_reserve: float = 0.0
    dead_zone_soc: float = 0.0

    day_0_priority_list: list = field(default_factory=list)
    day_1_priority_list: list = field(default_factory=list)
    day_2_priority_list: list = field(default_factory=list)
    day_3_priority_list: list = field(default_factory=list)
    day_4_priority_list: list = field(default_factory=list)
    day_5_priority_list: list = field(default_factory=list)
    day_6_priority_list: list = field(default_factory=list)

    sum_0_dem_soc: float = 0.0
    sum_1_dem_soc: float = 0.0
    sum_2_dem_soc: float = 0.0
    sum_3_dem_soc: float = 0.0
    sum_4_dem_soc: float = 0.0
    sum_5_dem_soc: float = 0.0
    sum_6_dem_soc: float = 0.0

    not_sum_0_dem_soc: float = 0.0
    not_sum_1_dem_soc: float = 0.0
    not_sum_2_dem_soc: float = 0.0
    not_sum_3_dem_soc: float = 0.0
    not_sum_4_dem_soc: float = 0.0
    not_sum_5_dem_soc: float = 0.0
    not_sum_6_dem_soc: float = 0.0

    sum_0_peak_soc: float = 0.0
    sum_1_peak_soc: float = 0.0
    sum_2_peak_soc: float = 0.0
    sum_3_peak_soc: float = 0.0
    sum_4_peak_soc: float = 0.0
    sum_5_peak_soc: float = 0.0
    sum_6_peak_soc: float = 0.0

    not_sum_0_peak_soc: float = 0.0
    not_sum_1_peak_soc: float = 0.0
    not_sum_2_peak_soc: float = 0.0
    not_sum_3_peak_soc: float = 0.0
    not_sum_4_peak_soc: float = 0.0
    not_sum_5_peak_soc: float = 0.0
    not_sum_6_peak_soc: float = 0.0

    NationalHoliday: list = field(default_factory=list)
    summer_period: dict = field(default_factory=dict)   # {"start": datetime, "end": datetime}

    sum_0_peak_period: list = field(default_factory=list)
    sum_1_peak_period: list = field(default_factory=list)
    sum_2_peak_period: list = field(default_factory=list)
    sum_3_peak_period: list = field(default_factory=list)
    sum_4_peak_period: list = field(default_factory=list)
    sum_5_peak_period: list = field(default_factory=list)
    sum_6_peak_period: list = field(default_factory=list)

    not_sum_0_peak_period: list = field(default_factory=list)
    not_sum_1_peak_period: list = field(default_factory=list)
    not_sum_2_peak_period: list = field(default_factory=list)
    not_sum_3_peak_period: list = field(default_factory=list)
    not_sum_4_peak_period: list = field(default_factory=list)
    not_sum_5_peak_period: list = field(default_factory=list)
    not_sum_6_peak_period: list = field(default_factory=list)

    sum_0_half_peak_period: list = field(default_factory=list)
    sum_1_half_peak_period: list = field(default_factory=list)
    sum_2_half_peak_period: list = field(default_factory=list)
    sum_3_half_peak_period: list = field(default_factory=list)
    sum_4_half_peak_period: list = field(default_factory=list)
    sum_5_half_peak_period: list = field(default_factory=list)
    sum_6_half_peak_period: list = field(default_factory=list)

    not_sum_0_half_peak_period: list = field(default_factory=list)
    not_sum_1_half_peak_period: list = field(default_factory=list)
    not_sum_2_half_peak_period: list = field(default_factory=list)
    not_sum_3_half_peak_period: list = field(default_factory=list)
    not_sum_4_half_peak_period: list = field(default_factory=list)
    not_sum_5_half_peak_period: list = field(default_factory=list)
    not_sum_6_half_peak_period: list = field(default_factory=list)

    # ── 系統限制參數 ──
    min_soc: float = 0.0
    max_soc: float = 100.0
    min_p: float = 0.0          # 最大充電功率（負值）
    max_p: float = 0.0          # 最大放電功率（正值）
    bess_capacity: float = 0.0  # 電池容量 (kWh)
    gc_interval: float = 1.0    # GC 執行週期 (秒)


# ─── Strategy ─────────────────────────────────────────────────────────────────

class MDemandLShiftStrategy(Strategy):
    """需量管理 + 負載轉移策略（csp_lib v0.4.2 版本）。

    從 context 讀取：
        context.soc                      → SOC (%)
        context.extra["current_load"]    → 當前負載 kW（TOTM p 值）
        context.extra["pcs_status_raw"]  → PCS mode1 暫存器原始值
        context.extra["bms_power_on"]    → BMS 上電狀態 (1=ready)
        context.last_command             → 上一次命令

    外部注入（gc_main.py 透過 set_parameter 設定）：
        parameter: MDemandLShiftParam
    """

    def __init__(self) -> None:
        self.parameter = MDemandLShiftParam()
        # 以下執行期狀態變數（每次 execute 前由 context 更新）
        self.soc: float = 0.0
        self.current_load: float = 0.0
        self.last_command: Command = Command()
        # 從 update_var 計算出的中間變數
        self.time_now: datetime = datetime.now()
        self.is_summer: bool = False
        self.max_interval: float = 1.0
        self.period_switcher: dict = {}
        self.soc_dem_switcher: float = 0.0
        self.soc_peak_switcher: float = 0.0
        self.priority_switcher: list = []

    @property
    def execution_config(self) -> ExecutionConfig:
        return ExecutionConfig(mode=ExecutionMode.PERIODIC, interval_seconds=1)

    def set_parameter(self, parameter: MDemandLShiftParam) -> None:
        """更新策略參數（由 gc_main.py MongoDB 輪詢任務呼叫）。"""
        self.parameter = parameter

    # ─── csp_lib Strategy entry point ─────────────────────────────────────────

    def execute(self, context: StrategyContext) -> Command:
        # ── 從 context 同步即時狀態 ──
        self.soc = context.soc or 0.0
        self.last_command = context.last_command
        self.current_load = -(context.extra.get("current_load") or 0.0)   # TOTM p 正值表示進電，策略用負號反向

        pcs_status_raw = context.extra.get("pcs_status_raw") or 0
        pcs_status = 1 if (int(pcs_status_raw) & 0b10) > 0 else 0
        bms_power_on = context.extra.get("bms_power_on")
        mbmu_status = 1 if bms_power_on == 1 else 0

        # ── 裝置就緒保護（對應原 command loop 保護邏輯）──
        if mbmu_status != 1 or pcs_status != 1:
            logger.info(f"MDemandLShift: 裝置未就緒 (bms_power_on={bms_power_on}, pcs_status_raw={pcs_status_raw})，輸出歸零")
            return Command(p_target=0.0, q_target=0.0)

        # ── 原有邏輯（完全保留）──
        self.update_var()
        if self.chk_param():
            return Command(p_target=0.0, q_target=0.0)

        chk_name_list = (
            self.chk_over_contract_capacity,
            self.chk_peak,
            self.chk_half_peak,
            self.chk_under_dem_soc,
        )
        chk_result_list = [chk() for chk in chk_name_list]

        line = "========Strategy========"
        text4 = f"switcher : {self.priority_switcher}"
        text5 = f"chk_name_order : {[chk_name_list[i].__name__ for i in self.priority_switcher]}"
        text0 = f"chk_name_order_result : {[chk_result_list[i] for i in self.priority_switcher]}"
        text1 = f"{self.time_now}, 夏季={self.is_summer}, day={self.time_now.weekday()}, update_period={self.max_interval}, bess_capacity={self.parameter.bess_capacity}"
        text2 = f"soc={self.soc}, min_soc={self.parameter.min_soc}, max_soc={self.parameter.max_soc}, dem_soc={self.soc_dem_switcher}, peak_soc={self.soc_peak_switcher}"
        text3 = f"current_load={self.current_load}, contract_capacity={self.parameter.contract_capacity}, PeakLoad_reserve={self.parameter.PeakLoad_reserve}"
        logger.info(f"\n{line}\n{text4}\n{text5}\n{text0}\n{text1}\n{text2}\n{text3}\n{line}")

        for i in self.priority_switcher:
            if chk_result_list[i]:
                logger.info(f"{chk_name_list[i].__name__} = True")
                if i == 0:      # 超過契約容量
                    return Command(p_target=self.cal_power_diff(), q_target=0.0)
                elif i == 1:    # 尖峰時段
                    return Command(p_target=self.cal_peak(), q_target=0.0)
                elif i == 2:    # 半尖峰時段
                    return Command(p_target=self.cal_half_peak(), q_target=0.0)
                elif i == 3:    # 低於需量管理 SOC 保留值
                    return Command(p_target=self.cal_power_diff(), q_target=0.0)

        # 預設：低於最大 SOC 時充電
        if self.chk_under_max_soc():
            return Command(p_target=self.cal_power_diff(), q_target=0.0)
        return Command(p_target=0.0, q_target=0.0)

    # ─── 參數驗證 ──────────────────────────────────────────────────────────────

    def chk_param(self) -> bool:
        is_error_param = False

        def is_invalid(param_name, param_value, expected_type=(int, float), must_be_positive=True, custom_check=None):
            if param_value is None:
                logger.warning(f"{param_name} 無效，值為: {param_value} (值為 None)")
                return True
            if not isinstance(param_value, expected_type):
                logger.warning(f"{param_name} 類型無效，預期類型: {expected_type}, 實際類型: {type(param_value)}")
                return True
            if must_be_positive and param_value <= 0:
                logger.warning(f"{param_name} 值無效，應大於0，實際值: {param_value}")
                return True
            if custom_check and not custom_check(param_value):
                logger.warning(f"{param_name} 未通過自定義檢查，值為: {param_value}")
                return True
            return False

        try:
            params_to_check = {
                "soc":               (self.soc,                          (int, float), True),
                "min_soc":           (self.parameter.min_soc,            (int, float), True),
                "max_soc":           (self.parameter.max_soc,            (int, float), True),
                "bess_capacity":     (self.parameter.bess_capacity,      (int, float), True),
                "current_load":      (self.current_load,                 (int, float), False),
                "time_now":          (self.time_now,                     (datetime,),  False),
                "contract_capacity": (self.parameter.contract_capacity,  (int, float), True),
                "PeakLoad_reserve":  (self.parameter.PeakLoad_reserve,   (int, float), True),
                "dead_zone_soc":     (self.parameter.dead_zone_soc,      (int, float), True),
            }
            for param_name, (param_value, expected_type, must_be_positive) in params_to_check.items():
                is_error_param |= is_invalid(param_name, param_value, expected_type, must_be_positive)
        except Exception as e:
            logger.exception(f"檢查參數時發生錯誤: {e}")
            is_error_param = True

        return is_error_param

    # ─── 時段/優先級更新 ───────────────────────────────────────────────────────

    def update_var(self) -> None:
        self.time_now = datetime.now()
        summer_period_start = self.parameter.summer_period["start"].replace(year=self.time_now.year).date()
        summer_period_end   = self.parameter.summer_period["end"].replace(year=self.time_now.year).date()
        self.is_summer = summer_period_start <= self.time_now.date() <= summer_period_end
        self.max_interval = max(self.parameter.gc_interval, self.execution_config.interval_seconds)
        prefix = f"{'sum' if self.is_summer else 'not_sum'}_{self.time_now.weekday()}"

        self.period_switcher = {
            peak_level: getattr(self.parameter, f"{prefix}_{peak_level}_period")
            for peak_level in ("peak", "half_peak")
        }

        if self.time_now.date() in self.parameter.NationalHoliday:
            self.priority_switcher = [
                item for item in getattr(self.parameter, f"day_{self.time_now.weekday()}_priority_list")
                if item not in (1, 2, 3)
            ]
        else:
            self.priority_switcher = getattr(self.parameter, f"day_{self.time_now.weekday()}_priority_list")

        self.soc_dem_switcher = (
            getattr(self.parameter, f"{prefix}_dem_soc") if 0 in self.priority_switcher else 0
        )
        self.soc_peak_switcher = getattr(self.parameter, f"{prefix}_peak_soc")

    # ─── 時段判斷 ──────────────────────────────────────────────────────────────

    def chk_in_period(self, period: dict) -> bool:
        if self.time_now.time() < period["end"] or period["end"] == datetime.strptime("00:00", "%H:%M").time():
            if period["start"] <= self.time_now.time():
                return True
        return False

    def chk_peak(self) -> bool:
        if "peak" in self.period_switcher:
            for period in self.period_switcher["peak"]:
                if self.chk_in_period(period):
                    return True
        return False

    def chk_will_peak(self) -> bool:
        if "peak" in self.period_switcher:
            for period in self.period_switcher["peak"]:
                if self.time_now.time() < period["start"] and period["start"] != period["end"]:
                    return True
        return False

    def chk_half_peak(self) -> bool:
        if "half_peak" in self.period_switcher:
            for period in self.period_switcher["half_peak"]:
                if self.chk_in_period(period):
                    if not self.chk_will_peak():
                        return True
        return False

    def chk_over_contract_capacity(self) -> bool:
        return self.last_command.p_target + self.current_load > self.parameter.contract_capacity

    # ─── SOC 狀態判斷 ──────────────────────────────────────────────────────────

    def chk_under_dem_soc(self) -> bool:
        soc_target = self.soc_dem_switcher + self.parameter.min_soc
        if self.soc < soc_target - self.parameter.dead_zone_soc:
            return True
        if self.soc < soc_target and self.last_command.p_target < 0:
            return True
        return False

    def chk_over_dem_soc(self) -> bool:
        soc_target = self.soc_dem_switcher + self.parameter.min_soc
        return self.soc > soc_target + self.parameter.dead_zone_soc

    def chk_over_peak_soc(self) -> bool:
        soc_target = self.soc_peak_switcher + self.soc_dem_switcher + self.parameter.min_soc
        return self.soc > soc_target

    def chk_under_max_soc(self) -> bool:
        if self.soc < self.parameter.max_soc - self.parameter.dead_zone_soc:
            return True
        if self.last_command.p_target < 0:
            return True
        return False

    # ─── 功率計算 ──────────────────────────────────────────────────────────────

    def cal_peak(self) -> float:
        if self.chk_over_dem_soc():
            return max(0.0, self.cal_power_shift())
        return 0.0

    def cal_half_peak(self) -> float:
        if self.chk_will_peak():
            if self.chk_over_peak_soc():
                return max(0.0, self.cal_power_shift())
            return 0.0
        return self.cal_peak()

    def cal_power_diff(self) -> float:
        return self.cal_power_soc_capacity(
            self.last_command.p_target + self.current_load - self.parameter.contract_capacity
        )

    def cal_power_shift(self) -> float:
        return self.cal_power_soc_capacity(
            self.last_command.p_target + self.current_load - self.parameter.PeakLoad_reserve
        )

    def cal_power_soc_capacity(self, power: float) -> float:
        T = self.max_interval / 3600
        charge_gap = (self.soc - self.parameter.max_soc) / 100 * self.parameter.bess_capacity / T
        charge_gap = min(charge_gap, 0.0)
        charge_gap = max(charge_gap, self.parameter.min_p)
        discharge_gap = (self.soc - self.parameter.min_soc) / 100 * self.parameter.bess_capacity / T
        discharge_gap = max(discharge_gap, 0.0)
        discharge_gap = min(discharge_gap, self.parameter.max_p)
        return max(charge_gap, min(discharge_gap, power))
