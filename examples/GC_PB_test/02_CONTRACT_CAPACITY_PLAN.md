# 契約容量保護與需量服務 — 設計計畫

> 本文件僅為設計計畫，尚未實作。
> 採用 `_demand_service` 注入 pattern（對齊 `_pv_service`）作為主要架構。

---

## 1. 介紹

### 1.1 問題背景

台電契約容量計費以 **每 15 分鐘一個固定時鐘週期** 為單位，對齊每小時的 `00` / `15` / `30` / `45` 分（需量週期）。計費以每週期的「平均功率」是否超過契約容量為準。既有 [examples/GC_PB_test/protection.py](protection.py) 的 `ContractCapacityProtection` 未完成（第 129 行後中斷），且採「由當前時刻往回滾動 N × window_size」的**滾動窗口邏輯**——與實際計費週期不對齊，可能誤判超約，且無法表達「當前週期剩餘時間內最大允許功率」的核心概念。

此外，實務需求擴充如下：
- **多段契約**：經常／半尖峰／週六半尖／離峰，依時段切換。
- **調度回應**：即時備轉、補充備轉等輔助服務指令，需臨時縮減場域契約上限。
- **告警**：接近／超約狀態需沿用既有設備告警管道（Redis Set + pub/sub）。
- **多消費者**：保護規則、策略（peak shaving、tariff optimizer）、告警皆需讀取需量指標，不應各自重算。

### 1.2 設計原則與方案選擇

**原則**：需量計算與防超約 clamp **職責分離**——前者為資料生產者，後者為純消費者。

csp_lib 已有「計算層 service 注入」pattern：
- `SystemControllerConfig._pv_service: PVDataService | None`（見 [csp_lib/integration/system_controller.py:74-129](../../csp_lib/integration/system_controller.py#L74-L129) 與 [csp_lib/controller/services/pv_data_service.py](../../csp_lib/controller/services/pv_data_service.py)）
- 建立於 controller init、由 `DeviceDataFeed` 餵料、由 strategy 直接讀取
- 同族：`_data_feed`、`_heartbeat`

本計畫沿用此 pattern，新增 `_demand_service: DemandService | None`，**不新增 virtual device process、不增加 Redis IPC 開銷**。

### 1.3 架構概觀

```
┌─ SystemController (strategy process) ────────────────────────────┐
│                                                                   │
│  DeviceDataFeed ────► DemandService ◄──── DispatchOverrideFeed   │
│   (meter_power)         (純計算)         (Redis: dispatch:overrides)│
│                          │                                         │
│                          ▼                                         │
│                     DemandState ──────► context.extra              │
│                                          │                         │
│                    ┌─────────────────────┼──────────────────┐     │
│                    ▼                     ▼                  ▼     │
│         ContractCapacityProtection  PeakShaving    AlarmRule       │
│            (clamp p_target)        Strategy       (產設備告警)      │
└───────────────────────────────────────────────────────────────────┘
```

`DemandService` in-process、跨 tick 累積狀態；`DemandState` 作為快照透過 `context.extra["demand_state"]` 提供給所有消費者，介面一致、無序列化成本。

---

## 2. 實作

### 2.1 資料結構

```python
# csp_lib/controller/services/demand_service.py

@dataclass(frozen=True, slots=True)
class DemandState:
    # 時鐘對齊
    window_start: datetime
    window_end: datetime
    window_elapsed_sec: float
    window_remaining_sec: float
    window_progress: float                     # 0.0..1.0

    # 契約（含多段 + 調度）
    base_contract_kW: float
    effective_contract_kW: float
    contract_source_tier: str                  # "經常" / "半尖峰" / "週六半尖" / "離峰"
    next_tier_switch: datetime | None
    active_dispatch_overrides: tuple[DispatchOverride, ...]

    # 當前週期即時指標
    instantaneous_power_kW: float
    consumed_energy_kWh: float
    running_avg_kW: float
    sample_count_in_window: int

    # 預測（兩種並存）
    projected_final_avg_kW: float              # 線性外推（給告警／策略）
    projected_final_avg_ewma_kW: float         # EWMA 外推（給保護 clamp）
    max_allowed_remaining_avg_kW: float        # 基於 effective_contract
    time_to_breach_sec: float | None

    # 歷史
    historical_window_avgs_kW: tuple[float, ...]
    historical_peak_kW: float
    historical_trend_kW_per_window: float

    # 旗標
    is_partial_window: bool
    is_breached: bool
    warning_level: Literal["ok", "near", "over"]
    data_stale: bool
```

### 2.2 服務介面

```python
# csp_lib/controller/services/demand_service.py

class DemandService:
    def __init__(
        self,
        tariff: TariffCalendar,
        dispatch_store: DispatchOverrideStore,
        demand_period_minutes: int = 15,
        history_windows: int = 4,
        ewma_alpha: float = 0.3,
        warning_ratio: float = 0.90,
        stale_threshold_sec: float = 10.0,
    ) -> None: ...

    # 餵料
    def ingest_meter(self, timestamp: datetime, power_kW: float) -> None: ...
    def apply_overrides(self, overrides: list[DispatchOverride]) -> None: ...

    # 讀取
    def snapshot(self, t_now: datetime | None = None) -> DemandState: ...

    # HA 支援
    def export_for_persistence(self) -> dict: ...
    def restore(self, persisted: dict) -> None: ...

    # 運維
    def reset(self) -> None: ...
```

### 2.3 輔助元件

```python
# csp_lib/controller/services/tariff.py

@dataclass(frozen=True, slots=True)
class ContractTier:
    name: str                              # "經常" / "半尖峰" / "週六半尖" / "離峰"
    contract_kW: float
    weekdays: frozenset[int]               # 0=Mon .. 6=Sun
    time_ranges: tuple[tuple[time, time], ...]
    season: Literal["summer", "winter", "all"]
    priority: int                          # 多段重疊時取 priority 最小者

class TariffCalendar:
    def __init__(
        self,
        tiers: list[ContractTier],
        summer_months: frozenset[int] = frozenset({6, 7, 8, 9}),
    ) -> None: ...
    def base_contract_kW(self, t: datetime) -> float: ...
    def current_tier(self, t: datetime) -> ContractTier: ...
    def next_tier_boundary(self, t: datetime) -> datetime: ...
```

```python
# csp_lib/controller/services/dispatch.py

@dataclass(frozen=True, slots=True)
class DispatchOverride:
    source: Literal["spinning_reserve", "supplemental_reserve", "manual_curtailment"]
    reduction_kW: float                    # 從 base_contract_kW 減去
    start: datetime
    end: datetime                          # 必填，防止忘記撤除
    reason: str = ""

class DispatchOverrideStore:
    def add(self, override: DispatchOverride) -> None: ...
    def remove(self, source: str) -> None: ...
    def active(self, t: datetime) -> list[DispatchOverride]: ...   # 自動過濾過期
    def total_reduction_kW(self, t: datetime) -> float: ...
```

**跨時段週期處理**：需量週期與 tier 邊界不一定對齊。採**保守做法**：以週期起始時刻的 tier 為準（進入新 tier 當下的週期仍套用前一 tier）。

### 2.4 Feed 層

```python
# csp_lib/integration/feeds/dispatch_override_feed.py

class DispatchOverrideFeed:
    """訂閱 Redis dispatch:overrides，推入 DemandService。"""
    def __init__(
        self,
        redis_client,
        demand_service: DemandService,
        key: str = "dispatch:overrides",
        poll_interval_sec: float = 1.0,
    ) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

**meter 餵料方式**（擇一，Phase 0 決定）：
- (a) 擴充既有 `DeviceDataFeed`，支援 meter_power → `demand_service.ingest_meter()`
- (b) 新增 `MeterDataFeed`，與 `DeviceDataFeed` 平行

### 2.5 Controller 整合

```python
# csp_lib/integration/system_controller.py

@dataclass
class SystemControllerConfig:
    ...
    _demand_service: DemandService | None = None
    demand_config: DemandConfig | None = None          # tiers + warning_ratio + history_windows ...

class SystemController:
    def __init__(self, config: SystemControllerConfig, ...):
        ...
        if config.demand_config is not None:
            self._demand_service = DemandService(
                tariff=TariffCalendar(config.demand_config.tiers),
                dispatch_store=DispatchOverrideStore(),
                demand_period_minutes=config.demand_config.demand_period_minutes,
                history_windows=config.demand_config.history_windows,
                ewma_alpha=config.demand_config.ewma_alpha,
                warning_ratio=config.demand_config.warning_ratio,
            )
            # meter 餵料掛到 DeviceDataFeed (或新 MeterDataFeed)
            # 啟動 DispatchOverrideFeed

    def _build_context(self, t_now: datetime) -> StrategyContext:
        ctx = ...
        if self._demand_service is not None:
            ctx.extra["demand_state"] = self._demand_service.snapshot(t_now)
        return ctx
```

### 2.6 消費端 — `ContractCapacityProtection` 重寫

```python
# examples/GC_PB_test/protection.py

class ContractCapacityProtection(ProtectionRule):
    """防超約 clamp：僅消費 DemandState，不做任何積分。

    保守模式（預設）：warning_level != "ok" 且 p_target < 0 時禁止充電。
    """
    def __init__(self, demand_state_key: str = "demand_state") -> None:
        self._demand_state_key = demand_state_key
        self._is_triggered = False

    @property
    def name(self) -> str:
        return "contract_capacity_protection"

    @property
    def is_triggered(self) -> bool:
        return self._is_triggered

    def evaluate(self, command: Command, context: StrategyContext) -> Command:
        state: DemandState | None = context.extra.get(self._demand_state_key)
        if state is None or state.is_partial_window:
            self._is_triggered = False
            return command

        # 保守策略：接近或超過契約時禁止充電
        if state.warning_level != "ok" and command.p_target < 0:
            self._is_triggered = True
            logger.warning(
                f"Contract protection: window=[{state.window_start:%H:%M}, "
                f"{state.window_end:%H:%M}], tier={state.contract_source_tier}, "
                f"consumed={state.consumed_energy_kWh:.2f}kWh, "
                f"effective_contract={state.effective_contract_kW:.1f}kW, "
                f"level={state.warning_level}, clamp P {command.p_target}→0"
            )
            return command.with_p(0.0)

        self._is_triggered = False
        return command
```

### 2.7 告警規則（可選但建議）

```python
# examples/GC_PB_test/protection.py

class ContractCapacityAlarmRule:
    """將 DemandState.warning_level 升級為既有設備告警管道的告警事件。

    告警 codes:
      DEMAND_NEAR         — warning_level == "near"
      DEMAND_BREACHED     — is_breached == True
      DEMAND_TREND_RISING — historical_trend_kW_per_window > threshold 且連續 K 週期
      DEMAND_DATA_STALE   — data_stale == True
      DISPATCH_ACTIVE     — active_dispatch_overrides 非空
    """
    # 沿用既有 StateSyncManager / device alarm pattern 推送告警
```

### 2.8 HA 狀態恢復

Strategy process 切換（leader election 結果變動）時，standby 接管後 `DemandService.restore()` 從 Redis key `demand_service:state` 載入：
- `window_start`（避免重新對齊造成週期斷點）
- `consumed_energy_kWh`（當前週期已累積）
- `last_sample_timestamp`、`last_ewma_power_kW`（EWMA 狀態）
- `historical_window_avgs_kW`（歷史窗滾動陣列）

每次 `snapshot()` 後將上述最小欄位寫回 Redis（TTL 略大於一個週期）。若 TTL 過期則視為冷啟動（`is_partial_window=True`）。

### 2.9 預測演算法說明

| 特性 | 線性外推（Linear） | EWMA |
|---|---|---|
| 公式 | `P_predict = P_latest` | `P_ewma = α·P_latest + (1−α)·P_ewma_prev` |
| 對突波反應 | 立即、敏感 | 平滑、延遲 |
| 對電表雜訊 | 高敏感、易誤判 | 抗雜訊、穩定 |
| 適用 | 告警、策略（要最快偵測） | 保護 clamp（避免誤觸） |
| α 建議 | — | `0.3` 保護、`0.5-0.7` 策略 |

`DemandState` 同時輸出兩者，consumer 自選。

### 2.10 實作順序

1. **Phase 0 — POC 決策**
   - 決定 meter 餵料方式：擴充 `DeviceDataFeed` vs. 新增 `MeterDataFeed`
   - 決定 `dispatch:overrides` Redis key naming 與 JSON schema
   - 決定 HA restore 最小欄位集合
2. **Phase 1 — 純計算核心**
   - `TariffCalendar` + `DispatchOverrideStore` + `DemandService`（含 export/restore）
   - 完整單元測試
3. **Phase 2 — Feed 與 Controller 整合**
   - `DispatchOverrideFeed` 實作
   - `SystemControllerConfig._demand_service` / `demand_config` 欄位
   - `SystemController.__init__` 條件建立
   - `_build_context` 注入 `demand_state`
4. **Phase 3 — 消費端**
   - 重寫 `ContractCapacityProtection`
   - 新增 `ContractCapacityAlarmRule`
5. **Phase 4 — 整合驗證**
   - GC_PB_test sim loop 跑多種情境
   - HA 切換場景（kill strategy leader → standby 接管）

### 2.11 驗證

**單元測試**（新增於 `tests/`）：
- `test_tariff_calendar.py`：時段切換、夏／冬月、priority、週六時段
- `test_dispatch_override_store.py`：過期清理、多筆累加、新增／移除
- `test_demand_service.py`：
  - 週期邊界對齊（10:14:59 → 10:15:00）
  - 梯形積分正確性
  - 線性 vs EWMA 輸出差異（突波情境）
  - `effective_contract_kW = base − override` 正確
  - 冷啟動 `is_partial_window=True`
  - 超約偵測 `is_breached=True` 且 `max_allowed_remaining_avg_kW=0`
  - 電表 stale → `data_stale=True`
  - `export_for_persistence` / `restore` round-trip 正確
- `test_contract_capacity_protection.py`：mock `DemandState` 驗證 clamp 決策

**整合測試**（GC_PB_test）：
- 啟動 controller，觀察 `context.extra["demand_state"]` 每 tick 更新
- 人工寫入 `dispatch:overrides` Redis key，下個 tick `effective_contract_kW` 即時反映
- 觸發高負載情境：`warning_level="near"` 時 `ContractCapacityAlarmRule` 產設備告警，`ContractCapacityProtection` log 含 window 資訊
- 時段切換邊界（07:30 進入半尖峰）`contract_source_tier` 正確切換
- Kill strategy leader → standby 接管後 window 累積不中斷（HA restore 生效）

---

## 3. 未明確定義的問題

以下項目在進入實作前需確認：

### 3.1 Meter 餵料方式
- **問題**：既有 `DeviceDataFeed` 是否可擴充支援 meter_power → `demand_service.ingest_meter()`？或需新增 `MeterDataFeed`？
- **影響**：Phase 0 決策，影響 `SystemController.__init__` 的整合程式碼
- **需要**：檢視 `DeviceDataFeed` 現有 API 與訂閱模型

### 3.2 `dispatch:overrides` Redis Schema
- **問題**：Redis key 存 List / Hash / JSON blob？寫入方 API 是誰（EMSCommandListener / 外部 HTTP endpoint）？
- **建議 schema（待確認）**：
  ```json
  [
    {
      "source": "spinning_reserve",
      "reduction_kW": 150.0,
      "start": "2026-04-21T14:00:00+08:00",
      "end": "2026-04-21T14:15:00+08:00",
      "reason": "台電調度指令 #20260421-001"
    }
  ]
  ```
- **影響**：`DispatchOverrideFeed` 反序列化邏輯

### 3.3 HA Restore 最小欄位集
- **問題**：哪些欄位必須持久化才能無縫恢復？過度持久化會增加 Redis 寫入壓力
- **候選最小集**：`window_start`、`consumed_energy_kWh`、`last_ewma_power_kW`、`historical_window_avgs_kW`
- **影響**：`DemandService.export_for_persistence()` / `restore()` 實作

### 3.4 EWMA α 實務調校
- **問題**：α=0.3 是先驗值，實際電表雜訊等級未測
- **建議**：Phase 4 整合驗證時以實測資料回測，決定最終值
- **影響**：`DemandConfig.ewma_alpha` 預設值

### 3.5 告警等級閾值
- **問題**：`DEMAND_TREND_RISING` 的「連續 K 週期」K 值、斜率閾值 threshold
- **影響**：`ContractCapacityAlarmRule` 實作

### 3.6 多段契約跨週期的保守策略確認
- **問題**：目前決定「以週期起始時刻 tier 為準」。若客戶希望更精準（例如在 07:30 進半尖峰當下立刻套用新契約），需改為「以較嚴格 tier 為準」或「依比例加權」
- **影響**：`DemandService._current_contract_for_window()` 邏輯

### 3.7 `ContractCapacityProtection` 策略模式選擇
- **目前預設**：保守模式（`warning_level != "ok"` 且 `p_target < 0` 禁止充電）
- **其他候選**：預測模式（mandate 放電補償）、區間模式（輸出允許區間由上層協調）
- **影響**：是否需加 `mode: Literal["conservative", "predictive", "advisory"]` 參數

---

## 附錄：關鍵檔案參考

- 現有 pattern 範本：
  - [csp_lib/integration/system_controller.py](../../csp_lib/integration/system_controller.py)
  - [csp_lib/controller/services/pv_data_service.py](../../csp_lib/controller/services/pv_data_service.py)
- 既有保護規則：
  - [csp_lib/controller/system/protection.py](../../csp_lib/controller/system/protection.py)
  - [csp_lib/controller/system/dynamic_protection.py](../../csp_lib/controller/system/dynamic_protection.py)
- 告警管道：
  - [csp_lib/manager/state/sync.py](../../csp_lib/manager/state/sync.py)
- 相關計畫：
  - [examples/GC_PB_test/01_REDUNDANCY_PLAN.md](01_REDUNDANCY_PLAN.md)
- 待完成（本計畫處理）：
  - [examples/GC_PB_test/protection.py](protection.py)
