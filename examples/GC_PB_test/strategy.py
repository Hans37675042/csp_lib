
from dataclasses import dataclass, fields
from datetime import datetime

from csp_lib.core import get_logger
from csp_lib.controller.core import Command, ConfigMixin, ExecutionConfig, ExecutionMode, Strategy, StrategyContext
from csp_lib.controller.strategies.pq_strategy import PQModeConfig, PQModeStrategy

logger = get_logger("Strategy")

class StrategyReprMixin:
    def __str__(self) -> str:
        cls_name = type(self).__name__
        if hasattr(self, '_config') and hasattr(self._config, '__dataclass_fields__'):
            params = ", ".join(
                f"{f.name}={getattr(self._config, f.name)!r}"
                for f in fields(self._config)
            )
        else:
            params = ""
        return f"{cls_name}({params})"

    @property
    def execution_config(self) -> ExecutionConfig:
        return ExecutionConfig(mode=ExecutionMode.PERIODIC, interval_seconds=self.interval_seconds)

# ramp 直接上升 ==============================
@dataclass(frozen=True, slots=True)
class PQ_ramp_ModeConfig():
    p: float = 0.0
    q: float = 0.0
    ramp_p: float = 0.0
    ramp_q: float = 0.0

class PQ_ramp_ModeStrategy(StrategyReprMixin, PQModeStrategy):
    def __init__(self, config: PQ_ramp_ModeConfig | None = None):
        self._config = config or PQ_ramp_ModeConfig()
        self.interval_seconds = 1.0

    def _ramp(self, end: float, last: float, ramp: float) -> float:
        diff = abs(ramp)*self.interval_seconds
        if abs(end - last) > diff and diff > 0:
            return last + diff * (1 if end > last else -1)
        else:                
            return end

    def execute(self, context: StrategyContext) -> Command:
        if context.extra.get("pcs_p") is None:
            msg = "StrategyContext.extra['pcs_p'] is required (PCS active power reading)"
            logger.error(msg)
            raise ValueError(msg)
        if context.extra.get("pcs_q") is None:
            msg = "StrategyContext.extra['pcs_q'] is required (PCS reactive power reading)"
            logger.error(msg)
            raise ValueError(msg)
        p_target = self._ramp(self._config.p, context.extra["pcs_p"], self._config.ramp_p)
        q_target = self._ramp(self._config.q, context.extra["pcs_q"], self._config.ramp_q)
        return Command(p_target=p_target, q_target=q_target)

    def update_config(self, config: PQ_ramp_ModeConfig) -> None:
        if config is None:
            raise ValueError("config must not be None")
        self._config = config

# ramp 設定完成時間 ==============================
@dataclass(frozen=True, slots=True)
class PQ_ramp_Time_ModeConfig():
    '''
    設定執行時間的斜率控制，必須滿足:
    1. start_time, end_time 至少設定一個，若兩者皆設定則忽略 2.
    2. ramp_p, seconds 擇一設定，若兩者皆設定則以ramp_p為主
    '''
    p_start: float = 0.0
    p_end: float = 0.0
    q: float = 0.0

    start_time: datetime | None = None
    end_time: datetime | None = None

    ramp_p: float | None = None
    seconds: int | None = None

    ramp_q: float = 0.0

class PQ_ramp_Time_ModeStrategy(PQ_ramp_ModeStrategy):
    def __init__(self, config: PQ_ramp_Time_ModeConfig | None = None):
        self._config = config or PQ_ramp_Time_ModeConfig()
        self.interval_seconds = 1.0
        self._check()

    def _check(self):
        self._validate_config()
        self.duration, self.ramp_p = self._resolve_duration_and_ramp()
        self.time_start, self.time_end = self._resolve_time_window()

    def _validate_config(self):
        """驗證參數合法性"""
        c = self._config
        if c.start_time is None and c.end_time is None:
            msg = "at least one of start_time or end_time must be provided"
            logger.error(msg)
            raise ValueError(msg)
        if c.start_time == c.end_time:
            msg = "start_time and end_time must differ (zero-length window)"
            logger.error(msg)
            raise ValueError(msg)
        if c.start_time is not None and c.end_time is not None and c.start_time > c.end_time:
            msg = f"start_time ({c.start_time}) must be earlier than end_time ({c.end_time})"
            logger.error(msg)
            raise ValueError(msg)
        if c.start_time is None or c.end_time is None:
            if c.ramp_p is None and c.seconds is None:
                msg = "when only one of start_time/end_time is provided, either ramp_p or seconds must be set to determine duration"
                logger.error(msg)
                raise ValueError(msg)
            if c.ramp_p is not None and c.ramp_p == 0:
                msg = "ramp_p must be non-zero (0 would yield infinite duration)"
                logger.error(msg)
                raise ValueError(msg)
            if c.seconds is not None and c.seconds <= 0:
                msg = f"seconds must be > 0 (got {c.seconds})"
                logger.error(msg)
                raise ValueError(msg)

    def _resolve_duration_and_ramp(self) -> tuple[float, float]:
        """根據 config 決定 duration 與 ramp_p"""
        c = self._config
        p_diff = abs(c.p_end - c.p_start)

        # 兩端時間都有 → 由時間差反推 ramp
        if c.start_time is not None and c.end_time is not None:
            duration = c.end_time.timestamp() - c.start_time.timestamp()
            return duration, p_diff / duration

        # 有 ramp → 由 ramp 反推 duration
        if c.ramp_p is not None:
            ramp = abs(c.ramp_p)
            return p_diff / ramp, ramp

        # 有 seconds → 由 seconds 反推 ramp
        duration = float(c.seconds)
        return duration, p_diff / duration

    def _resolve_time_window(self) -> tuple[float, float]:
        """根據 config 與 duration 決定起訖時間"""
        c = self._config
        if c.start_time is not None and c.end_time is not None:
            return c.start_time.timestamp(), c.end_time.timestamp()
        if c.start_time is not None:
            start = c.start_time.timestamp()
            return start, start + self.duration
        end = c.end_time.timestamp()
        return end - self.duration, end

    def _compute_p_target(self, context: StrategyContext) -> float:
        if context.current_time is None:
            msg = "StrategyContext.current_time is required for time-based PQ ramp strategy"
            logger.error(msg)
            raise ValueError(msg)
        c = self._config
        now = context.current_time.timestamp()
        if now < self.time_start:
            return c.p_start
        if now > self.time_end:
            return c.p_end
        elapsed = now - self.time_start
        sign = 1 if c.p_end > c.p_start else -1
        delta = min(elapsed * self.ramp_p, abs(c.p_end - c.p_start))
        return c.p_start + sign * delta

    def execute(self, context: StrategyContext) -> Command:
        if context.extra.get("pcs_q") is None:
            msg = "StrategyContext.extra['pcs_q'] is required (PCS reactive power reading)"
            logger.error(msg)
            raise ValueError(msg)

        p_target = self._compute_p_target(context)
        q_target = self._ramp(self._config.q, context.extra["pcs_q"], self._config.ramp_q)
        return Command(p_target=p_target, q_target=q_target)

    def update_config(self, config) -> None:
        self._config = config
        self._check()

# 設定 SOC ==============================
@dataclass(frozen=True, slots=True)
class PQ_SOC_ModeConfig():
    soc_target: float

    p: float = 0.0
    q: float = 0.0
    ramp_p: float = 0.0
    ramp_q: float = 0.0

    soc_time: datetime | None = None
    soc_time_interval: int | None = None
    capacity: float | None = None

class PQ_SOC_ModeStrategy(PQ_ramp_ModeStrategy):
    def __init__(self, config: PQ_SOC_ModeConfig) -> None:
        self._config = config or PQ_SOC_ModeConfig()
        self.interval_seconds = 1.0
        self._cached_duration_p: float | None = None
        self._last_recompute_ts: float | None = None
    
    def _compute_p_end(self, context: StrategyContext) -> float:
        soc = context.extra["soc"]
        target = self._config.soc_target

        # 充飽
        if soc >= target:
            return 0.0

        # 未設定充飽時間 → 用固定功率
        if self._config.soc_time is None or self._config.capacity is None:
            return self._config.p

        if context.current_time is None:
            msg = "StrategyContext.current_time is required when soc_time scheduling is enabled"
            logger.error(msg)
            raise ValueError(msg)
        now = context.current_time.timestamp()
        duration_h = (self._config.soc_time.timestamp() - now) / 3600  # 計算剩餘時間（小時）
        if duration_h > 0:
            # 按剩餘電量與剩餘時間計算所需功率（受 soc_time_interval 控制重算週期）
            interval = self._config.soc_time_interval
            now = context.current_time.timestamp()

            if interval is None or self._cached_duration_p is None or self._last_recompute_ts is None:
                self._cached_duration_p = (target - soc) / 100 * self._config.capacity / duration_h
                self._last_recompute_ts = now
                return self._cached_duration_p

            if now - self._last_recompute_ts >= interval:
                self._cached_duration_p = (target - soc) / 100 * self._config.capacity / duration_h
                self._last_recompute_ts = now

            return self._cached_duration_p

        # 已過充電時間
        return 0.0

    def execute(self, context: StrategyContext) -> Command:
        if context.extra.get("soc") is None:
            msg = "StrategyContext.extra['soc'] is required (battery SOC %)"
            logger.error(msg)
            raise ValueError(msg)
        if context.extra.get("pcs_p") is None:
            msg = "StrategyContext.extra['pcs_p'] is required (PCS active power reading)"
            logger.error(msg)
            raise ValueError(msg)
        if context.extra.get("pcs_q") is None:
            msg = "StrategyContext.extra['pcs_q'] is required (PCS reactive power reading)"
            logger.error(msg)
            raise ValueError(msg)

        p_end = self._compute_p_end(context)
        p_target = self._ramp(p_end, context.extra["pcs_p"], self._config.ramp_p)
        q_target = self._ramp(self._config.q, context.extra["pcs_q"], self._config.ramp_q)
        return Command(p_target=p_target, q_target=q_target)

    def update_config(self, config: PQ_SOC_ModeConfig) -> None:
        super().update_config(config)
        self._cached_duration_p = None
        self._last_recompute_ts = None
