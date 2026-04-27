from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass

from csp_lib.controller.core import Command, StrategyContext
from csp_lib.core import get_logger

from .protection import ProtectionRule, SOCProtectionConfig

if TYPE_CHECKING:
    from csp_lib.core.runtime_params import RuntimeParameters

logger = get_logger("Protection")

@dataclass(frozen=True, slots=True)
class ContractCapacityProtectionConfig:
    contract_capacity: float

class ContractCapacityProtection(ProtectionRule):
    """
    契約容量管理
        讀取MOF，滾動判斷是否超約
        計算當前需量是否超約，若需量超約則開始補償
    
    RuntimeParameters keys:
        Contract_capacity: 契約容量
        Now: 現在時間

    Args:
        params: RuntimeParameters 實例
        n_windows: 檢查區間數量
        window_size: 計算區間
    """
    def __init__(
        self,
        params: RuntimeParameters | ContractCapacityProtectionConfig,
        contract_capacity: float = 0.0,
        n_windows: float = 0.0,
        window_size: float = 0.0
    ) -> None:
        self._params = params
        self._contract_capacity_key = contract_capacity
        self._n_windows = n_windows
        self._window_size = window_size
        self._is_triggered = False

    @property
    def name(self) -> str:
        return "contract_capacity_protection"

    @property
    def is_triggered(self) -> bool:
        return self._is_triggered

    def _resolve_limits(self) -> float:
        """Resolve contract_capacity from the parameter source."""
        if isinstance(self._params, ContractCapacityProtectionConfig):
            return self._params.contract_capacity
        else:
            return self._contract_capacity_key

    def _interpolate_window(
        self, mof_ps: list[tuple[float, float]], w_start: float, w_end: float
    ) -> list[tuple[float, float]]:
        """Collect data points within [w_start, w_end], with linear interpolation at boundaries."""
        inside = [(t, p) for t, p in mof_ps if w_start <= t <= w_end]
        before = [(t, p) for t, p in mof_ps if t < w_start]
        after = [(t, p) for t, p in mof_ps if t > w_end]

        points: list[tuple[float, float]] = []

        if inside and inside[0][0] > w_start and before:
            t0, p0 = before[-1]
            t1, p1 = inside[0]
            ratio = (w_start - t0) / (t1 - t0)
            points.append((w_start, p0 + ratio * (p1 - p0)))

        points.extend(inside)

        if inside and inside[-1][0] < w_end and after:
            t0, p0 = inside[-1]
            t1, p1 = after[0]
            ratio = (w_end - t0) / (t1 - t0)
            points.append((w_end, p0 + ratio * (p1 - p0)))

        return points

    def _resample_windows(self, mof_ps: list[tuple[float, float]]) -> list[float]:
        """Resample mof_ps into window averages using trapezoidal area method.

        Returns list of average power per window, ordered from oldest to newest.
        """
        t_end = mof_ps[-1][0]
        n = int(self._n_windows)
        ws = self._window_size
        avg_mof_ps: list[float] = []

        for i in range(n, 0, -1):
            w_start = t_end - i * ws
            w_end = t_end - (i - 1) * ws

            points = self._interpolate_window(mof_ps, w_start, w_end)

            if len(points) == 0:
                continue
            if len(points) == 1:
                avg_mof_ps.append(points[0][1])
                continue

            area = sum(
                (points[j + 1][1] + points[j][1]) / 2 * (points[j + 1][0] - points[j][0])
                for j in range(len(points) - 1)
            )
            avg_mof_ps.append(area / ws)

        return avg_mof_ps

    def evaluate(self, command: Command, context: StrategyContext) -> Command:
        mof_ps = context.extra.get("mof_ps")
        if not isinstance(mof_ps, list) or len(mof_ps) == 0:
            self._is_triggered = False
            return command

        if not all(isinstance(p, (tuple, list)) and len(p) == 2 for p in mof_ps):
            raise TypeError("mof_ps must be a list of (timestamp, power)")

        avg_mof_ps = self._resample_windows(mof_ps)
