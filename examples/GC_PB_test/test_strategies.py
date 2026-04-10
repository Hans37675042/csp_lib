"""
GC_PB_test — 三個自訂策略的情境測試

測試範圍：
  - PQ_ramp_ModeStrategy
  - PQ_ramp_Time_ModeStrategy
  - PQ_SOC_ModeStrategy

設計：
  直接建立 StrategyContext（不經過真實 device），
  同步呼叫 strategy.execute() 驗證 Command 輸出。
  使用 assert 做斷言，失敗時 raise AssertionError。

Run:
  uv run python examples/GC_PB_test/test_strategies.py
"""

from datetime import datetime, timedelta

from csp_lib.controller.core import StrategyContext
from strategy import (
    PQ_ramp_ModeConfig,
    PQ_ramp_ModeStrategy,
    PQ_ramp_Time_ModeConfig,
    PQ_ramp_Time_ModeStrategy,
    PQ_SOC_ModeConfig,
    PQ_SOC_ModeStrategy,
)


# ---------- helpers ----------
def make_ctx(pcs_p: float | None = None,
             pcs_q: float | None = None,
             soc: float | None = None,
             current_time: datetime | None = None) -> StrategyContext:
    extra: dict = {}
    if pcs_p is not None:
        extra["pcs_p"] = pcs_p
    if pcs_q is not None:
        extra["pcs_q"] = pcs_q
    if soc is not None:
        extra["soc"] = soc
    return StrategyContext(soc=soc, extra=extra, current_time=current_time)


def approx(a: float, b: float, tol: float = 1e-6) -> bool:
    return abs(a - b) <= tol


def section(title: str) -> None:
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def check(label: str, cond: bool) -> None:
    status = "OK" if cond else "FAIL"
    print(f"  [{status}] {label}")
    assert cond, f"FAILED: {label}"


# ---------- 1. PQ_ramp_ModeStrategy ----------
def test_pq_ramp() -> None:
    section("1. PQ_ramp_ModeStrategy")

    strat = PQ_ramp_ModeStrategy(
        PQ_ramp_ModeConfig(p=100, q=50, ramp_p=10, ramp_q=5)
    )

    # 1a 正向 ramp: pcs_p=0, ramp=10 → 10 ; pcs_q=0, ramp=5 → 5
    cmd = strat.execute(make_ctx(pcs_p=0, pcs_q=0))
    check("正向 ramp: p_target=10", approx(cmd.p_target, 10))
    check("正向 ramp: q_target=5", approx(cmd.q_target, 5))

    # 1b 反向 ramp
    strat.update_config(PQ_ramp_ModeConfig(p=0, q=0, ramp_p=10, ramp_q=5))
    cmd = strat.execute(make_ctx(pcs_p=100, pcs_q=50))
    check("反向 ramp: p_target=90", approx(cmd.p_target, 90))
    check("反向 ramp: q_target=45", approx(cmd.q_target, 45))

    # 1c 差距 < ramp：直達
    strat.update_config(PQ_ramp_ModeConfig(p=100, q=50, ramp_p=10, ramp_q=5))
    cmd = strat.execute(make_ctx(pcs_p=95, pcs_q=48))
    check("差距<ramp: p 直達 100", approx(cmd.p_target, 100))
    check("差距<ramp: q 直達 50", approx(cmd.q_target, 50))

    # 1d ramp=0：直達
    strat.update_config(PQ_ramp_ModeConfig(p=50, q=0, ramp_p=0, ramp_q=0))
    cmd = strat.execute(make_ctx(pcs_p=0, pcs_q=0))
    check("ramp=0: p 直達 50", approx(cmd.p_target, 50))
    check("ramp=0: q 直達 0", approx(cmd.q_target, 0))

    # 1e 缺 PCS 資料 → ValueError（同一實例）
    try:
        strat.execute(make_ctx())
        check("缺 PCS 資料應 raise ValueError", False)
    except ValueError:
        check("缺 PCS 資料應 raise ValueError", True)


# ---------- 2. PQ_ramp_Time_ModeStrategy ----------
def test_pq_ramp_time() -> None:
    section("2. PQ_ramp_Time_ModeStrategy")

    base = datetime(2030, 1, 1, 0, 0, 0)

    strat = PQ_ramp_Time_ModeStrategy(
        PQ_ramp_Time_ModeConfig(
            p_start=0, p_end=100, q=10,
            start_time=base, end_time=base + timedelta(seconds=10),
            ramp_q=5,
        )
    )

    # 2a 雙時間反推 ramp
    check("雙時間: duration=10", approx(strat.duration, 10))
    check("雙時間: ramp_p=10", approx(strat.ramp_p, 10))

    # 2b 單 start_time + ramp_p → duration
    strat.update_config(PQ_ramp_Time_ModeConfig(
        p_start=0, p_end=50, q=0, start_time=base, ramp_p=5,
    ))
    check("單 start+ramp: duration=10", approx(strat.duration, 10))
    check(
        "單 start+ramp: time_end - time_start = 10",
        approx(strat.time_end - strat.time_start, 10),
    )

    # 2c 單 end_time + seconds → ramp
    strat.update_config(PQ_ramp_Time_ModeConfig(
        p_start=0, p_end=50, q=0,
        end_time=base + timedelta(seconds=20), seconds=20,
    ))
    check("單 end+seconds: duration=20", approx(strat.duration, 20))
    check("單 end+seconds: ramp_p=2.5", approx(strat.ramp_p, 2.5))

    # 2d 爬升期間多點取樣：0→100 / 10s, q=10 ramp_q=0 直達
    strat.update_config(PQ_ramp_Time_ModeConfig(
        p_start=0, p_end=100, q=10, start_time=base, seconds=10, ramp_q=0,
    ))
    for dt, expected_p in [(0, 0), (2.5, 25), (5, 50), (7.5, 75), (10, 100), (15, 100)]:
        ctx = make_ctx(pcs_q=0, current_time=base + timedelta(seconds=dt))
        cmd = strat.execute(ctx)
        check(f"正向取樣 +{dt}s: p_target={expected_p}", approx(cmd.p_target, expected_p))
        check(f"正向取樣 +{dt}s: q_target=10", approx(cmd.q_target, 10))

    # 2e 時間範圍前：p_target = p_start
    strat.update_config(PQ_ramp_Time_ModeConfig(
        p_start=10, p_end=50, q=0,
        start_time=base + timedelta(seconds=1000), seconds=10,
    ))
    cmd = strat.execute(make_ctx(pcs_q=0, current_time=base))
    check("時間範圍前: p_target=10", approx(cmd.p_target, 10))
    check("時間範圍前: q_target=0", approx(cmd.q_target, 0))

    # 2f 時間範圍後：p_target = p_end
    strat.update_config(PQ_ramp_Time_ModeConfig(
        p_start=0, p_end=50, q=0,
        start_time=base - timedelta(seconds=1000), seconds=10,
    ))
    cmd = strat.execute(make_ctx(pcs_q=0, current_time=base))
    check("時間範圍後: p_target=50", approx(cmd.p_target, 50))
    check("時間範圍後: q_target=0", approx(cmd.q_target, 0))

    # 2g 反向 ramp 多點取樣：100→0 / 10s
    strat.update_config(PQ_ramp_Time_ModeConfig(
        p_start=100, p_end=0, q=0, start_time=base, seconds=10,
    ))
    for dt, expected_p in [(0, 100), (2.5, 75), (5, 50), (7.5, 25), (10, 0), (15, 0)]:
        ctx = make_ctx(pcs_q=0, current_time=base + timedelta(seconds=dt))
        cmd = strat.execute(ctx)
        check(f"反向取樣 +{dt}s: p_target={expected_p}", approx(cmd.p_target, expected_p))
        check(f"反向取樣 +{dt}s: q_target=0", approx(cmd.q_target, 0))

    # 2h 缺 current_time → ValueError
    strat.update_config(PQ_ramp_Time_ModeConfig(
        p_start=0, p_end=10, q=0, start_time=base, seconds=10,
    ))
    try:
        strat.execute(make_ctx(pcs_q=0))
        check("缺 current_time 應 raise ValueError", False)
    except ValueError:
        check("缺 current_time 應 raise ValueError", True)


# ---------- 3. PQ_SOC_ModeStrategy ----------
def test_pq_soc() -> None:
    section("3. PQ_SOC_ModeStrategy")

    strat = PQ_SOC_ModeStrategy(
        PQ_SOC_ModeConfig(soc_target=80, p=50, q=0, ramp_p=0, ramp_q=0)
    )

    # 3a 已充飽 (soc >= target) → p_target = 0
    cmd = strat.execute(make_ctx(pcs_p=30, pcs_q=0, soc=90))
    check("已充飽: p_target=0", approx(cmd.p_target, 0))
    check("已充飽: q_target=0", approx(cmd.q_target, 0))

    # 3b 未充飽 + 無時間排程 → 固定功率 p=50
    cmd = strat.execute(make_ctx(pcs_p=0, pcs_q=0, soc=50))
    check("固定功率: p_target=50", approx(cmd.p_target, 50))
    check("固定功率: q_target=0", approx(cmd.q_target, 0))

    # 3c 未充飽 + 時間排程
    #   capacity=200 / 1h / Δsoc=30% → 60 kW (> cfg.p=50, 確保走公式分支)
    base = datetime(2030, 1, 1, 0, 0, 0)
    strat.update_config(PQ_SOC_ModeConfig(
        soc_target=80, p=50, q=0, ramp_p=0, ramp_q=0,
        soc_time=base + timedelta(hours=1), capacity=200,
    ))
    cmd = strat.execute(make_ctx(pcs_p=0, pcs_q=0, soc=50, current_time=base))
    check("時間排程: p_target=60 (> cfg.p=50)", approx(cmd.p_target, 60))
    check("時間排程: q_target=0", approx(cmd.q_target, 0))

    # 3d 未充飽 + 過期時間 → p_target=0
    strat.update_config(PQ_SOC_ModeConfig(
        soc_target=80, p=50, q=0, ramp_p=0, ramp_q=0,
        soc_time=base - timedelta(hours=1), capacity=200,
    ))
    cmd = strat.execute(make_ctx(pcs_p=0, pcs_q=0, soc=50, current_time=base))
    check("過期時間: p_target=0", approx(cmd.p_target, 0))
    check("過期時間: q_target=0", approx(cmd.q_target, 0))

    # 3e 缺 soc → ValueError
    strat.update_config(PQ_SOC_ModeConfig(soc_target=80, p=0, q=0, ramp_p=0, ramp_q=0))
    try:
        strat.execute(make_ctx(pcs_p=0, pcs_q=0))
        check("缺 soc 應 raise ValueError", False)
    except ValueError:
        check("缺 soc 應 raise ValueError", True)

    # 3f 時間排程 + 缺 current_time → ValueError
    strat.update_config(PQ_SOC_ModeConfig(
        soc_target=80, p=0, q=0, ramp_p=0, ramp_q=0,
        soc_time=base + timedelta(hours=1), capacity=100,
    ))
    try:
        strat.execute(make_ctx(pcs_p=0, pcs_q=0, soc=50))
        check("缺 current_time 應 raise ValueError", False)
    except ValueError:
        check("缺 current_time 應 raise ValueError", True)


# ---------- 4. PQ_ramp_Time_ModeStrategy 參數錯誤 ----------
def test_config_errors() -> None:
    section("4. PQ_ramp_Time_ModeStrategy 參數錯誤")

    cases = [
        (
            "start_time 與 end_time 皆 None",
            lambda: PQ_ramp_Time_ModeStrategy(
                PQ_ramp_Time_ModeConfig(p_start=0, p_end=10)
            ),
        ),
        (
            "start_time > end_time",
            lambda: PQ_ramp_Time_ModeStrategy(
                PQ_ramp_Time_ModeConfig(
                    p_start=0,
                    p_end=10,
                    start_time=datetime(2030, 1, 2),
                    end_time=datetime(2030, 1, 1),
                )
            ),
        ),
        (
            "start_time == end_time",
            lambda: PQ_ramp_Time_ModeStrategy(
                PQ_ramp_Time_ModeConfig(
                    p_start=0,
                    p_end=10,
                    start_time=datetime(2030, 1, 1),
                    end_time=datetime(2030, 1, 1),
                )
            ),
        ),
        (
            "僅 start_time 但 ramp_p 與 seconds 皆 None",
            lambda: PQ_ramp_Time_ModeStrategy(
                PQ_ramp_Time_ModeConfig(
                    p_start=0,
                    p_end=10,
                    start_time=datetime(2030, 1, 1),
                )
            ),
        ),
        (
            "ramp_p == 0",
            lambda: PQ_ramp_Time_ModeStrategy(
                PQ_ramp_Time_ModeConfig(
                    p_start=0,
                    p_end=10,
                    start_time=datetime(2030, 1, 1),
                    ramp_p=0,
                )
            ),
        ),
        (
            "seconds <= 0",
            lambda: PQ_ramp_Time_ModeStrategy(
                PQ_ramp_Time_ModeConfig(
                    p_start=0,
                    p_end=10,
                    start_time=datetime(2030, 1, 1),
                    seconds=0,
                )
            ),
        ),
    ]
    for label, thunk in cases:
        try:
            thunk()
            check(f"{label} 應 raise ValueError", False)
        except ValueError:
            check(f"{label} 應 raise ValueError", True)


# ---------- entry ----------
if __name__ == "__main__":
    test_pq_ramp()
    test_pq_ramp_time()
    test_pq_soc()
    test_config_errors()
    print("\n" + "=" * 60)
    print("  所有測試通過")
    print("=" * 60)
