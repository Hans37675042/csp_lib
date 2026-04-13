"""EMS CLI — publish strategy commands to GC via Redis."""

import asyncio
import json
import sys
from datetime import datetime, timedelta

from csp_lib.redis import RedisClient

CHANNEL = "gc:command"


async def send_command(cmd: dict) -> None:
    client = RedisClient(host="localhost", port=6379)
    async with client:
        msg = json.dumps(cmd)
        count = await client.publish(CHANNEL, msg)
        print(f"Published to {count} subscriber(s): {msg}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python ems_cli.py set_mode stop")
        print("  python ems_cli.py set_mode pq_mode")
        print("  python ems_cli.py set_pq 100 50")
        print("  python ems_cli.py push_override stop")
        print("  python ems_cli.py pop_override stop")
        sys.exit(1)

    action = sys.argv[1]

    if action == "set_mode":
        cmd = {"action": "set_mode", "mode": sys.argv[2]}
    elif action == "set_pq":
        # CLI 維持簡便：set_pq <p> <q>
        # 內部換算為「5 秒後開始、5 秒內爬完」的 PQ_ramp_Time 指令
        # 固定使用 ramp_p + end_time 格式，end_time 送絕對時間
        p_target = float(sys.argv[2])
        q_target = float(sys.argv[3])
        p_start = 0.0           # 假設由 stop 模式開始，起點為 0
        ramp_duration = 5.0     # 爬升耗時 (s)
        delay = 5.0             # 開始前延遲 (s)
        end_time = datetime.now() + timedelta(seconds=delay + ramp_duration)
        p_diff = abs(p_target - p_start)
        if p_diff == 0:
            print("set_pq 需要非零 p_end（p_start 假設為 0）。若要停機請使用 set_mode stop。")
            sys.exit(1)
        cmd = {
            "action": "set_pq",
            "p_start": p_start,
            "p_end": p_target,
            "q": q_target,
            "ramp_p": p_diff / ramp_duration,
            "end_time": end_time.isoformat(),
        }
    elif action in ("push_override", "pop_override"):
        cmd = {"action": action, "mode": sys.argv[2]}
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)

    asyncio.run(send_command(cmd))


if __name__ == "__main__":
    main()
