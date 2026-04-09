"""EMS CLI — publish strategy commands to GC via Redis."""

import asyncio
import json
import sys

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
        cmd = {"action": "set_pq", "p": float(sys.argv[2]), "q": float(sys.argv[3])}
    elif action in ("push_override", "pop_override"):
        cmd = {"action": action, "mode": sys.argv[2]}
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)

    asyncio.run(send_command(cmd))


if __name__ == "__main__":
    main()
