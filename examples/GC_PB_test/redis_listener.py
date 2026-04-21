import asyncio
import json
from datetime import datetime
from typing import Any

from csp_lib.core import get_logger
from csp_lib.redis import RedisClient
from csp_lib.integration import SystemController

from strategy import PQ_ramp_Time_ModeConfig

logger = get_logger(__name__)

CHANNEL = "gc:command"


class EMSCommandListener:
    """Listens for EMS commands on Redis pub/sub and applies them to SystemController."""

    def __init__(
        self,
        redis_client: RedisClient,
        controller: SystemController,
        ramp_mode_name: str = "pq_mode",
        channel: str = CHANNEL,
    ) -> None:
        self._client = redis_client
        self._controller = controller
        self._ramp_mode_name = ramp_mode_name
        self._channel = channel
        self._listen_task: asyncio.Task[None] | None = None
        self._pubsub: Any | None = None

    async def start(self) -> None:
        """Subscribe and begin listening."""
        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(self._channel)
        logger.info("EMSCommandListener subscribed to channel: {}", self._channel)
        self._listen_task = asyncio.create_task(self._listen())

    async def _listen(self) -> None:
        """Main listen loop (runs as background task)."""
        try:
            async for message in self._pubsub.listen():
                if message["type"] != "message":
                    continue
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await self._handle(data)
        except asyncio.CancelledError:
            return

    async def _handle(self, raw: str) -> None:
        """Parse JSON and dispatch to controller."""
        try:
            cmd = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON from EMS: {}", raw)
            return

        action = cmd.get("action")
        try:
            if action == "set_mode":
                mode = cmd["mode"]
                await self._controller.set_base_mode(mode)
                logger.info("Mode switched to: {}", mode)

            elif action == "set_pq":
                p_start = float(cmd["p_start"])
                p_end = float(cmd["p_end"])
                q = float(cmd["q"])
                ramp_p = float(cmd["ramp_p"])
                end_time = datetime.fromisoformat(cmd["end_time"])
                mm = getattr(self._controller, "mode_manager", None)
                if mm is None:
                    mm = self._controller.system_controller.mode_manager
                strategy = mm.registered_modes[self._ramp_mode_name].strategy
                strategy.update_config(PQ_ramp_Time_ModeConfig(
                    p_start=p_start,
                    p_end=p_end,
                    q=q,
                    ramp_p=ramp_p,
                    end_time=end_time,
                ))
                logger.info(
                    "Ramp updated on mode '{}': p={}->{}, q={}, ramp_p={}, end_time={}",
                    self._ramp_mode_name, p_start, p_end, q, ramp_p, end_time.isoformat(),
                )

            elif action == "push_override":
                await self._controller.push_override(cmd["mode"])
                logger.info("Override pushed: {}", cmd["mode"])

            elif action == "pop_override":
                await self._controller.pop_override(cmd["mode"])
                logger.info("Override popped: {}", cmd["mode"])

            else:
                logger.warning("Unknown EMS action: {}", action)

        except KeyError as e:
            logger.error("Missing field in EMS command: {}", e)
        except Exception as e:
            logger.error("Error handling EMS command: {}", e)

    async def stop(self) -> None:
        """Cancel listener and clean up pub/sub."""
        if self._listen_task is not None:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None
        if self._pubsub is not None:
            await self._pubsub.unsubscribe(self._channel)
            self._pubsub = None
