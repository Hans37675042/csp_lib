import asyncio
from datetime import datetime, timedelta

from strategy import PQ_ramp_Time_ModeConfig, PQ_ramp_Time_ModeStrategy
from redis_listener import EMSCommandListener

from csp_lib.core import get_logger
from csp_lib.redis import RedisClient, RedisConfig
from csp_lib.integration import (
    DeviceRegistry,
    SystemController,
    SystemControllerConfig,
)
from csp_lib.controller.strategies import StopStrategy
from csp_lib.controller.system import (
    DynamicSOCProtection,
    ModePriority,
    SOCProtectionConfig,
)
from csp_lib.integration.distributed import (
    DistributedConfig,
    DistributedController,
    RemoteSiteConfig,
)

logger = get_logger("STRAT")

SITE_OF = {
    "pcs_01": "site_pcs",
    "acm_01": "site_acm",
    "bms_01": "site_bms",
    "solar_01": "site_solar",
    "load_01": "site_load",
}


async def main() -> None:
    redis_client = RedisClient.from_config(RedisConfig(host="localhost", port=6379))
    await redis_client.connect()

    controller_config = (
        SystemControllerConfig.builder()
        .map_context(point_name="soc", target="soc", device_id="bms_01")
        .map_context(point_name="q_actual", target="extra.pcs_q", device_id="pcs_01")
        .map_command(field="p_target", point_name="p_setpoint", device_id="pcs_01")
        .map_command(field="q_target", point_name="q_setpoint", device_id="pcs_01")
        .map_context(point_name="active_power", target="extra.meter_power", device_id="acm_01")
        .map_context(point_name="frequency", target="extra.frequency", device_id="acm_01")
        .map_context(point_name="voltage_a", target="extra.voltage", device_id="acm_01")
        .map_context(point_name="ac_power", target="extra.solar_power", device_id="solar_01")
        .map_context(point_name="p_actual", target="extra.load_power", device_id="load_01")
        .protect(DynamicSOCProtection(SOCProtectionConfig(
            soc_high=95.0,
            soc_low=5.0,
            warning_band=5.0,
        )))
        .auto_stop(enabled=True)
        .build()
    )

    sys_controller = SystemController(DeviceRegistry(), controller_config)

    distributed_config = DistributedConfig(
        sites=[RemoteSiteConfig(site_id=s, device_ids=[d]) for d, s in SITE_OF.items()],
        trait_device_map={
            "pcs": ["pcs_01"],
            "meter": ["acm_01"],
            "bms": ["bms_01"],
            "solar": ["solar_01"],
            "load": ["load_01"],
        },
        poll_interval=1.0,
        system_alarm_on_device_offline=True,
    )
    dist_controller = DistributedController(distributed_config, sys_controller, redis_client)

    now = datetime.now()
    dist_controller.register_mode("stop", StopStrategy(), ModePriority.SCHEDULE, "停止模式")
    dist_controller.register_mode(
        "pq_mode",
        PQ_ramp_Time_ModeStrategy(PQ_ramp_Time_ModeConfig(
            p_start=0.0,
            p_end=0.0,
            q=0.0,
            start_time=now,
            end_time=now + timedelta(seconds=1),
        )),
        ModePriority.MANUAL,
        "時間 ramp PQ 模式",
    )

    await dist_controller.set_base_mode("stop")

    ems_listener = EMSCommandListener(
        redis_client=redis_client,
        controller=dist_controller,
        ramp_mode_name="pq_mode",
    )
    await ems_listener.start()

    try:
        async with dist_controller:
            logger.info("Strategy running")
            await asyncio.Event().wait()
    finally:
        await ems_listener.stop()
        await redis_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
