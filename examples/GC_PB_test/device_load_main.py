import asyncio

from sim import SIM_HOST, SIM_PORT
from device import load_read_points

from csp_lib.core import get_logger
from csp_lib.redis import RedisClient, RedisConfig
from csp_lib.mongo import create_mongo_client, MongoConfig
from csp_lib.mongo.config import UploaderConfig
from csp_lib.mongo.uploader import MongoBatchUploader
from csp_lib.equipment.device import AsyncModbusDevice, DeviceConfig
from csp_lib.manager import UnifiedConfig, UnifiedDeviceManager
from csp_lib.manager.command import MongoCommandRepository
from csp_lib.modbus import ModbusTcpConfig, PymodbusTcpClient
from csp_lib.integration.distributed import RemoteSiteConfig, RemoteSiteRunner

logger = get_logger("DEV_LOAD")

DEVICE_ID = "load_01"
SITE_ID = "site_load"
UNIT_ID = 40
TRAIT = "load"


async def main() -> None:
    device = AsyncModbusDevice(
        config=DeviceConfig(device_id=DEVICE_ID, unit_id=UNIT_ID, read_interval=1),
        client=PymodbusTcpClient(ModbusTcpConfig(host=SIM_HOST, port=SIM_PORT)),
        always_points=load_read_points,
    )

    mongo_db = create_mongo_client(MongoConfig(host="localhost", port=27017))["demo"]
    mongo_uploader = MongoBatchUploader(mongo_db, UploaderConfig(
        flush_interval=1,
        batch_size_threshold=100,
        max_queue_size=10000,
        max_retry_count=3,
    ))

    redis_client = RedisClient.from_config(RedisConfig(host="localhost", port=6379))
    await redis_client.connect()

    manager = UnifiedDeviceManager(UnifiedConfig(
        batch_uploader=mongo_uploader,
        redis_client=redis_client,
        command_repository=MongoCommandRepository(mongo_db, "commands"),
    ))
    manager.register(device, TRAIT)

    runner = RemoteSiteRunner(
        config=RemoteSiteConfig(site_id=SITE_ID, device_ids=[DEVICE_ID]),
        unified_manager=manager,
        redis_client=redis_client,
    )

    try:
        async with device:
            mongo_uploader.start()
            async with runner:
                logger.info(f"[{SITE_ID}] running")
                await asyncio.Event().wait()
    finally:
        await redis_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
