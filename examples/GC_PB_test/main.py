'''
完整GC實作
實體化:
    設備
    資料上傳器
    系統控制器

加入
    所有設備(電表、INVERTER、儲能)
    策略(可能要自訂)

研究
    EMS控制如何串接(透過redis)    
    
'''
import asyncio, time
from sim import *
from device import *
from redis_listener import EMSCommandListener


from csp_lib.core import get_logger
from csp_lib.redis import RedisClient
from motor.motor_asyncio import AsyncIOMotorClient
from csp_lib.mongo.config import UploaderConfig
from csp_lib.mongo.uploader import MongoBatchUploader

from csp_lib.equipment.device import AsyncModbusDevice, DeviceConfig
from csp_lib.integration import (
    DeviceRegistry,
    SystemController,
    SystemControllerConfig,
)
from csp_lib.manager import (
    UnifiedConfig,
    UnifiedDeviceManager,
)
from csp_lib.controller.strategies import (
    StopStrategy,
    PQModeConfig,
    PQModeStrategy,
)
from csp_lib.controller.system import (
    ModePriority,
)
from csp_lib.modbus import ModbusTcpConfig, PymodbusTcpClient

logger = get_logger("MAIN")

pcs_config = DeviceConfig(
    device_id="pcs_01",
    unit_id=10,
    read_interval=1
)
pcs_client = PymodbusTcpClient(ModbusTcpConfig(host=SIM_HOST, port=SIM_PORT))
pcs_device = AsyncModbusDevice(
    config=pcs_config,
    client=pcs_client,
    always_points=pcs_read_points,
    write_points=pcs_write_points,
)

acm_config = DeviceConfig(
    device_id="acm_01",
    unit_id=1,
    read_interval=1
)
acm_clent = PymodbusTcpClient(ModbusTcpConfig(host=SIM_HOST, port=SIM_PORT))
acm_device = AsyncModbusDevice(
    config=acm_config,
    client=acm_clent,
    always_points=meter_read_points,
)

registry = DeviceRegistry()
registry.register(pcs_device, traits=["pcs"])
registry.register(acm_device, traits=["meter"])

#=============================================================
mongo = AsyncIOMotorClient("mongodb://localhost:27017")["demo"]
mongo_config = UploaderConfig(
    flush_interval=5.0,
    batch_size_threshold=100,
    max_queue_size=10000,
    max_retry_count=3,
)
mongo_uploader = MongoBatchUploader(mongo, mongo_config)
unified_config = UnifiedConfig(
    batch_uploader=mongo_uploader,  # BatchUploader Protocol — 可以傳入任何實作
)
manager = UnifiedDeviceManager(unified_config)
manager.register(pcs_device, "pcs")
#=============================================================
controller_config = (
    SystemControllerConfig.builder()
    .map_context(point_name="soc", target="soc", device_id="pcs_01")
    .map_command(field="p_target", point_name="p_setpoint", device_id="pcs_01")
    .map_command(field="q_target", point_name="q_setpoint", device_id="pcs_01")
    
    .map_context(point_name="active_power", target="extra.meter_power", device_id="acm_01")
    .map_context(point_name="frequency", target="extra.frequency", device_id="acm_01")
    .map_context(point_name="voltage_a", target="extra.voltage", device_id="acm_01")
    .auto_stop(enabled=True)
    .build()
)
controller = SystemController(registry, controller_config)

stop_strategy = StopStrategy()
pq_strategy = PQModeStrategy(PQModeConfig(p=0, q=0))
controller.register_mode("stop", stop_strategy, ModePriority.SCHEDULE, "停止模式")
controller.register_mode("pq_mode", pq_strategy, ModePriority.MANUAL, "固定PQ模式")



async def main() -> None:
    sim_server = create_sim()
    start_time = time.monotonic()
    async with sim_server:
        logger.info("模擬伺服器已啟動，正在運行中...")
        async with pcs_device, acm_device:
            async with manager:
                await controller.set_base_mode("stop")

                redis_client = RedisClient(host="localhost", port=6379)
                await redis_client.connect()
                ems_listener = EMSCommandListener(
                    redis_client=redis_client,
                    controller=controller,
                    pq_strategy=pq_strategy,
                )
                await ems_listener.start()

                async with controller:
                        while True:
                            logger.info("*"*60)
                            now_time = time.monotonic()
                            start_time += 1
                            sleep_time = start_time - now_time
                            await asyncio.sleep(max(0, sleep_time))
    await ems_listener.stop()
        

if __name__ == "__main__":
    asyncio.run(main())
