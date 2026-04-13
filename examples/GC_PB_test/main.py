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
from sim import create_sim, SIM_HOST, SIM_PORT
from device import (
    pcs_read_points, pcs_write_points,
    meter_read_points,
    bms_read_points,
    solar_read_points,
    load_read_points,
)
from strategy import PQ_ramp_Time_ModeConfig, PQ_ramp_Time_ModeStrategy
from redis_listener import EMSCommandListener


from csp_lib.core import get_logger
from csp_lib.redis import RedisClient
from motor.motor_asyncio import AsyncIOMotorClient
from csp_lib.mongo import create_mongo_client, MongoConfig
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
acm_client = PymodbusTcpClient(ModbusTcpConfig(host=SIM_HOST, port=SIM_PORT))
acm_device = AsyncModbusDevice(
    config=acm_config,
    client=acm_client,
    always_points=meter_read_points,
)

bms_config = DeviceConfig(device_id="bms_01", unit_id=20, read_interval=1)
bms_client = PymodbusTcpClient(ModbusTcpConfig(host=SIM_HOST, port=SIM_PORT))
bms_device = AsyncModbusDevice(
    config=bms_config,
    client=bms_client,
    always_points=bms_read_points,
)

solar_config = DeviceConfig(device_id="solar_01", unit_id=30, read_interval=1)
solar_client = PymodbusTcpClient(ModbusTcpConfig(host=SIM_HOST, port=SIM_PORT))
solar_device = AsyncModbusDevice(
    config=solar_config,
    client=solar_client,
    always_points=solar_read_points,
)

load_config = DeviceConfig(device_id="load_01", unit_id=40, read_interval=1)
load_client = PymodbusTcpClient(ModbusTcpConfig(host=SIM_HOST, port=SIM_PORT))
load_device = AsyncModbusDevice(
    config=load_config,
    client=load_client,
    always_points=load_read_points,
)

registry = DeviceRegistry()
registry.register(pcs_device, traits=["pcs"])
registry.register(acm_device, traits=["meter"])
registry.register(bms_device, traits=["bms"])
registry.register(solar_device, traits=["solar"])
registry.register(load_device, traits=["load"])

#=============================================================

MONGO_CONFIG = MongoConfig(
    host="localhost",       # MongoDB 主機位址
    port=27017,             # MongoDB 連接埠
    # username="your_user", # 若有帳號驗證請取消註解
    # password="your_pass",
    # auth_source="admin",
)
MONGO_DATABASE = "demo"
mongo_db = create_mongo_client(MONGO_CONFIG)[MONGO_DATABASE]
mongo_config = UploaderConfig(
    flush_interval=1,
    batch_size_threshold=100,
    max_queue_size=10000,
    max_retry_count=3,
)
mongo_uploader = MongoBatchUploader(mongo_db, mongo_config)
unified_config = UnifiedConfig(
    batch_uploader=mongo_uploader,  # BatchUploader Protocol — 可以傳入任何實作
)
manager = UnifiedDeviceManager(unified_config)
manager.register(pcs_device, "pcs")
manager.register(acm_device, "acm")
manager.register(bms_device, "bms")
manager.register(solar_device, "solar")
manager.register(load_device, "load")
#=============================================================
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
    .auto_stop(enabled=True)
    .build()
)
controller = SystemController(registry, controller_config)

stop_strategy = StopStrategy()

# PQ_ramp_Time_ModeStrategy 初始以 placeholder config 建立，
# 實際參數由 EMSCommandListener 透過 update_config() 注入。
_ramp_placeholder_now = datetime.now()
ramp_strategy = PQ_ramp_Time_ModeStrategy(
    PQ_ramp_Time_ModeConfig(
        p_start=0.0,
        p_end=0.0,
        q=0.0,
        start_time=_ramp_placeholder_now,
        end_time=_ramp_placeholder_now + timedelta(seconds=1),
    )
)
controller.register_mode("stop", stop_strategy, ModePriority.SCHEDULE, "停止模式")
controller.register_mode("pq_mode", ramp_strategy, ModePriority.MANUAL, "時間 ramp PQ 模式")



async def main() -> None:
    sim_server = create_sim()
    start_time = time.monotonic()
    async with sim_server:
        logger.info("模擬伺服器已啟動，正在運行中...")
        async with pcs_device, acm_device, bms_device, solar_device, load_device:
            mongo_uploader.start()
            async with manager:
                await controller.set_base_mode("stop")

                redis_client = RedisClient(host="localhost", port=6379)
                await redis_client.connect()
                ems_listener = EMSCommandListener(
                    redis_client=redis_client,
                    controller=controller,
                    ramp_strategy=ramp_strategy,
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
