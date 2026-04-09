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
import asyncio
from sim import *
from device import *



from csp_lib.equipment.alarm import (
    AlarmDefinition,
    AlarmLevel,
    Operator,
    ThresholdAlarmEvaluator,
    ThresholdCondition,
)
from csp_lib.equipment.core import ReadPoint, WritePoint
from csp_lib.equipment.core.point import PointMetadata, RangeValidator
from csp_lib.equipment.device import AsyncModbusDevice, DeviceConfig
from csp_lib.integration import (
    DeviceRegistry,
    SystemController,
    SystemControllerConfig,
)
from csp_lib.controller.strategies import (
    StopStrategy,
    PQModeConfig,
    PQModeStrategy,
)
from csp_lib.controller.system import (
    DynamicSOCProtection,
    ModePriority,
    SOCProtectionConfig,
)
from csp_lib.modbus import Float32, ModbusTcpConfig, PymodbusTcpClient, UInt16
from csp_lib.modbus_server import PCSSimulator, ServerConfig, SimulationServer
from csp_lib.modbus_server.simulator.pcs import default_pcs_config

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
    async with sim_server:
        print("模擬伺服器已啟動，正在運行中...")
        async with pcs_device, acm_device:
            await controller.set_base_mode("stop")
            async with controller:
                while True:
                    await asyncio.sleep(1)
        

if __name__ == "__main__":
    asyncio.run(main())
