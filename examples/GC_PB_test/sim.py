import asyncio

from csp_lib.controller.core import StrategyContext, SystemBase
from csp_lib.controller.executor import StrategyExecutor
from csp_lib.controller.strategies import (
    FPConfig,
    FPStrategy,
    PQModeConfig,
    PQModeStrategy,
    QVConfig,
    QVStrategy,
)
from csp_lib.equipment.core import ReadPoint, WritePoint
from csp_lib.equipment.core.point import PointMetadata, RangeValidator
from csp_lib.equipment.device import AsyncModbusDevice, DeviceConfig
from csp_lib.modbus import Float32, ModbusTcpConfig, PymodbusTcpClient
from csp_lib.modbus_server import (
    PCSSimulator,
    PowerMeterSimulator,
    ServerConfig,
    SimulationServer,
)
from csp_lib.modbus_server.simulator.pcs import default_pcs_config
from csp_lib.modbus_server.simulator.power_meter import default_meter_config

# ============================================================
# 模擬伺服器設定
# ============================================================
SIM_HOST, SIM_PORT = "127.0.0.1", 5020


def create_sim() -> SimulationServer:
    """建立模擬伺服器：1 台 PCS + 1 台電表"""
    server = SimulationServer(ServerConfig(host=SIM_HOST, port=SIM_PORT, tick_interval=1.0))

    # PCS 模擬器
    pcs = PCSSimulator(
        config=default_pcs_config("pcs_01", unit_id=10),
        capacity_kwh=200.0,
        p_ramp_rate=50.0,
    )
    pcs.set_value("soc", 70.0)
    pcs.set_value("operating_mode", 1)
    pcs._running = True

    # 電表模擬器
    meter = PowerMeterSimulator(
        config=default_meter_config("meter_01", unit_id=1),
        voltage_noise=1.5,
        frequency_noise=0.01,
    )
    meter.set_system_reading(v=380.0, f=60.0, p=20.0, q=5.0)

    server.add_simulator(pcs)
    server.add_simulator(meter)
    return server