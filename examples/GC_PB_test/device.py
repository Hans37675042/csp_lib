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






# PCS 讀取點位
pcs_read_points = [
    ReadPoint(name="p_actual", address=4, data_type=Float32(), metadata=PointMetadata(unit="kW")),
    ReadPoint(name="q_actual", address=6, data_type=Float32(), metadata=PointMetadata(unit="kVar")),
    ReadPoint(name="soc", address=8, data_type=Float32(), metadata=PointMetadata(unit="%")),
]

# PCS 寫入點位
pcs_write_points = [
    WritePoint(name="p_setpoint", address=0, data_type=Float32(), validator=RangeValidator(min_value=-200.0, max_value=200.0)),
    WritePoint(name="q_setpoint", address=2, data_type=Float32(), validator=RangeValidator(min_value=-100.0, max_value=100.0)),
]

# 電表讀取點位
meter_read_points = [
    ReadPoint(name="voltage_a", address=0, data_type=Float32(), metadata=PointMetadata(unit="V")),
    ReadPoint(name="active_power", address=12, data_type=Float32(), metadata=PointMetadata(unit="kW")),
    ReadPoint(name="reactive_power", address=14, data_type=Float32(), metadata=PointMetadata(unit="kVar")),
    ReadPoint(name="frequency", address=20, data_type=Float32(), metadata=PointMetadata(unit="Hz")),
]

class PCS_device(AsyncModbusDevice):
    None