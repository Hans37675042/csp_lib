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
from csp_lib.modbus import UInt16, Float32, ModbusTcpConfig, PymodbusTcpClient
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
    ReadPoint(name="operating_mode", address=10, data_type=UInt16(), metadata=PointMetadata(unit="")),
    ReadPoint(name="alarm_register_1", address=11, data_type=UInt16(), metadata=PointMetadata(unit="")),
    ReadPoint(name="alarm_register_2", address=12, data_type=UInt16(), metadata=PointMetadata(unit="")),
    ReadPoint(name="voltage", address=15, data_type=Float32(), metadata=PointMetadata(unit="V")),
    ReadPoint(name="frequency", address=17, data_type=Float32(), metadata=PointMetadata(unit="Hz")),
]

# PCS 寫入點位
pcs_write_points = [
    WritePoint(name="p_setpoint", address=0, data_type=Float32(), validator=RangeValidator(min_value=-200.0, max_value=200.0)),
    WritePoint(name="q_setpoint", address=2, data_type=Float32(), validator=RangeValidator(min_value=-100.0, max_value=100.0)),
    WritePoint(name="alarm_reset_cmd", address=13, data_type=UInt16()),
    WritePoint(name="start_cmd", address=14, data_type=UInt16()),
]

# 電表讀取點位
meter_read_points = [
    ReadPoint(name="voltage_a", address=0, data_type=Float32(), metadata=PointMetadata(unit="V")),
    ReadPoint(name="voltage_b", address=2, data_type=Float32(), metadata=PointMetadata(unit="V")),
    ReadPoint(name="voltage_c", address=4, data_type=Float32(), metadata=PointMetadata(unit="V")),
    ReadPoint(name="current_a", address=6, data_type=Float32(), metadata=PointMetadata(unit="A")),
    ReadPoint(name="current_b", address=8, data_type=Float32(), metadata=PointMetadata(unit="A")),
    ReadPoint(name="current_c", address=10, data_type=Float32(), metadata=PointMetadata(unit="A")),
    ReadPoint(name="active_power", address=12, data_type=Float32(), metadata=PointMetadata(unit="kW")),
    ReadPoint(name="reactive_power", address=14, data_type=Float32(), metadata=PointMetadata(unit="kVar")),
    ReadPoint(name="apparent_power", address=16, data_type=Float32(), metadata=PointMetadata(unit="kVA")),
    ReadPoint(name="power_factor", address=18, data_type=Float32(), metadata=PointMetadata(unit="")),
    ReadPoint(name="frequency", address=20, data_type=Float32(), metadata=PointMetadata(unit="Hz")),
    ReadPoint(name="energy_total", address=22, data_type=Float32(), metadata=PointMetadata(unit="kWh")),
    ReadPoint(name="status", address=24, data_type=UInt16(), metadata=PointMetadata(unit="")),
]

class PCS_device(AsyncModbusDevice):
    None