import asyncio

from csp_lib.modbus_server import (
    ControllabilityMode,
    DeviceLinkConfig,
    MicrogridConfig,
    MicrogridSimulator,
    PCSSimConfig,
    BMSSimConfig,
    SolarSimConfig,
    LoadSimConfig,
    PowerMeterSimConfig,
    ServerConfig,
    SimulationServer,
)
from csp_lib.modbus_server.simulator.bms import BMSSimulator, default_bms_config
from csp_lib.modbus_server.simulator.load import LoadSimulator, default_load_config
from csp_lib.modbus_server.simulator.pcs import PCSSimulator, default_pcs_config
from csp_lib.modbus_server.simulator.power_meter import PowerMeterSimulator, default_meter_config
from csp_lib.modbus_server.simulator.solar import SolarSimulator, default_solar_config

# ============================================================
# 模擬伺服器設定
# ============================================================
SIM_HOST, SIM_PORT = "127.0.0.1", 5020


def create_sim() -> SimulationServer:
    """建立模擬伺服器：PCS + BMS + Solar + Load + Meter (完整微電網)"""
    server = SimulationServer(ServerConfig(host=SIM_HOST, port=SIM_PORT, tick_interval=1.0))

    mg = MicrogridSimulator(MicrogridConfig(
        grid_voltage=380.0,
        grid_frequency=60.0,
        voltage_noise=1.5,
        frequency_noise=0.01,
    ))

    # PCS: 200kW 儲能變流器
    pcs = PCSSimulator(
        config=default_pcs_config("pcs_01", unit_id=10),
        sim_config=PCSSimConfig(
            capacity_kwh=200.0,
            p_ramp_rate=50.0,
            q_ramp_rate=50.0,
            tick_interval=1.0,
        ),
    )
    pcs.on_write("start_cmd", 0, 1)
    mg.add_pcs(pcs)

    # BMS: 200kWh 電池管理系統
    bms = BMSSimulator(
        config=default_bms_config("bms_01", unit_id=20),
        sim_config=BMSSimConfig(
            capacity_kwh=200.0,
            initial_soc=70.0,
            nominal_voltage=700.0,
            cells_in_series=192,
            charge_efficiency=0.95,
        ),
    )
    mg.add_bms(bms)
    mg.link_pcs_bms("pcs_01", "bms_01")

    # Solar: 太陽能模擬器
    solar = SolarSimulator(
        config=default_solar_config("solar_01", unit_id=30),
        sim_config=SolarSimConfig(
            efficiency=0.95,
            power_noise=0.0,
        ),
    )
    solar.set_target_power(50.0)
    mg.add_solar(solar)

    # Load: 不可控負載
    load = LoadSimulator(
        config=default_load_config("load_01", unit_id=40, controllable=False),
        sim_config=LoadSimConfig(
            controllability=ControllabilityMode.UNCONTROLLABLE,
            power_factor=0.9,
            ramp_rate=50.0,
            base_load=80.0,
            load_noise=0.0,
        ),
    )
    mg.add_load(load)

    # Meter: 市電側電表
    meter = PowerMeterSimulator(
        config=default_meter_config("meter_01", unit_id=1),
        sim_config=PowerMeterSimConfig(
            power_sign=1.0,
            voltage_noise=1.5,
            frequency_noise=0.01,
        ),
    )
    mg.set_meter(meter)

    # Device links: PCS/Solar/Load → Meter
    mg.add_device_link(DeviceLinkConfig(source_device_id="pcs_01", target_meter_id="meter_01", loss_factor=0.02))
    mg.add_device_link(DeviceLinkConfig(source_device_id="solar_01", target_meter_id="meter_01"))
    mg.add_device_link(DeviceLinkConfig(source_device_id="load_01", target_meter_id="meter_01"))

    server.set_microgrid(mg)
    return server
