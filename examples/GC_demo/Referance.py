import asyncio
import threading
import signal
import atexit

from abc import ABC, abstractmethod
from asyncio import gather
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Any

from pymodbus import ExceptionResponse
from pymodbus.client import AsyncModbusTcpClient, ModbusBaseClient

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from csp_lib import logger
from csp_lib.grid_controller import PQMode, MDemand_LShiftMode, SiteControl, SystemLimit, SiteControlMode, DBFieldAdapter
from csp_lib.modbus import ModbusException
from csp_lib.modbus.modbus_datatype import IntegerData, FloatData, Endian
from csp_lib.modbus_equipment import Alarm, MultiPoint, SinglePoint, WritePoint, AlarmPoint, AlarmBitPoint

# from csp_lib.modbus_server import run_server_manager

# =================Command=================

@dataclass
class Command:
    p_target: float
    q_target: float

class Strategy(ABC):
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    try:
                        cls._instance = super().__new__(cls)
                        cls._instance._init(*args, **kwargs)
                    except Exception as e:
                        logger.exception(f"{cls.__name__} init failed: {e}")
        return cls._instance

    def _init(self, *args, **kwargs):
        self.parameter = None
        self.lock = threading.Lock()

    def set_parameter(self, parameter):
        with self.lock:
            self.parameter = parameter

    def get_parameter(self):
        with self.lock:
            return self.parameter

    @property
    @abstractmethod
    def interval(self) -> int:
        """返回策略的執行週期（秒）"""
        pass

    @abstractmethod
    def execute(self, last_command: Optional[Command]) -> Command:
        """執行策略的邏輯"""
        pass

    def __str__(self):
        return f"{self.__class__.__name__}(parameter={self.parameter})"

    def __repr__(self):
        return self.__str__()

class StopStrategy(Strategy):
    @property
    def interval(self) -> int:
        return 1

    def execute(self, last_command: Optional[Command]) -> Command:
        return Command(p_target=0, q_target=0)

class PQModeStrategy(Strategy):
    def _init(self):
        super()._init()
        self.parameter = PQMode(p=0, q=0)

    @property
    def interval(self) -> int:
        return 1

    def execute(self, last_command: Optional[Command]) -> Command:
        return Command(p_target=self.parameter.p,q_target=self.parameter.q)

class MDemand_LShiftStrategy(Strategy):
    def _init(self):
        super()._init()
        # 輸入參數 
        self.parameter = MDemand_LShiftMode(contract_capacity=0, PeakLoad_reserve=0, dead_zone_soc=0,
                day_0_priority_list=[],day_1_priority_list=[],day_2_priority_list=[],day_3_priority_list=[],day_4_priority_list=[],day_5_priority_list=[],day_6_priority_list=[],
                sum_0_dem_soc=0, sum_1_dem_soc=0, sum_2_dem_soc=0, sum_3_dem_soc=0, sum_4_dem_soc=0, sum_5_dem_soc=0, sum_6_dem_soc=0,
                not_sum_0_dem_soc=0, not_sum_1_dem_soc=0, not_sum_2_dem_soc=0, not_sum_3_dem_soc=0, not_sum_4_dem_soc=0, not_sum_5_dem_soc=0, not_sum_6_dem_soc=0,
                sum_0_peak_soc=0, sum_1_peak_soc=0, sum_2_peak_soc=0, sum_3_peak_soc=0, sum_4_peak_soc=0, sum_5_peak_soc=0, sum_6_peak_soc=0,
                not_sum_0_peak_soc=0, not_sum_1_peak_soc=0, not_sum_2_peak_soc=0, not_sum_3_peak_soc=0, not_sum_4_peak_soc=0, not_sum_5_peak_soc=0, not_sum_6_peak_soc=0,
                NationalHoliday=[], summer_period={},
                sum_0_peak_period=[], sum_1_peak_period=[], sum_2_peak_period=[], sum_3_peak_period=[], sum_4_peak_period=[], sum_5_peak_period=[], sum_6_peak_period=[],
                not_sum_0_peak_period=[], not_sum_1_peak_period=[], not_sum_2_peak_period=[], not_sum_3_peak_period=[], not_sum_4_peak_period=[], not_sum_5_peak_period=[], not_sum_6_peak_period=[],
                sum_0_half_peak_period=[], sum_1_half_peak_period=[], sum_2_half_peak_period=[], sum_3_half_peak_period=[], sum_4_half_peak_period=[], sum_5_half_peak_period=[], sum_6_half_peak_period=[],
                not_sum_0_half_peak_period=[], not_sum_1_half_peak_period=[], not_sum_2_half_peak_period=[], not_sum_3_half_peak_period=[], not_sum_4_half_peak_period=[], not_sum_5_half_peak_period=[], not_sum_6_half_peak_period=[])

        # 系統參數 
        self.soc = 0
        self.min_soc = 0
        self.max_soc = 0
        self.min_p = 0
        self.max_p = 0        
        self.bess_capacity = 0
        self.gc_interval = 0
        self.max_interval = 0
        self.current_load = 0
        self.period_switcher = {}
        self.soc_dem_switcher = {}
        self.soc_peak_switcher = {}
        self.priority_switcher = []
        self.time_now = datetime.strptime("2024/12/16 16:00:00", "%Y/%m/%d %H:%M:%S")

    @property
    def interval(self) -> int:
        return 1
    
    def chk_param(self):
        is_error_param = False

        def is_invalid(param_name, param_value, expected_type=(int, float), must_be_positive=True, custom_check=None):
            if param_value is None:
                logger.warning(f"{param_name} 無效，值為: {param_value} (值為 None)")
                return True
            if not isinstance(param_value, expected_type):
                logger.warning(f"{param_name} 類型無效，預期類型: {expected_type}, 實際類型: {type(param_value)}")
                return True
            if must_be_positive and param_value <= 0:
                logger.warning(f"{param_name} 值無效，應大於0，實際值: {param_value}")
                return True
            if custom_check and not custom_check(param_value):
                logger.warning(f"{param_name} 未通過自定義檢查，值為: {param_value}")
                return True
            logger.debug(f"{param_name} 檢查通過，值為: {param_value}")
            return False

        try:
            params_to_check = {
                "soc": (self.soc, (int, float), True),
                "min_soc": (self.min_soc, (int, float), True),
                "max_soc": (self.max_soc, (int, float), True),
                "bess_capacity": (self.bess_capacity, (int, float), True),
                # "gc_interval": (self.gc_interval, (int, float), True),
                # "max_interval": (self.max_interval, (int, float), True),

                "current_load": (self.current_load, (int, float), False),
                "time_now": (self.time_now, (datetime,), False),

                "contract_capacity": (self.parameter.contract_capacity, (int, float), True),
                "PeakLoad_reserve": (self.parameter.PeakLoad_reserve, (int, float), True),
                "dead_zone_soc": (self.parameter.dead_zone_soc, (int, float), True),
            }

            # 統一檢查所有參數
            for param_name, (param_value, expected_type, must_be_positive) in params_to_check.items():
                is_error_param |= is_invalid(param_name, param_value, expected_type, must_be_positive)

        except Exception as e:
            logger.exception(f"檢查參數時發生錯誤: {e}")
            is_error_param = True

        return is_error_param
    
    def chk_in_period(self, period):
        """檢查是否在期間內"""
        if self.time_now.time() < period["end"] or period["end"] == datetime.strptime("00:00", "%H:%M").time():
            if period["start"] <= self.time_now.time():
                return True
        return False
    
    def chk_over_contract_capacity(self):
        """檢查是否超過契約容量"""
        return self.last_command.p_target + self.current_load > self.parameter.contract_capacity
    
    def chk_peak(self):
        """檢查是否為尖峰時段"""
        if "peak" in self.period_switcher:
            for period in self.period_switcher["peak"]:
                if self.chk_in_period(period):
                    return True
        return False
    
    def chk_will_peak(self):
        """當天未來有尖峰時段"""
        if "peak" in self.period_switcher:
            for period in self.period_switcher["peak"]:
                if self.time_now.time() < period["start"] and period["start"] != period["end"]:
                    return True
        return False

    def chk_half_peak(self):
        """檢查是否為半尖峰時段"""
        # if self.time_now.date() in self.parameter.NationalHoliday:
        #     return False
        if "half_peak" in self.period_switcher:
            for period in self.period_switcher["half_peak"]:
                if self.chk_in_period(period):
                    if self.chk_will_peak() == False:
                        return True
        return False

    def chk_under_dem_soc(self):
        """檢查是否低於需量管理SOC保留值(包含死區)"""
        soc_target = self.soc_dem_switcher + self.min_soc
        if self.soc < soc_target - self.parameter.dead_zone_soc:
            return True
        if self.soc < soc_target and self.last_command.p_target < 0:
            return True
        return False
    
    def chk_over_dem_soc(self):
        """檢查是否高於需量管理SOC保留值"""
        soc_target = self.soc_dem_switcher + self.min_soc
        if self.soc > soc_target + self.parameter.dead_zone_soc:
            return True
        return False
    
    def chk_over_peak_soc(self):
        """檢查是否低於尖峰時段SOC保留值"""
        soc_target = self.soc_peak_switcher + self.soc_dem_switcher + self.min_soc
        if self.soc > soc_target:
            return True
        return False
    
    def chk_under_max_soc(self):
        """檢查是否低於最大SOC"""
        if self.soc < self.max_soc - self.parameter.dead_zone_soc:
            return True
        if self.last_command.p_target < 0:
            return True
        return False

    def cal_peak(self):
        """尖峰時段執行"""
        if self.chk_over_dem_soc():
            # logger.warning(f"B_over_dem_soc_{self.cal_power_shift()}")
            discharge_power = max(0, self.cal_power_shift())
            return discharge_power
        else:
            return 0

    def cal_half_peak(self):
        """半尖峰時段執行"""
        if self.chk_will_peak():
            if self.chk_over_peak_soc():
                discharge_power = max(0, self.cal_power_shift())
                return discharge_power
            else:
                return 0
            # return 0
        else:
            discharge_power = self.cal_peak()
            return discharge_power

    def cal_power_diff(self):
        """計算功率差"""
        return self.cal_power_soc_capacity(self.last_command.p_target + self.current_load - self.parameter.contract_capacity)
    
    def cal_power_shift(self):
        """計算功率轉移"""
        return self.cal_power_soc_capacity(self.last_command.p_target + self.current_load - self.parameter.PeakLoad_reserve)
    
    def cal_power_soc_capacity(self, power):
        """計算功率、SOC容量"""
        T = (self.max_interval/3600)
        charge_gap = (self.soc - self.max_soc)/100 * self.bess_capacity / T  #為負值
        charge_gap = min(charge_gap, 0)
        charge_gap = max(charge_gap, self.min_p)
        discharge_gap = (self.soc - self.min_soc)/100 * self.bess_capacity / T  #為正值
        discharge_gap = max(discharge_gap, 0)
        discharge_gap = min(discharge_gap, self.max_p)
        return max(charge_gap, min(discharge_gap, power))

    def update_var(self):
        # 設置夏季判斷條件
        self.time_now = datetime.now()
        summer_period_start = self.parameter.summer_period["start"].replace(year=self.time_now.year).date()
        summer_period_end = self.parameter.summer_period["end"].replace(year=self.time_now.year).date()
        self.is_summer = summer_period_start <= self.time_now.date() <= summer_period_end
        self.max_interval = max(self.gc_interval, self.interval)
        prefix = f"{'sum' if self.is_summer else 'not_sum'}_{self.time_now.weekday()}"
        # period_switcher 設定當日時段
        self.period_switcher = {
            peak_level: getattr(self.parameter, f"{prefix}_{peak_level}_period") for peak_level in ("peak", "half_peak")
        }
        # 優先級切換器
        if self.time_now.date() in self.parameter.NationalHoliday: # 根據 NationalHoliday 移除 priority_switcher 中的 1、2、3
            self.priority_switcher = [item for item in getattr(self.parameter, f"day_{self.time_now.weekday()}_priority_list") if item not in (1, 2, 3)]
        else:
            self.priority_switcher = getattr(self.parameter, f"day_{self.time_now.weekday()}_priority_list")
        # soc_dem_switcher
        self.soc_dem_switcher = getattr(self.parameter, f"{prefix}_dem_soc") if 0 in self.priority_switcher else 0
        # soc_peak_switcherdem_so
        self.soc_peak_switcher = getattr(self.parameter, f"{prefix}_peak_soc")
        
    def execute(self, last_command: Optional[Command]) -> Command:
        self.last_command = last_command
        self.update_var()
        if self.chk_param():
            return Command(p_target=0, q_target=0)
        
        chk_name_list = (self.chk_over_contract_capacity, self.chk_peak, self.chk_half_peak, self.chk_under_dem_soc)
        chk_result_list = [chk() for chk in chk_name_list]
        line = "========Strategy========"
        text4 = f"switcher : {self.priority_switcher}"
        text5 = f"chk_name_order : {[chk_name_list[i].__name__ for i in self.priority_switcher]}"
        text0 = f"chk_name_order_result : {[chk_result_list[i] for i in self.priority_switcher]}"
        text1 = f"{self.time_now}, 夏季={self.is_summer}, day={self.time_now.weekday()}, update_period={self.max_interval}, bess_capacity={self.bess_capacity}"
        text2 = f"soc={self.soc}, min_soc={self.min_soc}, max_soc={self.max_soc}, dem_soc={self.soc_dem_switcher}, peak_soc={self.soc_peak_switcher}"
        text3 = f"current_load={self.current_load}, contract_capacity={self.parameter.contract_capacity}, PeakLoad_reserve={self.parameter.PeakLoad_reserve}"
        logger.info(f"\n{line}\n{text4}\n{text5}\n{text0}\n{text1}\n{text2}\n{text3}\n{line}")

        for i in self.priority_switcher:
            if chk_result_list[i]:
                logger.info(f"{chk_name_list[i].__name__} = True")
                if i == 0: # 超過契約容量
                    # discharge_power = max(0, self.cal_power_diff())
                    discharge_power = self.cal_power_diff()
                    return Command(p_target=discharge_power, q_target=0)
                elif i == 1: # 尖峰時段
                    # logger.warning(f"A_enter_Peak")
                    discharge_power = self.cal_peak()
                    return Command(p_target=discharge_power, q_target=0)
                elif i == 2: # 半尖峰時段
                    discharge_power = self.cal_half_peak()
                    return Command(p_target=discharge_power, q_target=0)
                elif i == 3: # 電量低於需量管理SOC保留值
                    # charge_power = min(0, self.cal_power_diff())
                    charge_power = self.cal_power_diff()
                    return Command(p_target=charge_power, q_target=0)
                
        # 其他情況充電
        if self.chk_under_max_soc():
            # charge_power = min(0, self.cal_power_diff())
            charge_power = self.cal_power_diff()
            return Command(p_target=charge_power, q_target=0)
        return Command(p_target=0, q_target=0)
                
# ===============Equipment=================

class AsyncBaseModbusEquipment:
    OPTIMIZE = True
    PLC = False  # 1 base address
    TWO_READ_SLEEP_INTERVAL = 0
    FUNCTION_CODE_MAX_LENGTH = {
        1: 2000,
        2: 2000,
        3: 120,
        4: 120
    }

    def __init__(self):
        self.client: Optional[ModbusBaseClient] = None
        self.slave_id: int = -1
        self.ID: str = ""
        self.read_list: list[MultiPoint] = []
        self.alarm_list: dict[str, AlarmPoint] = {}
        self.write_list: dict[str, WritePoint] = {}
        self.empty_read_result = {}

        self._get_read_list()
        self._get_alarm_list()
        self._get_write_list()
        self._init_empty_read_result()

    # =============Implement in subclass=================

    def get_read_list(self) -> list[SinglePoint | MultiPoint]:
        return []

    def get_alarm_list(self) -> list[AlarmPoint]:
        return []

    def get_write_list(self) -> list[WritePoint]:
        return []

    def pre_read(self, read_list: list[MultiPoint]) -> list[MultiPoint]:
        return read_list

    def post_read(self, result: dict[str, Any]) -> dict[str, Any]:
        return result

    # =============Public Method=================

    def set_ID(self, ID: str) -> "AsyncBaseModbusEquipment":
        if not isinstance(ID, str):
            raise TypeError("ID must be a string")
        self.ID = ID
        return self

    def set_client(self, client: ModbusBaseClient) -> "AsyncBaseModbusEquipment":
        if not isinstance(client, ModbusBaseClient):
            raise TypeError("client must be an instance of pymodbus.client.async.ModbusSerialClient")
        self.client = client
        return self

    def set_slave_id(self, slave_id: int) -> "AsyncBaseModbusEquipment":
        if not isinstance(slave_id, int):
            raise TypeError("slave_id must be an integer")
        if not 0 <= slave_id <= 255:
            raise ValueError("slave_id must be between 0 and 255")
        self.slave_id = slave_id
        return self

    async def reset_client(self):
        if self.client is None:
            return
        self.client.close()
        await self.client.connect()

    async def read(self) -> dict[str, Any]:
        try:
            await self._validate_before_operation()
        except Exception as e:
            logger.error(f"Validate before operation failed: {e}")
            return self.get_empty_read_result()
        if not self.read_list:
            return {}
        read_list = self.pre_read(self.read_list)
        raw_data = await gather(*[self.read_multipoint(multipoint) for multipoint in read_list])
        if any([len(data) == 0 for data in raw_data]):
            raise ValueError(f"{self} Read data failed")

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(pool, self._process_read_result, read_list, raw_data)
        result = self.post_read(result)
        # logger.info(f"{self} - Read Success")
        return result

    def get_alarm(self, read_result: dict[str, Any]) -> list[Alarm]:
        result = []
        for name, alarm in self.alarm_list.items():
            if name not in read_result:
                logger.warning(f"alarm point: {name} not found in read_result.")
                continue
            if read_result.get(name) is None:
                raise ValueError(f"alarm point: {name} value is None.")
            alarm_result = alarm.check_alarm(read_result[name])
            if alarm_result:
                result.extend(alarm_result)
        return result

    async def write(self, name: str, value: Any) -> bool:
        try:
            await self._validate_before_operation()
        except Exception as e:
            logger.error(f"Validate before operation failed: {e}")
            return False

        write_point = self.write_list.get(name)
        if write_point is None:
            logger.error(f"Write point: {name} not found.")
            return False

        address = write_point.address
        if self.PLC:
            address += 1
        if write_point.function_code == 5:
            response = await self.client.write_coil(
                address=address,
                value=write_point.write(value),
                slave=self.slave_id
            )
        elif write_point.function_code == 6:
            response = await self.client.write_register(
                address=address,
                value=write_point.write(value),
                slave=self.slave_id
            )
        elif write_point.function_code == 16:
            values = write_point.write(value)
            response = await self.client.write_registers(
                address=address,
                values=values,
                slave=self.slave_id
            )
        else:
            raise ValueError(f"Unsupported write function code: {write_point.function_code}")

        if isinstance(response, ExceptionResponse):
            raise ModbusException(f"Error response from slave {self.slave_id}: {response}")
        if response.isError():
            raise ModbusException(f"Error response from slave {self.slave_id}: {response.function_code}")
        return True

    # ============Private Method=================
    def _process_read_result(self, read_list, raw_data) -> dict[str, Any]:
        result = {}
        for multipoint, raw_data in zip(read_list, raw_data):
            result.update(self.process_multipoint(multipoint, raw_data))
        return result

    @staticmethod
    def _sort_point_list(point_list: list[SinglePoint | MultiPoint]) -> list[SinglePoint | MultiPoint]:
        return sorted(point_list, key=lambda x: x.address)

    @staticmethod
    def _classify_read_point(point_list: list[SinglePoint | MultiPoint]) -> dict[int, dict[str, list[SinglePoint | MultiPoint]]]:
        function_code_dict = {}
        for point in point_list:
            if point.function_code not in [1, 2, 3, 4]:
                raise ValueError(f"Unsupported function code: {point.function_code}")
            if point.function_code not in function_code_dict:
                function_code_dict[point.function_code] = {
                    "single_point_list": [],
                    "multi_point_list": []
                }
            if isinstance(point, SinglePoint):
                function_code_dict[point.function_code]["single_point_list"].append(point)
            elif isinstance(point, MultiPoint):
                function_code_dict[point.function_code]["multi_point_list"].append(point)
        return function_code_dict

    @staticmethod
    def _group_single_point(point_list: list[SinglePoint], max_length: int) -> list[MultiPoint]:
        if not point_list:
            return []
        groups = []
        current_group = []
        for point in point_list:
            if not current_group or (point.address - current_group[0].address) <= max_length:
                current_group.append(point)
            else:
                groups.append(MultiPoint(point_list=current_group))
                current_group = [point]
        if current_group:
            groups.append(MultiPoint(point_list=current_group))
        return groups

    def _get_read_list(self):
        read_list = self.get_read_list()
        if not read_list:
            return
        if not all(isinstance(x, (SinglePoint, MultiPoint)) for x in read_list):
            raise TypeError("All elements in read_list must be an instance of SinglePoint or MultiPoint.")
        read_list = self._sort_point_list(read_list)
        function_code_dict = self._classify_read_point(read_list)
        read_list_result = []
        if self.OPTIMIZE:
            for function_code, point_dict in function_code_dict.items():
                max_length = self.FUNCTION_CODE_MAX_LENGTH[function_code]
                grouped_point_list = self._group_single_point(point_dict["single_point_list"], max_length)
                point_dict["multi_point_list"].extend(grouped_point_list)
                read_list_result.extend(point_dict["multi_point_list"])
        else:
            for function_code, point_dict in function_code_dict.items():
                read_list_result.extend(point_dict["multi_point_list"])
                for single_point in point_dict["single_point_list"]:
                    read_list_result.append(MultiPoint(point_list=[single_point]))
        self.read_list = read_list_result

    def _get_alarm_list(self) -> None:
        if (alarm_list := self.get_alarm_list()) is None:
            return
        if not all(isinstance(x, AlarmPoint) for x in alarm_list):
            raise TypeError("All elements in alarm_list must be an instance of AlarmPoint.")
        self.alarm_list = {alarm.point_name: alarm for alarm in alarm_list}

    def _get_write_list(self) -> None:
        write_list = self.get_write_list()
        if not write_list:
            return
        if not all(isinstance(x, WritePoint) for x in write_list):
            raise TypeError("All elements in write_list must be an instance of WritePoint.")
        self.write_list = {write.name: write for write in write_list}

    def _init_empty_read_result(self) -> None:
        result = {}
        for multi_point in self.read_list:
            for single_point in multi_point:
                result[single_point.name] = None
        result = self.post_read(result)
        self.empty_read_result = result

    def get_empty_read_result(self) -> dict[str, Any]:
        return self.empty_read_result

    async def _validate_before_operation(self) -> None:
        if self.client is None:
            raise ValueError("client is not set.")
        if not self.client.connected:
            await self.client.connect()
            if not self.client.connected:
                raise ValueError("client is not connected after trying to reconnect.")
        if self.slave_id == -1:
            raise ValueError("slave_id is not set.")

    async def read_multipoint(self, multipoint: MultiPoint) -> list[int]:
        address = multipoint.address + (1 if self.PLC else 0)
        read_length = multipoint.read_length
        function_code = multipoint.function_code

        if self.slave_id == -1:
            raise ValueError("slave_id is not set.")
        if read_length == 0:
            raise ValueError("read_length is invalid")

        if function_code == 1:
            response = await self.client.read_coils(address=address, count=read_length, slave=self.slave_id)
        elif function_code == 2:
            response = await self.client.read_discrete_inputs(address=address, count=read_length, slave=self.slave_id)
        elif function_code == 3:
            response = await self.client.read_holding_registers(address=address, count=read_length, slave=self.slave_id)
        elif function_code == 4:
            response = await self.client.read_input_registers(address=address, count=read_length, slave=self.slave_id)
        else:
            raise ValueError(f"Unsupported function code: {function_code}")

        if isinstance(response, ExceptionResponse):
            logger.warning(f"Error response from slave {self.slave_id}, {response}")
            return []

        if response.isError():
            logger.warning(f"Error response from slave {self.slave_id}: {response.function_code}")
            return []

        raw_data = response.registers if function_code in [3, 4] else response.bits
        return raw_data

    def process_multipoint(self, multipoint: MultiPoint, raw_data: list) -> dict[str, Any]:
        result = {}
        if len(raw_data) == 0:
            for point in multipoint:
                result[point.name] = None
            return result

        for point in multipoint:
            start = point.address - multipoint.address
            length = point.modbus_data_type.get_length()
            data_slice = raw_data[start: start + length]
            processed_data = point.modbus_data_type.from_modbus(data_slice, endian=point.endian)
            if isinstance(point.modbus_data_type, (IntegerData, FloatData)):
                processed_data = round(processed_data * point.magnitude + point.offset, point.round)
            result[point.name] = processed_data
        return result

    # =============Magic Method=================

    def __str__(self):
        return f"<{self.__class__.__name__} ID={self.ID} slave_id={self.slave_id} client={self.client}>"

    def __repr__(self):
        return self.__str__()

class Equipment(AsyncBaseModbusEquipment):
    def __init__(self, interval: int = 1, disconnect_threshold:int = 5, retries: int = 0):
        super().__init__()
        self.interval = interval
        self.disconnect_threshold = disconnect_threshold
        self.retries = retries
        self.stop_event = asyncio.Event()
        self.disconnect_counter = 0

        self.protection = False
        self.disconnect = True
        self.latest_alarm: list[Alarm] = []
        self.latest_read = self.get_empty_read_result()
        self.start_task = None

    # =============Public Method=================

    def set_slave_id(self, slave_id: int) -> "Equipment":
        super().set_slave_id(slave_id)
        return self

    def set_client(self, client: ModbusBaseClient) -> "Equipment":
        super().set_client(client)
        return self

    def set_interval(self, interval: int) -> "Equipment":
        self.interval = interval
        return self

    def start(self) -> "Equipment":
        self.stop_event.clear()
        self.start_task = asyncio.create_task(self._run())
        return self

    def stop(self) -> "Equipment":
        self.stop_event.set()
        return self

    # =============Protected Method=================
    def _process_alarm(self, read_result: dict[str, Any]):
        self.latest_alarm = self.get_alarm(read_result)
        protected_alarm = list(filter(lambda alarm: alarm.protection == 1, self.latest_alarm))
        if len(protected_alarm) > 0:
            self.protection = True
        else:
            self.protection = False

    async def _run(self):
        while not self.stop_event.is_set():
            try:
                self.latest_read = await self.read()
                self.disconnect_counter = 0
                self.disconnect = False
                with ThreadPoolExecutor() as pool:
                    await asyncio.get_running_loop().run_in_executor(pool, self._process_alarm, self.latest_read)
            except asyncio.CancelledError:
                self.stop_event.set()
                break
            except Exception as e:
                self.disconnect_counter += 1
                if self.disconnect_counter >= self.disconnect_threshold:
                    self.latest_read = self.get_empty_read_result()
                    self.disconnect = True
                    logger.error(f"{self} - {e}")
            finally:            
                # logger.warning(f"1 {asyncio.get_running_loop().time()}")
                await asyncio.sleep(self.interval - (asyncio.get_running_loop().time()+0.3) % self.interval)

    # ============= Magic Method ================

    def __str__(self):
        return f"{self.__class__.__name__}(protection={self.protection}, disconnect={self.disconnect}, client={self.client}, slave_id={self.slave_id})"

    def __repr__(self):
        return self.__str__()

class PM335(Equipment):
# LRS = 0
# HRS = 9999
# VS = 828
# CS = 100
# Energy roll value = 5
# Interval energy period, min = 5
    def get_read_list(self) -> list[SinglePoint | MultiPoint]:
        return [
            SinglePoint(name="p", address=13696, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="v_a", address=13952, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            SinglePoint(name="v_b", address=13954, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            SinglePoint(name="v_c", address=13956, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            SinglePoint(name="i_a", address=13958, data_type="uint32", function_code=0x03, magnitude=0.01, offset=0, round=2, endian=Endian.LITTLE),
            SinglePoint(name="i_b", address=13960, data_type="uint32", function_code=0x03, magnitude=0.01, offset=0, round=2, endian=Endian.LITTLE),
            SinglePoint(name="i_c", address=13962, data_type="uint32", function_code=0x03, magnitude=0.01, offset=0, round=2, endian=Endian.LITTLE),
            SinglePoint(name="p_a", address=13964, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="p_b", address=13966, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="p_c", address=13968, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="q_a", address=13970, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="q_b", address=13972, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="q_c", address=13974, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="s_a", address=13976, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="s_b", address=13978, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="s_c", address=13980, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="pf_a", address=13982, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="pf_b", address=13984, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="pf_c", address=13986, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="vl_ab", address=14012, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            SinglePoint(name="vl_bc", address=14014, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            SinglePoint(name="vl_ca", address=14016, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            # SinglePoint(name="p_old", address=14336, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="q", address=14338, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="s", address=14340, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="pf", address=14342, data_type="int32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="v", address=14356, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            SinglePoint(name="vl", address=14358, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            SinglePoint(name="i", address=14360, data_type="uint32", function_code=0x03, magnitude=0.01, offset=0, round=2, endian=Endian.LITTLE),
            SinglePoint(name="f", address=14468, data_type="uint32", function_code=0x03, magnitude=0.01, offset=0, round=2, endian=Endian.LITTLE),
            SinglePoint(name="imp_kwh", address=14720, data_type="uint32", function_code=0x04, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="exp_kwh", address=14722, data_type="uint32", function_code=0x04, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="tot_kwh", address=14726, data_type="uint32", function_code=0x04, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="imp_kvarh", address=14728, data_type="uint32", function_code=0x04, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="exp_kvarh", address=14730, data_type="uint32", function_code=0x04, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="tot_kvarh", address=14734, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="imp_kvah", address=14742, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="exp_kvah", address=14744, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="tot_kvah", address=14736, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="ins_kw", address=14610, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="ins_kvar", address=14612, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="ins_kva", address=14614, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="acc_kw", address=14622, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
            SinglePoint(name="acc_kvar", address=14624, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3, endian=Endian.LITTLE),
        ]

class VAW(Equipment):
    def get_read_list(self) -> list[SinglePoint | MultiPoint]:
        return [
            SinglePoint(name="exp_kwh", address=0x0000, data_type="uint48", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="imp_kwh", address=0x0003, data_type="int48", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="p", address=0x001E, data_type="int32", function_code=0x03, magnitude=0.01, offset=0, round=2),
            SinglePoint(name="v", address=0x002A, data_type="int16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="i", address=0x002B, data_type="int16", function_code=0x03, magnitude=0.01, offset=0, round=2),
        ]

    def post_read(self, result: dict[str, Any]) -> dict[str, Any]:
        if result["imp_kwh"] is not None:
            result["imp_kwh"] = round(result["imp_kwh"]/10, 1)
        if result["exp_kwh"] is not None:
            result["exp_kwh"] = round(result["exp_kwh"]/10, 1)
        return result

class SE4900(Equipment):
    def get_read_list(self) -> list[SinglePoint | MultiPoint]:
        return [
            SinglePoint(name="acc_kwh", address=0x0000, data_type="uint32", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="v", address=0x0002, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="i", address=0x0004, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="p", address=0x0006, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="v_a", address=0x0008, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="v_b", address=0x000A, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="v_c", address=0x000C, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="i_a", address=0x000E, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="i_b", address=0X0010, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="i_c", address=0x0012, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="p_a", address=0x0020, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="p_b", address=0x0021, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="p_c", address=0x0022, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
        ]

class CATL_BMS(Equipment):
    def __init__(self, module_num: int, cell_num: int, interval: int = 1, disconnect_threshold: int = 5, retries: int = 0):
        self.module_num = module_num
        self.cell_num = cell_num
        balance_cell_calculate = int(cell_num/16)
        if balance_cell_calculate < cell_num/16:
            balance_cell_calculate += 1
        self.balance_cell_calculate = balance_cell_calculate
        temp_cell_calculate = int(cell_num/2)
        if temp_cell_calculate < cell_num/2:
            temp_cell_calculate += 1
        self.temp_cell_calculate = 4

        self.EMS_HB = 0
        super().__init__(interval=interval, disconnect_threshold=disconnect_threshold, retries=retries)

    def start(self) -> "Equipment":
        self.stop_event.clear()
        self.start_task = asyncio.create_task(self._run())
        self.heartbeat_task = asyncio.create_task(self._EMS_HB_update())
        # self.fault_clear_task = asyncio.create_task(self.EMS_fault_clear())
        
        return self

    def get_read_list(self) -> list[SinglePoint | MultiPoint]:
        return [
            MultiPoint(point_list=[
                SinglePoint(name="v", address=0x0020, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="i", address=0x0021, data_type="uint16", function_code=0x03, magnitude=1, offset=-20000, round=0),
                SinglePoint(name="soc", address=0x0022, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="soh", address=0x0023, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="v_max_cell", address=0x0024, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
                SinglePoint(name="v_min_cell", address=0x0025, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
                SinglePoint(name="v_avg_cell", address=0x0026, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
                SinglePoint(name="temp_max_cell", address=0x0027, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="temp_min_cell", address=0x0028, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="temp_avg_cell", address=0x0029, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="i_max_charge", address=0x002a, data_type="uint16", function_code=0x03, magnitude=1, offset=-20000, round=0),
                SinglePoint(name="i_max_discharge", address=0x002b, data_type="uint16", function_code=0x03, magnitude=1, offset=-20000, round=0),
                SinglePoint(name="p_max_charge", address=0x002c, data_type="uint16", function_code=0x03, magnitude=1, offset=-20000, round=0),
                SinglePoint(name="p_max_discharge", address=0x002d, data_type="uint16", function_code=0x03, magnitude=1, offset=-20000, round=0),
                SinglePoint(name="p", address=0x002e, data_type="uint16", function_code=0x03, magnitude=1, offset=-20000, round=0),
                SinglePoint(name="soe_charge", address=0x002f, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="soe_discharge", address=0x0030, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="sys_charge_remain_energy", address=0x0031, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="sys_discharge_remain_energy", address=0x0032, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="v_max_charge", address=0x0033, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="v_min_discharge", address=0x0034, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="ins_det_fun_status", address=0x0035, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="pos_ins_gnd", address=0x0036, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="neg_ins_gnd", address=0x0037, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="sys_avg_temp_environment_MBMU1", address=0x0038, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="sys_avg_temp_environment_MBMU2", address=0x0039, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="sys_ambient_humi_MBMU1", address=0x003e, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="sys_ambient_humi_MBMU2", address=0x003f, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="sys_soc_maintenance_req_status", address=0x0044, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="BMS_TMS1_mode", address=0x0060, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS1_temp_set_by_bms", address=0x0061, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS1_real_mode", address=0x0062, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="Rack_inlet_temp_TMS1_Outlet", address=0x0063, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="Rack_outlet_temp_TMS1_inlet", address=0x0064, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS1_environment_temp", address=0x0065, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS1_fault_code", address=0x0066, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS1_fault_level", address=0x0067, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS1_cooling_mode_protection", address=0x0068, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="BMS_TMS2_mode", address=0x0069, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS2_temp_set_by_bms", address=0x006a, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS2_real_mode", address=0x006b, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="Rack_inlet_temp_TMS2_Outlet", address=0x006c, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="Rack_outlet_temp_TMS2_inlet", address=0x006d, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS2_environment_temp", address=0x006e, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS2_fault_code", address=0x006f, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS2_fault_level", address=0x0070, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS2_cooling_mode_protection", address=0x0071, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="BMS_TMS3_mode", address=0x0072, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS3_temp_set_by_bms", address=0x0073, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS3_real_mode", address=0x0074, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="Rack_inlet_temp_TMS3_Outlet", address=0x0075, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="Rack_outlet_temp_TMS3_inlet", address=0x0076, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS3_environment_temp", address=0x0077, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS3_fault_code", address=0x0078, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS3_fault_level", address=0x0079, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS3_cooling_mode_protection", address=0x007a, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="BMS_TMS4_mode", address=0x007b, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS4_temp_set_by_bms", address=0x007c, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS4_real_mode", address=0x007d, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="Rack_inlet_temp_TMS4_Outlet", address=0x007e, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="Rack_outlet_temp_TMS4_inlet", address=0x007f, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS4_environment_temp", address=0x0080, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="TMS4_fault_code", address=0x0081, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS4_fault_level", address=0x0082, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="TMS4_cooling_mode_protection", address=0x0083, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            ]),
            MultiPoint(point_list=[
                SinglePoint(name="max_cell_v_hv_on", address=0x0130, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="min_cell_v_hv_on", address=0x0131, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="avg_cell_v_hv_on", address=0x0132, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="max_cell_temp_hv_connect_racks", address=0x0133, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="min_cell_temp_hv_connect_racks", address=0x0134, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="avg_cell_temp_hv_connect_racks", address=0x0135, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="SBMU_max_cell_temp_hv_connect_racks", address=0x0136, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="CSC_max_cell_temp_hv_connect_racks", address=0x0137, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="SBMU_min_cell_temp_hv_connect_racks", address=0x0138, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="CSC_min_cell_temp_hv_connect_racks", address=0x0139, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="SBMU_max_cell_v_hv_connect_racks", address=0x013A, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="CSC_max_cell_v_hv_connect_racks", address=0x013B, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="cell_max_cell_v_hv_connect_racks", address=0x013C, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="SBMU_min_cell_v_hv_connect_racks", address=0x013D, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="CSC_min_cell_v_hv_connect_racks", address=0x013E, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="cell_min_cell_v_hv_connect_racks", address=0x013F, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="SBMU_max_cell_temp_hv_disconnect_racks", address=0x0140, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="CSC_max_cell_temp_hv_disconnect_racks", address=0x0141, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="SBMU_max_cell_v_hv_disconnect_racks", address=0x0142, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="CSC_max_cell_v_hv_disconnect_racks", address=0x0143, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="cell_max_cell_v_hv_disconnect_racks", address=0x0144, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="SBMU_min_cell_v_hv_disconnect_racks", address=0x0145, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="CSC_min_cell_v_hv_disconnect_racks", address=0x0146, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="cell_min_cell_v_hv_disconnect_racks", address=0x0147, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="SBMU_min_cell_temp_hv_disconnect_racks", address=0x0148, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="CSC_min_cell_temp_hv_disconnect_racks", address=0x0149, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            ]),
            MultiPoint(point_list=[
                SinglePoint(name="BMS_heartbeat", address=0x0300, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="BMS_power_on", address=0x0301, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="BMS_status", address=0x0302, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="number_connected_hv_bms", address=0x0304, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="sys_step_charge_mode", address=0x0305, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="number_of_racks", address=0x0306, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            ]),
            MultiPoint(point_list=[
                SinglePoint(name="ems_heartbeat", address=0x0380, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="ems_cmd", address=0x0381, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="bat_sys_aux_power_state", address=0x0382, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="ems_rtc_cmd", address=0x038a, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="ems_ins_sw_control_cmd", address=0x038b, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="fault_clear_cmd", address=0x038c, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="rack_dis_cmd1", address=0x038d, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="rack_dis_cmd2", address=0x038e, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="rack_dis_cmd3", address=0x038f, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            ]),
            MultiPoint(point_list=[
                SinglePoint(name="preChg_relay_rack", address=0x0410, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="relay_pos_rack", address=0x0411, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="relay_neg_rack", address=0x0412, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="HV_online_rack", address=0x0413, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="bat_rack_maintenance", address=0x0414, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="v_rack", address=0x0420, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="i_rack", address=0x0422, data_type="uint16", function_code=0x03, magnitude=0.1, offset=-2000, round=1),
                SinglePoint(name="soc_rack", address=0x0423, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="soh_rack", address=0x0424, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="cv_max_rack", address=0x0425, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
                SinglePoint(name="cv_min_rack", address=0x0426, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
                SinglePoint(name="cv_avg_rack", address=0x0427, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3),
                SinglePoint(name="ct_max_rack", address=0x0428, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="ct_min_rack", address=0x0429, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="ct_avg_rack", address=0x042a, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="i_chg_limit_rack", address=0x042b, data_type="uint16", function_code=0x03, magnitude=0.1, offset=-2000, round=1),
                SinglePoint(name="i_dischg_limit_rack", address=0x042c, data_type="uint16", function_code=0x03, magnitude=0.1, offset=-2000, round=1),
                SinglePoint(name="p_chg_limit_rack", address=0x042d, data_type="uint16", function_code=0x03, magnitude=0.1, offset=-2000, round=1),
                SinglePoint(name="p_dischg_limit_rack", address=0x042e, data_type="uint16", function_code=0x03, magnitude=0.1, offset=-2000, round=1),
                SinglePoint(name="p_rack", address=0x042f, data_type="uint16", function_code=0x03, magnitude=0.1, offset=-2000, round=1),
                SinglePoint(name="cv_max_index_rack", address=0x0430, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="cv_min_index_rack", address=0x0431, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="ct_max_index_rack", address=0x0432, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="ct_min_index_rack", address=0x0433, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="chg_SOE_rack", address=0x0434, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="dischg_SOE_rack", address=0x0435, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="cv_sum_rack", address=0x0438, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="tms_mode_rack", address=0x0448, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="tms_temp_rack", address=0x0449, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="tms_realMode_rack", address=0x044a, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="rack_inlet_temp_rack", address=0x044b, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="rack_outlet_temp_rack", address=0x044c, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="tms_env_temp_rack", address=0x044d, data_type="uint16", function_code=0x03, magnitude=1, offset=-50, round=0),
                SinglePoint(name="tms_fault_code_rack", address=0x044f, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="kwh_chg_rack", address=0x0454, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
                SinglePoint(name="kwh_dischg_rack", address=0x0456, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            ]),
            MultiPoint(point_list=[
                SinglePoint(name="error1", address=0x0000, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error2", address=0x0001, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error3", address=0x0002, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error4", address=0x0003, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error5", address=0x0004, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error6", address=0x0005, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error7", address=0x0006, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error8", address=0x0007, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error9", address=0x0008, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error10", address=0x0009, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error11", address=0x0010, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error12", address=0x0011, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error13", address=0x0012, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error14", address=0x0013, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            ]),
            MultiPoint(point_list=[
                SinglePoint(name="error15", address=0x0400, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error16", address=0x0401, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error17", address=0x0402, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error18", address=0x0403, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error19", address=0x0404, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error20", address=0x0405, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error21", address=0x0406, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error22", address=0x0407, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error23", address=0x0408, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="error24", address=0x0409, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            ]),
            ] + [
            MultiPoint(point_list=[
                SinglePoint(name=f"v_cell_{module}_{cell}", address=0x0480+module*self.cell_num+cell, data_type="uint16", function_code=0x03, magnitude=0.001, offset=0, round=3)  for cell in range(self.cell_num)
            ]) for module in range(self.module_num)
            ] + [
            MultiPoint(point_list=[
                SinglePoint(name=f"temp_cell_{module}_{cell}", address=int(0x06c0+module*self.temp_cell_calculate+cell), data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0)  for cell in range(self.temp_cell_calculate)
            ]) for module in range(self.module_num)
            ] + [
            MultiPoint(point_list=[
                SinglePoint(name=f"balance_cell_{module}_{cell}", address=0x0790+module*self.balance_cell_calculate+cell, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0) for module in range(self.module_num) for cell in range(self.balance_cell_calculate)
            ])
            ]

    def get_alarm_list(self) -> list[AlarmPoint]:
        return [
            AlarmBitPoint(point_name="error1", mapping={
                0:Alarm(name="Single cell over-voltage warning level 1", level=1, protection=0),
                1:Alarm(name="Single cell over-voltage warning level 2", level=2, protection=1),
                2:Alarm(name="Single cell over-voltage warning level 3", level=2, protection=1),
                3:Alarm(name="Single cell under-voltage warning level 1", level=1, protection=0),
                4:Alarm(name="Single cell under-voltage warning level 2", level=2, protection=1),
                5:Alarm(name="Single cell under-voltage warning level 3", level=2, protection=1),
                6:Alarm(name="Single cell extreme over-voltage warning", level=2, protection=1),
                7:Alarm(name="Single cell extreme under-voltage warning", level=2, protection=1),
                8:Alarm(name="Single rack over-voltage warning level 1", level=1, protection=0),
                9:Alarm(name="Single rack over-voltage warning level 2", level=2, protection=1),
                10:Alarm(name="Single rack under-voltage warning level 1", level=1, protection=0),
                11:Alarm(name="Single rack under-voltage warning level 2", level=2, protection=1),
                12:Alarm(name="Big voltage difference between cells warning", level=2, protection=1),
                13:Alarm(name="Invalid cell voltage warning", level=2, protection=1),
                14:Alarm(name="Replacement required due to single cell over discharge", level=2, protection=1),
                15:Alarm(name="Replacement required due to single cell over charge", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error2", mapping={
                0:Alarm(name="Low SOC over discharge warning", level=2, protection=1),
                1:Alarm(name="Low SOC over discharge warning", level=2, protection=1),
                2:Alarm(name="Discharge over-current warning level 1", level=1, protection=0),
                3:Alarm(name="Discharge over-current warning level 2", level=2, protection=1),
                4:Alarm(name="Discharge over-current warning level 3", level=2, protection=1),
                5:Alarm(name="Discharge over-current warning level 4", level=2, protection=1),
                6:Alarm(name="Charge over-current warning level 1", level=1, protection=0),
                7:Alarm(name="Charge over-current warning level 2", level=2, protection=1),
                8:Alarm(name="Charge over-current warning level 3", level=2, protection=1),
                9:Alarm(name="Charge over-current warning level 4", level=2, protection=1),
                10:Alarm(name="Current sensor failed warning", level=2, protection=1),
                14:Alarm(name="Single cell over-temperature warning level 1", level=1, protection=0),
                15:Alarm(name="Single cell over-temperature warning level 2", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error3", mapping={
                0:Alarm(name="Single cell over-temperature warning level 3", level=2, protection=1),
                1:Alarm(name="Single cell over-temperature warning level 4", level=2, protection=1),
                2:Alarm(name="Single cell under-temperature warning level 1", level=1, protection=0),
                3:Alarm(name="Single cell under-temperature warning level 2", level=2, protection=1),
                4:Alarm(name="Single cell under-temperature warning level 3", level=2, protection=1),
                5:Alarm(name="Big temperature difference between cells warning level 1", level=1, protection=0),
                6:Alarm(name="Big temperature difference between cells warning level 2", level=2, protection=1),
                7:Alarm(name="Big temperature difference between cells warning level 3", level=2, protection=1),
                15:Alarm(name="Single temperature sampling abnormal warning", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error4", mapping={
                0:Alarm(name="Multiple temperature sampling abnormal warning", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error5", mapping={
                9:Alarm(name="CSC 24V power supply abnormal warning", level=2, protection=1),
                10:Alarm(name="SBMU 24V power supply abnormal warning", level=2, protection=1),
                14:Alarm(name="MSD warning", level=2, protection=1),
                15:Alarm(name="Rack fuse warning", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error6", mapping={
                0:Alarm(name="Rack isolate switch warning", level=2, protection=1),
                1:Alarm(name="Main positive relay sticking warning", level=2, protection=1),
                2:Alarm(name="Main negative relay sticking warning", level=2, protection=1),
                3:Alarm(name="Both main positive relay and main negative relay sticking fault", level=2, protection=1),
                4:Alarm(name="Main positive relay open circuit warning", level=2, protection=1),
                5:Alarm(name="Main negative open circuit warning", level=2, protection=1),
                6:Alarm(name="Battery rack door (travel switch) fault", level=2, protection=1),
                7:Alarm(name="Main control box fan warning", level=2, protection=1),
                9:Alarm(name="Rack fire system level 1 fault (gas detector)", level=1, protection=0),
                10:Alarm(name="Rack fire system level 1 fault (smoke detector)", level=1, protection=0),
                11:Alarm(name="Rack fire system level 2 fault (temperature detector)", level=2, protection=1),
                14:Alarm(name="Aerosol eruption state fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error7", mapping={
                0:Alarm(name="Rack fire system level 2 fault (gas & smoke sensors)", level=2, protection=1),
                3:Alarm(name="Rack humidifier warning", level=2, protection=1),
                5:Alarm(name="Inner communication warning (CCAN)", level=2, protection=1),
                6:Alarm(name="Inner communication warning (SCAN)", level=2, protection=1),
                7:Alarm(name="Inner communication warning (MCAN)", level=2, protection=1),
                8:Alarm(name="Inner communication warning 1(CCAN-TMS)", level=2, protection=1),
                10:Alarm(name="Inner communication warning 2(CCAN-TMS)", level=2, protection=1),
                11:Alarm(name="Balancing circuit warning", level=2, protection=1),
                13:Alarm(name="SOC low warning level 1", level=1, protection=0),
                14:Alarm(name="SOC low warning level 2", level=2, protection=1),
                15:Alarm(name="HV circuit open circuit warning", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error8", mapping={
                0:Alarm(name="Distributed TMS warning", level=2, protection=1),
                1:Alarm(name="Distributed TMS mode conflict warning", level=2, protection=1),
                2:Alarm(name="Distributed TMS warning", level=2, protection=1),
                3:Alarm(name="Pre-charging failed twice warning", level=2, protection=1),
                9:Alarm(name="HV+&HV- reversed connection fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error9", mapping={
                0:Alarm(name="Thermal runaway caused fire fault", level=2, protection=1),
                5:Alarm(name="HV circuit (Fuse)open circuit warning", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error10", mapping={
                8:Alarm(name="Big temperature difference between racks", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error11", mapping={
                0:Alarm(name="Insulation warning", level=2, protection=1),
                1:Alarm(name="Insulation fault", level=2, protection=1),
                2:Alarm(name="Invalid insulation fault", level=2, protection=1),
                3:Alarm(name="The quantity of HV racks less than setting value fault", level=2, protection=1),
                6:Alarm(name="SBMU communication lost warning", level=2, protection=1),
                7:Alarm(name="SBMU communication lost fault", level=2, protection=1),
                8:Alarm(name="EMS communication lost fault", level=2, protection=1),
                9:Alarm(name="IMM communication lost fault", level=2, protection=1),
                10:Alarm(name="Air conditioner communication lost warning", level=2, protection=1),
                11:Alarm(name="Centralized TMS communication fault", level=2, protection=1),
                12:Alarm(name="Centralized TMS communication warning", level=2, protection=1),
                13:Alarm(name="Centralized TMS fault level 2", level=2, protection=1),
                14:Alarm(name="Centralized TMS mode conflict fault", level=2, protection=1),
                15:Alarm(name="Centralized TMS fault level 1", level=1, protection=0),
            }),
            AlarmBitPoint(point_name="error12", mapping={
                0:Alarm(name="SPD failure warning", level=2, protection=1),
                1:Alarm(name="AUX Power DCDC failure warning", level=2, protection=1),
                2:Alarm(name="AUX Power ACDC failure warning", level=2, protection=1),
                3:Alarm(name="AUX Power failure fault", level=2, protection=1),
                4:Alarm(name="Fire system fault level 1_MBMU", level=1, protection=0),
                5:Alarm(name="Fire system fault level 2_MBMU", level=2, protection=1),
                6:Alarm(name="Fire system failure warning MBMU", level=2, protection=1),
                7:Alarm(name="E-STOP fault", level=2, protection=1),
                8:Alarm(name="Client E-STOP fault", level=2, protection=1),
                9:Alarm(name="Electrical rack door (travel switch) fault", level=2, protection=1),
                10:Alarm(name="Electrical rack fan warning", level=2, protection=1),
                11:Alarm(name="Smoke exhaust ventilation body warning", level=2, protection=1),
                12:Alarm(name="Smoke exhaust ventilation state fault", level=2, protection=1),
                13:Alarm(name="Humidifier1 failure warning", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error13", mapping={
                0:Alarm(name="Humidifier2 failure warning", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error14", mapping={
                3:Alarm(name="MBMU 24V power supply abnormal fault", level=2, protection=1),
                4:Alarm(name="Slave MBMU communication lost fault", level=2, protection=1),
                7:Alarm(name="Centralized TMS warning level 3", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error15", mapping={
                0:Alarm(name="Single cell over-voltage warning level 1", level=1, protection=0),
                1:Alarm(name="Single cell over-voltage warning level 2", level=2, protection=1),
                2:Alarm(name="Single cell over-voltage warning level 3", level=2, protection=1),
                3:Alarm(name="Single cell under-voltage warning level 1", level=1, protection=0),
                4:Alarm(name="Single cell under-voltage warning level 2", level=2, protection=1),
                5:Alarm(name="Single cell under-voltage warning level 3", level=2, protection=1),
                6:Alarm(name="Single cell extreme over-voltage fault", level=2, protection=1),
                7:Alarm(name="Single cell extreme under-voltage fault", level=2, protection=1),
                8:Alarm(name="Single rack over-voltage warning level 1", level=1, protection=0),
                9:Alarm(name="Single rack over-voltage warning level 2", level=2, protection=1),
                10:Alarm(name="Single rack under-voltage warning level 1", level=1, protection=0),
                11:Alarm(name="Single rack under-voltage warning level 2", level=2, protection=1),
                12:Alarm(name="Big voltage difference between cells warning", level=2, protection=1),
                13:Alarm(name="Invalid cell voltage fault", level=2, protection=1),
                14:Alarm(name="Replacement required due to single cell over discharge", level=2, protection=1),
                15:Alarm(name="Replacement required due to single cell over charge", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error16", mapping={
                0:Alarm(name="Low SOC over discharge warning", level=2, protection=1),
                1:Alarm(name="Low SOC over discharge fault", level=2, protection=1),
                2:Alarm(name="Discharge over-current warning level 1", level=1, protection=0),
                3:Alarm(name="Discharge over-current warning level 2", level=2, protection=1),
                4:Alarm(name="Discharge over-current warning level 3", level=2, protection=1),
                5:Alarm(name="Discharge over-current fault level 4", level=2, protection=1),
                6:Alarm(name="Charge over-current warning level 1", level=1, protection=0),
                7:Alarm(name="Charge over-current warning level 2", level=2, protection=1),
                8:Alarm(name="Charge over-current warning level 3", level=2, protection=1),
                9:Alarm(name="Charge over-current fault level 4", level=2, protection=1),
                10:Alarm(name="Current sensor failed fault", level=2, protection=1),
                14:Alarm(name="Single cell over-temperature warning level 1", level=1, protection=0),
                15:Alarm(name="Single cell over-temperature warning level 2", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error17", mapping={
                0:Alarm(name="Single cell over-temperature warning level 3", level=2, protection=1),
                1:Alarm(name="Single cell over-temperature fault level 4", level=2, protection=1),
                2:Alarm(name="Single cell under-temperature warning level 1", level=1, protection=0),
                3:Alarm(name="Single cell under-temperature warning level 2", level=2, protection=1),
                4:Alarm(name="Single cell under-temperature warning level 3", level=2, protection=1),
                5:Alarm(name="Big temperature difference between cells warning level 1", level=1, protection=0),
                6:Alarm(name="Big temperature difference between cells warning level 2", level=2, protection=1),
                7:Alarm(name="Big temperature difference between cells warning level 3", level=2, protection=1),
                15:Alarm(name="Single temperature sampling abnormal fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error18", mapping={
                0:Alarm(name="Multiple temperature sampling abnormal fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error19", mapping={
                9:Alarm(name="CSC 24V power supply abnormal fault", level=2, protection=1),
                10:Alarm(name="SBMU 24V power supply abnormal fault", level=2, protection=1),
                14:Alarm(name="MSD fault", level=2, protection=1),
                15:Alarm(name="Rack fuse fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error20", mapping={
                0:Alarm(name="Rack isolate switch fault", level=2, protection=1),
                1:Alarm(name="Main positive relay", level=2, protection=1),
                2:Alarm(name="Main negative relay sticking fault", level=2, protection=1),
                3:Alarm(name="Both main positive relay and main negative relay sticking fault", level=2, protection=1),
                4:Alarm(name="Main positive relay open circuit fault", level=2, protection=1),
                5:Alarm(name="Main negative open circuit fault", level=2, protection=1),
                6:Alarm(name="Battery rack door (travel switch) fault", level=2, protection=1),
                7:Alarm(name="Main control box fan fault", level=2, protection=1),
                9:Alarm(name="Rack fire system level 1 fault (gas sensor)", level=1, protection=0),
                10:Alarm(name="Rack fire system level 1 fault (smoke sensor)", level=1, protection=0),
                11:Alarm(name="Rack fire system level 2 fault (temperature sensor)", level=2, protection=1),
                14:Alarm(name="Aerosol eruption state warning", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error21", mapping={
                0:Alarm(name="Rack fire system level 2 fault (gas & smoke sensors)", level=2, protection=1),
                3:Alarm(name="Rack humidifier warning", level=2, protection=1),
                5:Alarm(name="Inner communication fault (CCAN)", level=2, protection=1),
                6:Alarm(name="Inner communication fault (SCAN)", level=2, protection=1),
                7:Alarm(name="Inner communication fault (MCAN)", level=2, protection=1),
                8:Alarm(name="Inner communication warning (CCAN-TMS)", level=2, protection=1),
                10:Alarm(name="Inner communication fault (CCAN-TMS)", level=2, protection=1),
                11:Alarm(name="Balancing circuit fault", level=2, protection=1),
                13:Alarm(name="SOC low warning level 1", level=1, protection=0),
                14:Alarm(name="SOC low warning level 2", level=2, protection=1),
                15:Alarm(name="HV circuit open circuit fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error22", mapping={
                0:Alarm(name="Distributed TMS warning", level=2, protection=1),
                1:Alarm(name="Distributed TMS mode conflict fault", level=2, protection=1),
                2:Alarm(name="Distributed TMS fault", level=2, protection=1),
                3:Alarm(name="Pre-charging failed twice fault", level=2, protection=1),
                4:Alarm(name="HV+&HVreversed connection fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error23", mapping={
                0:Alarm(name="Thermal runaway caused fire fault", level=2, protection=1),
                5:Alarm(name="HV circuit (Fuse)open circuit fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="error24", mapping={
                8:Alarm(name="Big temperature difference between racks", level=2, protection=1),
            }),
        ]

    def get_write_list(self) -> list[WritePoint]:
        return [
            WritePoint(name="BMS_heartbeat", address=0x0380, data_type="uint16", function_code=0x06, magnitude=1, offset=0, min_value=0, max_value=15),
            WritePoint(name="BMS_on_off", address=0x0381, data_type="uint16", function_code=0x06, magnitude=1, offset=0, in_value=[0, 1, 2, 3]),
            WritePoint(name="BMS_fault_clear", address=0x038c, data_type="uint16", function_code=0x06, magnitude=1, offset=0, in_value=[0, 1]),
        ]

    async def _EMS_HB_update(self):
        while not self.stop_event.is_set():
            try:
                ok = await self.write("BMS_heartbeat", self.EMS_HB)
                if not ok:
                    logger.error(f"{self} - BMS_heartbeat write returned False")
            except Exception as e:
                logger.error(f"{self} - BMS_heartbeat write failed: {e}")
                await asyncio.sleep(1)
                continue

            self.EMS_HB += 1
            if self.EMS_HB > 15:
                self.EMS_HB = 0
            await asyncio.sleep(self.interval - asyncio.get_running_loop().time() % self.interval)

    async def EMS_fault_clear(self):
        logger.warning(f"{self} - 清除BMS告警")
        await self.write("BMS_fault_clear", 1)
        await asyncio.sleep(1)
        await self.write("BMS_fault_clear", 0)

    async def set_bess_on(self):
        logger.info(f"{self} - set bess on")
        return await self.write("BMS_on_off", 2)
    
    async def set_bess_off(self):
        logger.info(f"{self} - set bess off")
        return await self.write("BMS_on_off", 3)
    
    def post_read(self, result: dict[str, Any]) -> dict[str, Any]:
        # v_cell_{module}_{cell}  ->  v_cell[][]
        result["v_cell"] = [[[result[f"v_cell_{module}_{cell}"] for cell in range(self.cell_num)] for module in range(self.module_num)]]
        for module in range(self.module_num):
            for cell in range(self.cell_num):
                del result[f"v_cell_{module}_{cell}"]

        # temp_cell_{module}_{cell}  ->  temp_cell[][] 
        # Each byte represents one temperaturey point,
        # Low byte means odd number temperature points,
        # High byte means even number temperature points,
        # Numbers count from one
        temp_cell_result = []
        for module in range(self.module_num):
            temp_result = []
            for cell in range(self.temp_cell_calculate):
                twobyte_temp = result[f"temp_cell_{module}_{cell}"]
                if twobyte_temp is not None:
                    low_byte_temp = (twobyte_temp & 0x00FF) - 50
                    high_byte_temp = ((twobyte_temp & 0xFF00) >> 8) - 50
                else:
                    low_byte_temp = None
                    high_byte_temp = None
                temp_result.append(low_byte_temp)
                temp_result.append(high_byte_temp)
            temp_cell_result.append(temp_result)
                                    
        result["temp_cell"] = [temp_cell_result]
        for module in range(self.module_num):
            for cell in range(self.temp_cell_calculate):
                del result[f"temp_cell_{module}_{cell}"]

        # balance_cell_{module}_{cell}  ->  balance_cell[][]    
        result["balance_cell"] = [[[result[f"balance_cell_{module}_{cell}"] for cell in range(self.balance_cell_calculate)] for module in range(self.module_num)]]
        for module in range(self.module_num):
            for cell in range(self.balance_cell_calculate):
                del result[f"balance_cell_{module}_{cell}"]
        return result

class SNPOWER_PCS(Equipment):
    def get_read_list(self) -> list[SinglePoint | MultiPoint]:
        return [
            SinglePoint(name="vl_ab", address=0, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1, endian=Endian.LITTLE),
            SinglePoint(name="vl_bc", address=1, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="vl_ca", address=2, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="f", address=3, data_type="uint32", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="i_a", address=5, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="i_b", address=6, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="i_c", address=7, data_type="uint16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="p", address=9, data_type="int16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="q", address=11, data_type="int16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="s", address=12, data_type="int32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="pf", address=14, data_type="int16", function_code=0x03, magnitude=0.001, offset=0, round=3),
            SinglePoint(name="v_battery", address=15, data_type="int16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="i_battery", address=16, data_type="int16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="p_battery", address=17, data_type="int32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="temp_IGBT", address=19, data_type="int16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="temp_area", address=20, data_type="int16", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="available_kva", address=21, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="tot_charge_kwh", address=1150, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="tot_discharge_kwh", address=1152, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="tot_charge_time", address=1158, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="tot_discharge_time", address=1160, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="daily_charge_kwh", address=1154, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="daily_discharge_kwh", address=1156, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="daily_charge_time", address=1162, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="daily_discharge_time", address=1164, data_type="uint32", function_code=0x03, magnitude=0.1, offset=0, round=1),
            SinglePoint(name="alarm1", address=750, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm2", address=751, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm3", address=753, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm4", address=755, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm5", address=757, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm6", address=759, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm7", address=760, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm8", address=761, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm9", address=767, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm10", address=769, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm11", address=771, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm12", address=772, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm13", address=950, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm14", address=951, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            SinglePoint(name="alarm15", address=952, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            MultiPoint(point_list=[
                SinglePoint(name="mode1", address=850, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="mode2", address=852, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="mode3", address=855, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            ]),
            MultiPoint(point_list=[
                SinglePoint(name="mode4", address=1050, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
                SinglePoint(name="mode5", address=1051, data_type="uint16", function_code=0x03, magnitude=1, offset=0, round=0),
            ])
        ]
    
    def get_alarm_list(self) -> list[AlarmPoint]:
        return [
            AlarmBitPoint(point_name="alarm1", mapping={
                0:Alarm(name="AC Undervoltage Alarm", level=2, protection=1),
                1:Alarm(name="AC Overvoltage Alarm", level=2, protection=1),
                2:Alarm(name="AC Underfrequency Alarm", level=2, protection=1),
                3:Alarm(name="AC Overfrequency Alarm", level=2, protection=1),
                4:Alarm(name="AC Voltage Imbalance Alarm", level=2, protection=1),
                5:Alarm(name="AC Current Imbalance Alarm", level=2, protection=1),
                6:Alarm(name="Low Voltage Ride Through  Alarm", level=2, protection=1),
                7:Alarm(name="High Voltage Ride Through Alarm", level=2, protection=1),
                8:Alarm(name="AC Reverse Phase Sequence Alarm", level=2, protection=1),
                9:Alarm(name="AC Phase Loss Alarm", level=2, protection=1),
                10:Alarm(name="AC Voltage Abnormality Alarm", level=2, protection=1),
                11:Alarm(name="Frequency Abnormality Alarm", level=2, protection=1),
                12:Alarm(name="Anti-Islanding Alarm", level=2, protection=1),
                13:Alarm(name="AC Side Short-Circuit Protection Alarm", level=2, protection=1),
                14:Alarm(name="AC Current Abnormality Alarm", level=2, protection=1),
                15:Alarm(name="Harmonic Current Overlimit Alarm", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm2", mapping={
                0:Alarm(name="Phase-Locked Loop Desynchronization Alarm", level=2, protection=1),
                1:Alarm(name="AC Ground Voltage Abnormality Alarm", level=2, protection=1),
                2:Alarm(name="Output Leakage Current Abnormality Alarm", level=2, protection=1),
                3:Alarm(name="Inverter DC Component Voltage Detection Abnormality Alarm", level=2, protection=1),
                4:Alarm(name="Module Inductor Current Imbalance Alarm", level=2, protection=1),
                5:Alarm(name="AC Overcurrent Alarm", level=2, protection=1),
                6:Alarm(name="AC Power Abnormality Alarm", level=2, protection=1),
                7:Alarm(name="Pre-Charge Fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm3", mapping={
                0:Alarm(name="Battery Undervoltage Alarm", level=2, protection=1),
                1:Alarm(name="Battery Overvoltage Alarm", level=2, protection=1),
                2:Alarm(name="Battery Reverse Connection Alarm", level=2, protection=1),
                3:Alarm(name="Battery Overcurrent Alarm", level=2, protection=1),
                4:Alarm(name="Insulation Resistance Abnormality Alarm", level=2, protection=1),
                5:Alarm(name="Battery Ground Voltage Abnormality Alarm", level=2, protection=1),
                6:Alarm(name="Battery Fuse Disconnection Alarm", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm4", mapping={
                0:Alarm(name="DC Surge Arrester Fault", level=2, protection=1),
                1:Alarm(name="AC Surge Arrester Fault", level=2, protection=1),
                4:Alarm(name="U1 Hardware Version Error", level=2, protection=1),
                5:Alarm(name="Leakage Current Detection Circuit Abnormality", level=2, protection=1),
                6:Alarm(name="Inverter Current Detection Circuit Abnormality", level=2, protection=1),
                7:Alarm(name="DC Current Detection Circuit Abnormality", level=2, protection=1),
                8:Alarm(name="AC Relay Open Circuit", level=2, protection=1),
                9:Alarm(name="AC Relay Short Circuit", level=2, protection=1),
                10:Alarm(name="DC Relay Open Circuit", level=2, protection=1),
                11:Alarm(name="DC Relay Short Circuit", level=2, protection=1),
                12:Alarm(name="DC Positive Pole Relay Contact Short Circuit", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm5", mapping={
                0:Alarm(name="12V Power Abnormality", level=2, protection=1),
                1:Alarm(name="Drive Power Abnormality", level=2, protection=1),
                2:Alarm(name="5V Power Abnormality", level=2, protection=1),
                3:Alarm(name="Relay Power Abnormality", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm6", mapping={
                0:Alarm(name="Positive and Negative Bus Imbalance", level=2, protection=1),
                1:Alarm(name="Positive Bus Overvoltage", level=2, protection=1),
                2:Alarm(name="Negative Bus Overvoltage", level=2, protection=1),
                3:Alarm(name="Bus Software Overvoltage", level=2, protection=1),
                4:Alarm(name="Bus Hardware Overvoltage", level=2, protection=1),
                5:Alarm(name="Bus Undervoltage", level=2, protection=1),
                6:Alarm(name="Positive Bus Short Circuit", level=2, protection=1),
                7:Alarm(name="Negative Bus Short Circuit", level=2, protection=1),
                8:Alarm(name="Bus Short Circuit", level=2, protection=1),
                9:Alarm(name="DC Voltage Detection Abnormality", level=2, protection=1),
                10:Alarm(name="Conversion Efficiency Abnormality", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm7", mapping={
                0:Alarm(name="Inductor Current Detection Abnormality", level=2, protection=1),
                1:Alarm(name="Inductor Current Abnormality", level=2, protection=1),
                2:Alarm(name="Inverter Voltage Abnormality", level=2, protection=1),
                3:Alarm(name="Cycle-by-Cycle Current Limiting", level=2, protection=1),
                4:Alarm(name="Phase A Cycle-by-Cycle Current Limiting Alarm", level=2, protection=1),
                5:Alarm(name="Phase B Cycle-by-Cycle Current Limiting Alarm", level=2, protection=1),
                6:Alarm(name="Phase C Cycle-by-Cycle Current Limiting Alarm", level=2, protection=1),
                7:Alarm(name="Phase A Cycle-by-Cycle Current Limiting Protection", level=2, protection=1),
                8:Alarm(name="Phase B Cycle-by-Cycle Current Limiting Protection", level=2, protection=1),
                9:Alarm(name="Phase C Cycle-by-Cycle Current Limiting Protection", level=2, protection=1),
                10:Alarm(name="Phase A Wave Generation Abnormality", level=2, protection=1),
                11:Alarm(name="Phase B Wave Generation Abnormality", level=2, protection=1),
                12:Alarm(name="Phase C Wave Generation Abnormality", level=2, protection=1),
                13:Alarm(name="IGBT Driver Cable Fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm8", mapping={
                0:Alarm(name="DC Soft Start Failure", level=2, protection=1),
                1:Alarm(name="AC Soft Start Failure", level=2, protection=1),
                3:Alarm(name="DC Side Power Abnormality", level=2, protection=1),
                4:Alarm(name="AC Contactor Open Circuit Fault", level=2, protection=1),
                5:Alarm(name="Discharge Resistor Overtemperature", level=2, protection=1),
                6:Alarm(name="Inverter Voltage Detection Abnormality", level=2, protection=1),
                7:Alarm(name="AC Capacitor Maintenance Required", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm9", mapping={
                0:Alarm(name="Internal Compartment Overtemperature", level=2, protection=1),
                1:Alarm(name="Discharge Resistor Overtemperature", level=2, protection=1),
                2:Alarm(name="IGBT Overtemperature Protection", level=2, protection=1),
                3:Alarm(name="Converter Overtemperature Operation", level=2, protection=1),
                4:Alarm(name="Capacitor Overtemperature", level=2, protection=1),
                5:Alarm(name="Capacitor Temperature Detection NTC Abnormality", level=2, protection=1),
                6:Alarm(name="Compartment Temperature 1 Detection NTC Abnormality", level=2, protection=1),
                7:Alarm(name="Compartment Temperature 2 Detection NTC Abnormality", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm10", mapping={
                0:Alarm(name="Q_U Parameter Setting Error", level=2, protection=1),
                1:Alarm(name="P_Freq Parameter Setting Error", level=2, protection=1),
                2:Alarm(name="Model Capacity Setting Error", level=2, protection=1),
                3:Alarm(name="Frequency Response Curve Setting Error", level=2, protection=1),
                4:Alarm(name="System Parameter Setting Abnormality", level=2, protection=1),
                5:Alarm(name="Control E2 Storage Error", level=2, protection=1),
                6:Alarm(name="Control Flash Storage Error", level=2, protection=1),
                7:Alarm(name="Cycle-by-Cycle Current Limiting Parameter Setting Abnormality", level=2, protection=1),
                8:Alarm(name="Real-Time Information Abnormality", level=2, protection=1),
                9:Alarm(name="Dispatch Information Abnormality", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm11", mapping={
                0:Alarm(name="Parallel CAN Communication Fault", level=2, protection=1),
                1:Alarm(name="Dispatch CAN Communication Fault", level=2, protection=1),
                2:Alarm(name="Standard Module Communication Fault", level=2, protection=1),
                3:Alarm(name="DSP_FPGA SPI Abnormality", level=2, protection=1),
                4:Alarm(name="Fast Module Communication Fault", level=2, protection=1),
                5:Alarm(name="DSP CLA Abnormality", level=2, protection=1),
                6:Alarm(name="DSP Interrupt Abnormality", level=2, protection=1),
                7:Alarm(name="DSP Main Loop Abnormality", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm12", mapping={
                0:Alarm(name="Single Unit Overload", level=2, protection=1),
                1:Alarm(name="Parallel System Overload", level=2, protection=1),
                2:Alarm(name="Parallel System Current Sharing Fault", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm13", mapping={
                0:Alarm(name="External 485 Communication Interruption", level=2, protection=1),
                1:Alarm(name="BMS Communication Interruption", level=2, protection=1),
                3:Alarm(name="Fast SCI Communication Abnormality", level=2, protection=1),
                4:Alarm(name="ModbusTCP Communication Interruption", level=2, protection=1),
                5:Alarm(name="Dry Contact Fast Shutdown", level=2, protection=1),
                6:Alarm(name="Remote EPO", level=2, protection=1),
                7:Alarm(name="Smoke Alarm", level=2, protection=1),
                14:Alarm(name="Storage Abnormality", level=2, protection=1),
                15:Alarm(name="Clock Abnormality", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm14", mapping={
                0:Alarm(name="Monitoring 12V Power Abnormality", level=2, protection=1),
                1:Alarm(name="Monitoring 5V Power Abnormality", level=2, protection=1),
                2:Alarm(name="Monitoring 3.3V Power Abnormality", level=2, protection=1),
            }),
            AlarmBitPoint(point_name="alarm15", mapping={
                0:Alarm(name="Turbulence Fan Fault", level=2, protection=1),
                1:Alarm(name="IGBT Fan Fault", level=2, protection=1),
            }),
        ]
    
    def get_write_list(self) -> list[WritePoint]:
        return [
            WritePoint(name="PQ_p_ref", address=13200, data_type="int16", function_code=0x06, magnitude=0.1, offset=0, max_value=240, min_value=-240),
            WritePoint(name="PQ_q_ref", address=13201, data_type="int16", function_code=0x06, magnitude=0.1, offset=0, max_value=240, min_value=-240),
            WritePoint(name="PCS_on_off", address=5500, data_type="uint16", function_code=0x06, in_value=[0x0000, 0xFF00]),
        ]

    async def set_pcs_on(self):
        logger.info(f"{self} - set pcs on")
        return await self.write("PCS_on_off", 0xFF00)
    
    async def set_pcs_off(self):
        logger.info(f"{self} - set pcs off")
        return await self.write("PCS_on_off", 0x0000)
    
    async def set_pcs_pq(self, p: int | float, q: int | float):
        logger.info(f"{self} - set pcs pq - p:{p}, q:{q}")
        await self.write("PQ_p_ref", p)
        await self.write("PQ_q_ref", q)

class ET7x00_RIO(Equipment):
    def get_read_list(self) -> list[SinglePoint | MultiPoint]:
        return [
            SinglePoint(name="switch_on", address=0x0000, data_type="uint16", function_code=0x02),
            SinglePoint(name="switch_off", address=0x0001, data_type="uint16", function_code=0x02),
        ]

    def get_alarm_list(self) -> list[AlarmPoint]:
        return [
            # AlarmTablePoint(point_name="switch_on", mapping={
            #     1:Alarm(name="MVCB開關告警",level=2, protection=2),
            # }),
            # AlarmTablePoint(point_name="switch_off", mapping={
            #     1:Alarm(name="MVCB開關告警",level=2, protection=1),
            # }),
        ]
    
    def post_read(self, result: dict[str, Any]) -> dict[str, Any]:
        switch_state = None
        if result["switch_on"] == 1 and result["switch_off"] == 0: # 開
            switch_state = 1
        elif result["switch_on"] == 0 and result["switch_off"] == 1: # 關
            switch_state = 0
        else:
            switch_state = 2
        result["switch_on"] = switch_state
        result.pop("switch_off", None)
        return result

# ================== MongoDB ==================

class MongoUploader:
    def __init__(self, mongo_db, equipment: Equipment, ID: str, collection: str, interval: int = 1):
        self.mongo_db = mongo_db
        self.equipment = equipment
        self.ID = ID
        self.collection = collection
        self.interval = interval
        self.stop_event = asyncio.Event()
        self._tasks = []

    def start(self):
        t1 = asyncio.create_task(self.run())
        self._tasks.append(t1)
        t2 = asyncio.create_task(self.clean_old_data())
        self._tasks.append(t2)
        return self

    def stop(self):
        self.stop_event.set()
        for t in self._tasks:
            try:
                t.cancel()
            except Exception:
                pass
        self._tasks.clear()

    def Alarm_to_dict(self, alarm: Alarm) -> dict:
        return {
            "name": alarm.name,
            "level": alarm.level,
            "threshold": alarm.threshold,
            "protection": alarm.protection,
        }

    async def run(self):
        while not self.stop_event.is_set():
            try:
                data = self.equipment.latest_read.copy()
                data["ID"] = self.ID
                data["time"] = datetime.now().replace(microsecond=0)
                data["seen"] = False
                await self.mongo_db[self.collection].insert_one(data)
                logger.debug(f"{self.equipment.__class__.__name__} upload success")
            except asyncio.CancelledError:
                self.stop_event.set()
                break
            except Exception as e:
                logger.error(f"{self.equipment.__class__.__name__} upload failed: {e}")
            await asyncio.sleep(self.interval - asyncio.get_running_loop().time() % self.interval)

    async def clean_old_data(self):
        while not self.stop_event.is_set():
            try:
                # 清除1年前的數據
                delete_time = datetime.now() - timedelta(days=365)
                result = await self.mongo_db[self.collection].delete_many({"time": {"$lt": delete_time}})
                logger.info(f"Deleted {result.deleted_count} old data from {self.collection}")
            except asyncio.CancelledError:
                self.stop_event.set()
                break
            except Exception as e:
                logger.error(f"Failed to clean old data: {e}")

            # 每天固定清除一次
            now = datetime.now()
            next_run = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            wait_time = (next_run - now).total_seconds()
            await asyncio.sleep(wait_time)

# ================== Grid Controller ==================

class GridController:
    def __init__(self, pcs:SNPOWER_PCS, bess:CATL_BMS, mof:PM335, process_interval: int = 1):
        self.pcs = pcs
        self.bess = bess
        self.mof = mof
        self.process_interval = process_interval

        self.stop_event = asyncio.Event()
        self.strategy_stop_event = asyncio.Event()
        self.strategy_loop_task = None
        self.command_loop_task = None
        self.update_parameter_task = None

        self.system_limit = SystemLimit(max_p=0, min_p=0, max_q=0, min_q=0, max_soc=0, min_soc=0)
        self.last_command = Command(p_target=0, q_target=0)
        self.current_strategy = StopStrategy()

        self.soc = None
        self.pcs_status = None
        self.mbmu_status = None

    # ============ Public Methods ============
    def start(self):
        self._start_command_loop()
        self._start_strategy_loop()
        self._start_update_parameter_loop()
        return self

    def stop(self):
        self.stop_event.set()

    async def update_strategy(self, new_strategy: Strategy):
        if not isinstance(new_strategy, Strategy):
            logger.error(f"接收到未效的策略{new_strategy}，停止更新策略")
            return

        if isinstance(new_strategy, type(self.current_strategy)):
            # logger.info(f"新策略與當前策略相同，跳過本次更新 {self.current_strategy}")
            return

        logger.info(f"切換策略: {self.current_strategy} -> {new_strategy}")
        self.current_strategy = new_strategy
        await self._restart_strategy_loop()

    # ============ Private Methods ============

    def _start_update_parameter_loop(self):
        if self.update_parameter_task is None or self.update_parameter_task.done():
            self.update_parameter_task = asyncio.create_task(self.execute_update_parameter_loop())
            logger.info("開始更新參數循環")
            return
        logger.warning("更新參數循環已經開始，本次操作無效")

    def _start_command_loop(self):
        if self.command_loop_task is None or self.command_loop_task.done():
            self.command_loop_task = asyncio.create_task(self.execute_command_loop())
            logger.info("開始指令執行循環")
            return
        logger.warning("指令執行循環已經開始，本次操作無效")

    def _start_strategy_loop(self):
        if self.strategy_loop_task is None or self.strategy_loop_task.done():
            self.strategy_stop_event.clear()
            self.strategy_loop_task = asyncio.create_task(self.execute_strategy_loop())
            logger.info("開始策略執行循環")
            return
        logger.warning("策略執行循環已經開始，本次操作無效")

    def _stop_strategy_loop(self):
        if self.strategy_loop_task is not None and not self.strategy_loop_task.done():
            self.strategy_stop_event.set()
            logger.info("停止策略執行循環")
            return
        logger.warning("策略執行循環未開始，本次操作無效")

    async def _restart_strategy_loop(self):
        self._stop_strategy_loop()
        await self.strategy_loop_task
        self._start_strategy_loop()

    def _limit_p(self, p_target: int | float) -> int | float:
        """
        限制 p_command 的值根據 SOC 和目標值的方向。
        放電 (p > 0) 或 充電 (p < 0)。
        """
        if not isinstance(p_target, (int, float)):
            logger.error("接收到未效的 p_target，停止實功輸出")
            return 0

        if self.soc is None:
            logger.error("無法讀取到SOC，停止實功輸出")
            return 0

        if p_target > 0:
            if self.soc < self.system_limit.min_soc:
                logger.warning("SOC低於系統限制值，禁止放電")
                return 0
            # 限制放電功率不超過系統限制
            limited_p = min(p_target, self.system_limit.max_p)
            return limited_p
        elif p_target < 0:
            if self.soc > self.system_limit.max_soc:
                logger.warning("SOC高於系統限制值，禁止充電")
                return 0
            # 限制充電功率不超過系統限制
            limited_p = max(p_target, self.system_limit.min_p)
            return limited_p
        else:
            return 0

    def _limit_q(self, q_target: int | float) -> int | float:
        """
        限制 q_command 的值在 min_q 和 max_q 之間。
        """

        if not isinstance(q_target, (int, float)):
            logger.error("接收到未效的 q_target，停止無功輸出")
            return 0

        if self.soc is None:
            logger.error("無法讀取到SOC，停止無功輸出")
            return 0

        if q_target > self.system_limit.max_q:
            logger.warning("無功輸出超過系統限制，限制為最大值")
            return self.system_limit.max_q
        elif q_target < self.system_limit.min_q:
            logger.warning("無功輸出低於系統限制，限制為最小值")
            return self.system_limit.min_q
        else:
            return q_target

    # ============ Equipment Control Methods ============

    async def set_pcs_on(self):
        pcs_status = self.pcs_status
        mbmu_status = self.mbmu_status
        if pcs_status is None:
            logger.warning("無法讀取到PCS狀態，停止開啟PCS")
            return
        if mbmu_status is None:
            logger.warning("無法讀取到BESS狀態，停止開啟PCS")
            return
        
        if mbmu_status != 1:
            logger.warning("未開啟BESS，停止開啟PCS")
            return
        
        if pcs_status == 1:
            # logger.info("PCS已經開啟，本次操作無效")
            return
        if pcs_status == 2:
            logger.warning("PCS未就緒，暫停發送PCS開啟指令")
            return

        logger.info("開啟PCS")
        await self.pcs.set_pcs_on()

    async def set_pcs_off(self):
        pcs_status = self.pcs_status
        if pcs_status is None:
            logger.warning("無法讀取到PCS狀態，關閉PCS")
            await self.pcs.set_pcs_off()
            return
        if pcs_status == 0:
            # logger.info("PCS已經關閉，本次操作無效")
            return
        if pcs_status == 2:
            logger.warning("PCS未就緒，暫停發送PCS關閉指令")
            return

        logger.info("關閉PCS")
        await self.pcs.set_pcs_off()
        # self.last_command = Command(p_target=0, q_target=0)

    async def set_pcs_pq(self, p_target: int | float, q_target: int | float):
        logger.info(f"設定PCS P: {p_target}, Q: {q_target}")
        p_target = self._limit_p(p_target)
        q_target = self._limit_q(q_target)
        await self.pcs.set_pcs_pq(p_target, q_target)

    async def set_bess_on(self):
        mbmu_status = self.mbmu_status
        pcs_status = self.pcs_status
        if pcs_status is None:
            logger.warning("無法讀取到PCS狀態，停止開啟BESS")
            return
        if mbmu_status is None:
            logger.warning("無法讀取到BESS狀態，停止開啟BESS")
            return
        
        if mbmu_status == 1:
            # logger.info("BESS已經開啟，本次操作無效")
            return
        if pcs_status == 1:
            logger.warning("PCS未關閉，停止開啟BESS")
            return
        
        logger.info("開啟BESS")
        await self.bess.set_bess_on()

    async def set_bess_off(self):
        mbmu_status = self.mbmu_status
        pcs_status = self.pcs_status
        if pcs_status is None:
            logger.warning("無法讀取到PCS狀態，停止關閉BESS")
            return
        if pcs_status != 0:
            logger.warning("PCS未關閉或就緒，暫停發送BESS關閉指令")
            return
        if mbmu_status is None:
            logger.warning("無法讀取到BESS狀態，關閉BESS")
            await self.bess.set_bess_off()
            return
        if mbmu_status == 0:
            # logger.info("BESS已經關閉，本次操作無效")
            return
        
        logger.info("關閉BESS")
        await self.bess.set_bess_off()

    async def set_bess_reset(self):
        await self.bess.EMS_fault_clear()

    # ============ Core Methods ============

    async def execute_strategy_loop(self):
        first_run = True
        while not self.strategy_stop_event.is_set() and not self.stop_event.is_set():
            try:
                self.last_command = await asyncio.to_thread(self.current_strategy.execute, self.last_command)
                if first_run:
                    self.last_command = Command(p_target=0, q_target=0)
                    first_run = False
                logger.info(f"更新策略目標: {self.current_strategy}: {self.last_command}")

                # 根據策略執行週期進行等待
                if self.current_strategy.interval > 0:
                    try:
                        await asyncio.wait_for(self.strategy_stop_event.wait(), timeout=self.current_strategy.interval - (asyncio.get_running_loop().time()+0.1) % self.current_strategy.interval)
                        if self.strategy_stop_event.is_set():
                            break
                    except asyncio.TimeoutError:
                        continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Strategy failed: {e}")
                logger.exception(e)

    async def execute_command_loop(self):
        while not self.stop_event.is_set():
            try:

                soc = self.soc
                # ========= BESS Protection =================
                # BESS 存在需保護告警
                if self.bess.protection:
                    logger.warning(f"BESS存在告警，停止輸出")
                    # await asyncio.gather(
                    #     self.set_pcs_off(),
                    #     self.set_bess_off()
                    # )
                    await self.set_pcs_pq(0, 0)
                    await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    continue

                # ========= SOC Protection =================
                # SOC 讀取失敗達一定次數，輸出降為0
                if soc is None:
                    logger.warning("無法讀取到SOC，輸出降為0")
                    # await asyncio.gather(
                    #     self.set_pcs_off(),
                    #     self.set_bess_off()
                    # )
                    await self.set_pcs_pq(0, 0)
                    await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    continue

                # # ========= PCS Protection =================
                # # PCS 存在需保護告警
                if self.pcs.protection:
                    logger.warning(f"PCS存在告警，停止輸出")
                    await self.set_pcs_pq(0, 0)
                    await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    continue

                # ========= MOF Protection =================
                # MOF 讀取失敗達一定次數，進入保護模式
                if self.sum_load is None:
                    logger.warning("MOF斷線，停止輸出")
                    await self.set_pcs_pq(0, 0)
                    await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    continue

                # ========= Command =================

                # 讀取最新指令
                command = self.last_command
                p_target = command.p_target
                q_target = command.q_target
                # ========= Strategy =================
                # 停止策略
                if isinstance(self.current_strategy, StopStrategy):
                    logger.info("停止策略")
                    await self.set_pcs_pq(0, 0)
                    # await asyncio.gather(
                    #     self.set_pcs_off(),
                    #     self.set_bess_off()
                    # )
                    await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    continue

                # PQ模式策略
                if isinstance(self.current_strategy, PQModeStrategy):
                    if self.mbmu_status != 1 or self.pcs_status != 1:
                        logger.info("設備未搭接")
                        # await self.set_bess_on()
                        await self.set_pcs_pq(0, 0)                    
                        await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                        continue

                    p_target = await asyncio.to_thread(self._limit_p, p_target)
                    q_target = await asyncio.to_thread(self._limit_q, q_target)
                    # if p_target == 0 and q_target == 0:
                    #     logger.info("PQ模式: P和Q目標值均為0，關閉PCS")
                    #     await self.set_pcs_off()
                    #     await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    #     continue
                    logger.info(f"PQ模式: 設定P: {p_target}, Q: {q_target}")

                    # await self.set_pcs_on()
                    await self.set_pcs_pq(p_target, q_target)
                    await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    continue

                # 需量管理與負載轉移策略
                if isinstance(self.current_strategy, MDemand_LShiftStrategy):
                    if self.mbmu_status != 1 or self.pcs_status != 1:
                        logger.info("設備未搭接")
                        self.last_command = Command(p_target=0, q_target=0)
                        await self.set_pcs_pq(0, 0)  
                        await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                        continue

                    p_target = await asyncio.to_thread(self._limit_p, p_target)
                    q_target = await asyncio.to_thread(self._limit_q, q_target)
                    # if p_target == 0 and q_target == 0:
                    #     logger.info("需量管理與負載轉移策略: P和Q目標值均為0，關閉PCS")
                    #     await self.set_pcs_off()
                    #     await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    #     continue
                    logger.info(f"需量管理與負載轉移策略: 設定P: {p_target}, Q: {q_target}")

                    # await self.set_pcs_on()
                    await self.set_pcs_pq(p_target, q_target)
                    # await self.set_pcs_pq(0, 0)
                    await asyncio.sleep(self.process_interval - asyncio.get_running_loop().time() % self.process_interval)
                    continue

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Control failed: {e}")
            finally:
                await asyncio.sleep(0)

    async def execute_update_parameter_loop(self):
        while not self.stop_event.is_set():
            try:
                # 讀取系統參數
                if (mbmu_status := self.bess.latest_read.get("BMS_power_on")) is not None: # 1: ON, 0: OFF
                    self.mbmu_status = mbmu_status
                if (soc := self.bess.latest_read.get("soc")) is not None:
                    self.soc = soc
                    MDemand_LShiftStrategy().soc = self.soc
                if (pcs_mode1 := self.pcs.latest_read.get("mode1")) is not None: # bit 1=1: ON, bit 1=0: OFF
                    # self.pcs_status_bit5 = pcs_mode1 & 0b100000 # bit 5
                    # self.pcs_status_bit6 = pcs_mode1 & 0b1111111000000 # bit 6
                    # if self.pcs_status_bit5 == 0 and self.pcs_status_bit6 > 0:
                    #     self.pcs_status = 1 # 1: ON
                    # elif self.pcs_status_bit5 > 0 and self.pcs_status_bit6 == 0:
                    #     self.pcs_status = 0 # 2: OFF
                    # else:
                    #     self.pcs_status = 2 # 未就緒
                    self.pcs_status_bit1 = pcs_mode1 & 0b10 # bit 1
                    if self.pcs_status_bit1 > 0:
                        self.pcs_status = 1
                    else:
                        self.pcs_status = 0

                if (sum_load := self.mof.latest_read.get("p")) is not None:
                    self.sum_load = sum_load
                if (pcs_load := self.pcs.latest_read.get("p")) is not None:
                    self.pcs_load = pcs_load

                MDemand_LShiftStrategy().current_load = -self.sum_load


                logger.info(f"更新參數:SOC={self.soc}, PCS狀態={self.pcs_status}, 儲能狀態={self.mbmu_status}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Update parameter failed: {e}")
            finally:
                # logger.warning(f"2 {asyncio.get_running_loop().time()}")
                await asyncio.sleep(self.process_interval - (asyncio.get_running_loop().time()+0.2) % self.process_interval)

# async def handle_input(stop_event, pcs, bess):
#     while not stop_event.is_set():
#         try:
#             device_choice = await asyncio.get_running_loop().run_in_executor(None, input, "請選擇設備(1. PCS, 2. BESS, 5. TestCurve)：")

#             if device_choice == "1":
#                 pcs_command = await asyncio.get_running_loop().run_in_executor(None, input, "請輸入指令(0. 開機, 1. 關機, 2. PQ)：")
#                 if pcs_command == "0":
#                     await pcs.set_pcs_on()
#                 elif pcs_command == "1":
#                     await pcs.set_pcs_off()
#                 elif pcs_command == "2":
#                     p = await asyncio.get_running_loop().run_in_executor(None, input, "請輸入P：")
#                     q = await asyncio.get_running_loop().run_in_executor(None, input, "請輸入Q：")
#                     await pcs.set_pcs_pq(float(p), float(q))
#             elif device_choice == "2":
#                 bess_command = await asyncio.get_running_loop().run_in_executor(None, input, "請輸入指令(0. 開機, 1. 關機)：")
#                 if bess_command == "0":
#                     await bess.set_bess_on()
#                 elif bess_command == "1":
#                     await bess.set_bess_off()
#             # elif device_choice == "3":
#             #     dc1_command = await asyncio.get_running_loop().run_in_executor(None, input, "請輸入指令(0. 開機, 1. 關機)：")
#             #     if dc1_command == "0":
#             #         await dc1.set_dc_on()
#             #     elif dc1_command == "1":
#             #         await dc1.set_dc_off()
#             # elif device_choice == "4":
#             #     dc2_command = await asyncio.get_running_loop().run_in_executor(None, input, "請輸入指令(0. 開機, 1. 關機)：")
#             #     if dc2_command == "0":
#             #         await dc2.set_dc_on()
#             #     elif dc2_command == "1":
#             #         await dc2.set_dc_off()
#             # elif device_choice == "5":
#             #     test_curve = await asyncio.get_running_loop().run_in_executor(None, input, "請輸入指令(1. 功率精確度測試, 2. 功率因數調整測試, 3. 額定功率持續放電, 4.額定功率持續充電, 5. 步階輸出/輸入功率測試)：")
#             #     if test_curve == "1":
#             #         # while not pcs.latest_read.get("status") == 1:
#             #         #     await pcs.set_pcs_on()
#             #         #     await asyncio.sleep(1)
#             #         #     continue

#             #         for i in range(10):
#             #             await pcs.set_pcs_pq(50+50*i,0)
#             #             await asyncio.sleep(20)
#             #         for i in range(10):
#             #             await pcs.set_pcs_pq(500-50*i,0)
#             #             await asyncio.sleep(20)
#             #         await pcs.set_pcs_off()
#             #     elif test_curve == "2":
#             #         # while not pcs.latest_read.get("status") == 1:
#             #         #     await pcs.set_pcs_on()
#             #         #     await asyncio.sleep(1)
#             #         #     continue

#             #         # await pcs.set_pcs_pq(50.0,497.5)
#             #         # await asyncio.sleep(60)
#             #         # await pcs.set_pcs_pq(100.0,489.9)
#             #         # await asyncio.sleep(60)
#             #         # await pcs.set_pcs_pq(150.0,474.3)
#             #         # await asyncio.sleep(60)
#             #         # await pcs.set_pcs_pq(200.0,447.2)
#             #         # await asyncio.sleep(60)
#             #         await pcs.set_pcs_pq(250.0,433.0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(300.0,400.0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(350.0,357.1)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(400.0,300.0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(450.0,218.0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(400,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(350,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(300,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(250,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(200,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(150,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(100,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(50,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_off()
#             #     elif test_curve == "3":
#             #         await pcs.set_pcs_pq(50,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(100,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(200,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(300,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(400,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(500,0)
#             #         await asyncio.sleep(900)
#             #         await pcs.set_pcs_pq(400,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(300,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(200,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(100,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_off()
#             #     elif test_curve == "4":
#             #         await pcs.set_pcs_pq(-50,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(-100,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(-200,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(-300,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(-400,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(-500,0)
#             #         await asyncio.sleep(900)
#             #         await pcs.set_pcs_pq(-400,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(-300,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(-200,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_pq(-100,0)
#             #         await asyncio.sleep(10)
#             #         await pcs.set_pcs_off()
#             #     elif test_curve == "5":
#             #         await pcs.set_pcs_pq(-45,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(45,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(-45,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(45,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(-115,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(115,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(-240,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(240,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(-395,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(395,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(-500,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(500,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(-500,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(500,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(-500,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(500,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(-500,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(500,0)
#             #         await asyncio.sleep(30)
#             #         await pcs.set_pcs_pq(0,0)
#             #         await asyncio.sleep(30)

#             #         await pcs.set_pcs_off()

#         except asyncio.CancelledError:
#             break
#         except Exception as e:
#             logger.error(f"Input failed: {e}")

async def handle_strategy_parameter(
        stop_event: asyncio.Event, 
        bypass_strategy_event: asyncio.Event, 
        mongo_db: AsyncIOMotorDatabase, 
        grid_controller: GridController, 
        pcs_db,
        bess_db,
        process_interval: int):
    logger.info("開始更新策略參數")
    while not stop_event.is_set():
        if bypass_strategy_event.is_set():
            await grid_controller.update_strategy(StopStrategy())
            logger.error("EMS 斷線，策略停止")
            await asyncio.sleep(process_interval - asyncio.get_running_loop().time() % process_interval)
            continue
        try:
            site_control_data = await mongo_db['site_control'].find_one({}, sort=[('time', -1)])
            if not site_control_data:
                site_control = SiteControl.get_default()
            else:
                site_control = SiteControl.from_dict(DBFieldAdapter.convert(site_control_data))
            pcs_control = await mongo_db['eqpt_control'].find_one({
                    "ID": str(pcs_db['_id']),
                    # "time": {"$gt": datetime.now() - timedelta(seconds=10)},
                    # "seen": False,
                    },
                    # {"$set": {"seen": True}}
                    sort=[('time', -1)]
                )
            if pcs_control:
                # logger.info("pcs_control 接收資料 : " + str(pcs_control.get("control")))
                if pcs_control.get("control") == 1:
                    await grid_controller.set_pcs_on()
                elif pcs_control.get("control") == 0:
                    await grid_controller.set_pcs_off()
            bess_control = await mongo_db['eqpt_control'].find_one({
                    "ID": str(bess_db['_id']),
                    # "time": {"$gt": datetime.now() - timedelta(seconds=10)},
                    # "$or": [
                    #     {"seen": False},
                    #     {"seen": {"$exists": False}}
                    # ],
                    },
                    # {"$set": {"seen": True}}
                    sort=[('time', -1)]
                )
            if bess_control:
                # logger.info("bess_control 接收資料 : " + str(bess_control.get("control")))
                if bess_control.get("control") == 1:
                    await grid_controller.set_bess_on()
                if bess_control.get("control") == 0:
                    await grid_controller.set_bess_off()
                if bess_control.get("reset") == 1:
                    await grid_controller.set_bess_reset()
                    await mongo_db['eqpt_control'].insert_one({
                            "ID": str(bess_db['_id']),
                            "control": 0,
                            "reset": 0,
                            "seen": True,
                            "time": datetime.now(),
                        })

            grid_controller.system_limit = site_control.system_limit
            PQModeStrategy().parameter = site_control.pq_mode
            MDemand_LShiftStrategy().parameter = site_control.mdemand_lshift
            MDemand_LShiftStrategy().min_soc = site_control.system_limit.min_soc
            MDemand_LShiftStrategy().max_soc = site_control.system_limit.max_soc
            MDemand_LShiftStrategy().min_p = site_control.system_limit.min_p
            MDemand_LShiftStrategy().max_p = site_control.system_limit.max_p
            MDemand_LShiftStrategy().bess_capacity = 407
            MDemand_LShiftStrategy().gc_interval = grid_controller.process_interval
            
            if pcs_control is None or bess_control is None:
                logger.warning("未接收到設備控制資料，停止策略")
                await grid_controller.update_strategy(StopStrategy())
            elif pcs_control.get("control") == 0 or bess_control.get("control") == 0:
                logger.warning("設備控制為關閉，停止策略")
                await grid_controller.update_strategy(StopStrategy())
            elif site_control.mode == SiteControlMode.STOP:
                await grid_controller.update_strategy(StopStrategy())
            elif site_control.mode == SiteControlMode.PQMode:
                await grid_controller.update_strategy(PQModeStrategy())
            elif site_control.mode == SiteControlMode.MDemand_LShift:
                await grid_controller.update_strategy(MDemand_LShiftStrategy())
            elif site_control.mode == SiteControlMode.Schedule:
                await grid_controller.update_strategy(StopStrategy())
            else:
                await grid_controller.update_strategy(StopStrategy())

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Strategy parameter failed: {e}")
        finally:
            await asyncio.sleep(process_interval - asyncio.get_running_loop().time() % process_interval)

# async def handle_control(
#         stop_event: asyncio.Event,
#         mongo_db:AsyncIOMotorDatabase,
#         pcs_id: str,
#         bess_id: str,
#         grid_controller: GridController, 
#         process_interval: int):
    
#     while not stop_event.is_set():
#         try:
#             pcs_control = await mongo_db['eqpt_control'].find_one_and_update({
#                     "ID": pcs_id,
#                     "time": {"$gt": datetime.now() - timedelta(seconds=10)},
#                     "seen": False
#                 }, {"$set": {"seen": True}})
#             if pcs_control:
#                 logger.info("pcs_control 接收資料 : " + pcs_control.get("control"))
#                 if pcs_control.get("control") == 1:
#                     await grid_controller.set_pcs_on()
#                 elif pcs_control.get("control") == 0:
#                     await grid_controller.set_pcs_off()

#             bess_control = await mongo_db['eqpt_control'].find_one_and_update({
#                     "ID": bess_id,
#                     "time": {"$gt": datetime.now() - timedelta(seconds=10)},
#                     "seen": False
#                 }, {"$set": {"seen": True}})
#             if bess_control:
#                 logger.info("bess_control 接收資料 : " + bess_control.get("control"))
#                 if bess_control.get("control") == 1:
#                     await grid_controller.set_bess_on()
#                 if bess_control.get("control") == 0:
#                     await grid_controller.set_bess_off()
#                 if bess_control.get("reset") == 1:
#                     await grid_controller.set_bess_reset()
#                     await mongo_db['eqpt_control'].insert_one({
#                             "ID": bess_id,
#                             "control": 0,
#                             "reset": 0,
#                             "time": datetime.now(),
#                         })


#         except asyncio.CancelledError:
#             break
#         except Exception as e:
#             logger.error(f"Control failed: {e}")
#         finally:
#             await asyncio.sleep(process_interval - asyncio.get_running_loop().time() % process_interval)

async def handle_heartbeat(stop_event: asyncio.Event, bypass_strategy_event: asyncio.Event, mongo_db: AsyncIOMotorDatabase, grid_controller: GridController, process_interval: int):
    EMS_time = datetime.now()
    while not stop_event.is_set():
        try:
            # 接收HB
            # hb = await mongo_db['heartbeat'].find_one(
            #     {"EMS_time": {"$exists": True}},  # 確保查詢的記錄包含 EMS==True
            #     sort=[('_id', -1)]
            # )
            # if hb:
            #     EMS_time = hb.get("EMS_time")

            # # if (datetime.now() - EMS_time) > timedelta(seconds=30):
            # #     if not bypass_strategy_event.is_set():
            # #         bypass_strategy_event.set()
            # else:
            #     if bypass_strategy_event.is_set():
            #         bypass_strategy_event.clear()

            # 提供HB
            await mongo_db['pc_info'].insert_one({
                "time": datetime.now(),
            })
            await asyncio.sleep(process_interval - asyncio.get_running_loop().time() % process_interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Heartbeat failed: {e}")

async def _main_loop(stop_event, base_interval):
    while not stop_event.is_set():
            print ("------------------------------------------------")
            await asyncio.sleep(base_interval - (asyncio.get_running_loop().time()+0.4) % base_interval)

async def main():
    stop_event = asyncio.Event()
    bypass_strategy_event = asyncio.Event()
    try:
        # 受控連線池與逾時，避免長時運行造成 FD 壓力
        mongo_client = AsyncIOMotorClient(
            "mongodb://root:PaSsw0rd@localhost:27017",
            maxPoolSize=50, minPoolSize=0,
            socketTimeoutMS=20000, connectTimeoutMS=20000,
            retryWrites=False,
        )
        # 註冊 atexit，確保非預期結束時釋放 Mongo 資源
        atexit.register(lambda: mongo_client.close())
        # mongo_client = AsyncIOMotorClient("mongodb://root:PaSs0rd@192.168.1.2:27017") #root:pc152@
        # mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
        db = mongo_client["EMS"]
        base_interval = 1

        db_equipment_bess = await db['equipment'].find_one({"type": "bms"})
        db_equipment_pcs = await db['equipment'].find_one({"type": "pcs"})
        db_equipment_totm = await db['equipment'].find_one({"type": "acm", "num": 0})
        db_equipment_acm1 = await db['equipment'].find_one({"type": "acm", "num": 1})
        db_equipment_acm2 = await db['equipment'].find_one({"type": "acm", "num": 2})
        db_equipment_acm3 = await db['equipment'].find_one({"type": "acm", "num": 3})
        db_equipment_dcm = await db['equipment'].find_one({"type": "dcm"})
        db_equipment_io = await db['equipment'].find_one({"type": "rio"})

        totm = PM335(interval=db_equipment_totm['interval'], disconnect_threshold=5).set_client(AsyncModbusTcpClient(host="192.168.1.50")).set_slave_id(1).start()
        pcs = SNPOWER_PCS(interval=db_equipment_totm['interval'], disconnect_threshold=5).set_client(AsyncModbusTcpClient(host="192.168.1.60")).set_slave_id(1).start()
        bess = CATL_BMS(module_num=8, cell_num=52, interval=db_equipment_bess['interval'], disconnect_threshold=300).set_client(AsyncModbusTcpClient(host="192.168.1.70",timeout=0.3)).set_slave_id(1).start()
        acm1 = SE4900(interval=db_equipment_acm1['interval'], disconnect_threshold=5).set_client(AsyncModbusTcpClient(host="192.168.1.81")).set_slave_id(1).start()
        acm2 = SE4900(interval=db_equipment_acm2['interval'], disconnect_threshold=5).set_client(AsyncModbusTcpClient(host="192.168.1.81")).set_slave_id(2).start()
        acm3 = SE4900(interval=db_equipment_acm3['interval'], disconnect_threshold=5).set_client(AsyncModbusTcpClient(host="192.168.1.81")).set_slave_id(3).start()
        dcm = VAW(interval=db_equipment_dcm['interval'], disconnect_threshold=5).set_client(AsyncModbusTcpClient(host="192.168.1.81")).set_slave_id(4).start()
        io = ET7x00_RIO(interval=db_equipment_io['interval'], disconnect_threshold=5).set_client(AsyncModbusTcpClient(host="192.168.1.80")).set_slave_id(1).start()
        
        grid_controller = GridController(pcs, bess, totm, process_interval=base_interval).start()        
        # input_task = asyncio.create_task(handle_input(stop_event, pcs, bess))
        
        uploader_pcs = MongoUploader(db, pcs, str(db_equipment_pcs['_id']), "pcs", db_equipment_pcs['interval']).start()
        uploader_bess = MongoUploader(db, bess, str(db_equipment_bess['_id']), "bms", db_equipment_bess['interval']).start()
        uploader_totm = MongoUploader(db, totm, str(db_equipment_totm['_id']), "acm", db_equipment_totm['interval']).start()
        uploader_acm1 = MongoUploader(db, acm1, str(db_equipment_acm1['_id']), "acm", db_equipment_acm1['interval']).start()
        uploader_acm2 = MongoUploader(db, acm2, str(db_equipment_acm2['_id']), "acm", db_equipment_acm2['interval']).start()
        uploader_acm3 = MongoUploader(db, acm3, str(db_equipment_acm3['_id']), "acm", db_equipment_acm3['interval']).start()
        uploader_dcm = MongoUploader(db, dcm, str(db_equipment_dcm['_id']), "dcm", db_equipment_dcm['interval']).start()
        uploader_io = MongoUploader(db, io, str(db_equipment_io['_id']), "rio", db_equipment_io['interval']).start()

        # systemd/docker 傳入 SIGTERM 觸發關閉
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                # Windows 或不支援訊號時忽略
                pass

        await asyncio.gather(
            handle_heartbeat(stop_event, bypass_strategy_event, db, grid_controller, process_interval=5),
            handle_strategy_parameter(stop_event, bypass_strategy_event, db, grid_controller, db_equipment_pcs, db_equipment_bess, process_interval=base_interval),
            # run_server_manager(stop_event, db, totm, dcm, pcs, bess),
            _main_loop(stop_event, base_interval),
        )
        
    except asyncio.CancelledError:
        pass
    finally:
        stop_event.set()
        if locals().get("grid_controller"):
            grid_controller.stop()
        if locals().get("pcs"):
            pcs.stop()
        if locals().get("bess"):
            bess.stop()
        if locals().get("totm"):
            totm.stop()
        if locals().get("dcm"):
            dcm.stop()
        if locals().get("acm1"):
            acm1.stop()
        if locals().get("acm2"):
            acm2.stop()
        if locals().get("acm3"):
            acm3.stop()
        if locals().get("uploader_pcs"):
            uploader_pcs.stop()
        if locals().get("uploader_bess"):
            uploader_bess.stop()
        if locals().get("uploader_totm"):
            uploader_totm.stop()
        if locals().get("uploader_dcm"):
            uploader_dcm.stop()
        if locals().get("uploader_acm1"):
            uploader_acm1.stop()
        if locals().get("uploader_acm2"):
            uploader_acm2.stop()
        if locals().get("uploader_acm3"):
            uploader_acm3.stop()
        if locals().get("uploader_io"):
            uploader_io.stop()
        if locals().get("mongo_client"):
            try:
                mongo_client.close()
            except Exception:
                pass

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass