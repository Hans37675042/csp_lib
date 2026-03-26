"""
GC Main — Grid Controller 主程式

使用 csp_lib v0.4.2 API 實現與 Referance.py 等效的 GC 功能：
  - 8 台設備 Modbus 輪詢（PCS / BMS / 總電表 / 分支電表 / DC電表 / RIO）
  - BMS 心跳（0-15 遞增，1 秒週期）
  - 三種策略：Stop / PQMode / MDemandLShift
  - MongoDB 策略切換（site_control collection）
  - BMS 設備控制與 fault clear（eqpt_control collection）
  - GC 心跳上傳（pc_info collection）
  - 所有設備資料上傳至 MongoDB（DataUploadManager）
"""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient

from csp_lib import logger
from csp_lib.controller.strategies import PQModeConfig, PQModeStrategy, StopStrategy
from csp_lib.controller.system import ModePriority
from csp_lib.equipment.device import AsyncModbusDevice
from csp_lib.integration import (
    CommandMapping,
    ContextMapping,
    DeviceRegistry,
    HeartbeatMapping,
    HeartbeatMode,
    SystemController,
    SystemControllerConfig,
)
from csp_lib.manager import DataUploadManager
from csp_lib.modbus import ModbusTcpConfig
from csp_lib.mongo import MongoBatchUploader

from devices.CATL_bms_V0_0_0 import CATL_bms_read_points, CATL_bms_write_points
from devices.ET7x00_rio_V0_0_0 import ET7x00_rio_read_points
from devices.PM335_pm_V0_0_0 import PM335_pm_read_points
from devices.SE4900_pm_V0_0_0 import SE4900_pm_read_points
from devices.SNPOWER_pcs_V0_0_0 import SNPOWER_pcs_read_points, SNPOWER_pcs_write_points
from devices.VAW_dcm_V0_0_0 import VAW_dcm_read_points
from strategies.mdemand_lshift_strategy import MDemandLShiftParam, MDemandLShiftStrategy

# ─── 環境變數設定 ─────────────────────────────────────────────────────────────

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://root:PaSsw0rd@localhost:27017")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "EMS")

PCS_HOST = os.environ.get("PCS_HOST", "0.0.0.0")
BMS_HOST = os.environ.get("BMS_HOST", "0.0.0.0")
TOTM_HOST = os.environ.get("TOTM_HOST", "0.0.0.0")
ACM_HOST = os.environ.get("ACM_HOST", "0.0.0.0")
RIO_HOST = os.environ.get("RIO_HOST", "0.0.0.0")

# CATL BMS 電池配置（依現場實際設定）
BMS_MODULE_NUM = int(os.environ.get("BMS_MODULE_NUM", "8"))
BMS_CELL_NUM = int(os.environ.get("BMS_CELL_NUM", "52"))

# BMS 電池容量（kWh），用於 MDemandLShift 策略
BESS_CAPACITY_KWH = float(os.environ.get("BESS_CAPACITY_KWH", "407"))


# ─── 設備建立 ─────────────────────────────────────────────────────────────────

def _build_devices() -> tuple[
    AsyncModbusDevice,  # pcs
    AsyncModbusDevice,  # bms
    AsyncModbusDevice,  # totm
    AsyncModbusDevice,  # acm1
    AsyncModbusDevice,  # acm2
    AsyncModbusDevice,  # acm3
    AsyncModbusDevice,  # dcm
    AsyncModbusDevice,  # rio
]:
    pcs = AsyncModbusDevice(
        device_id="pcs_01",
        config=ModbusTcpConfig(host=PCS_HOST, port=5020),
        slave_id=1,
        read_points=SNPOWER_pcs_read_points,
        write_points=SNPOWER_pcs_write_points,
    )
    bms = AsyncModbusDevice(
        device_id="bms_01",
        config=ModbusTcpConfig(host=BMS_HOST, port=5020, timeout=0.3),
        slave_id=2,
        read_points=CATL_bms_read_points,
        write_points=CATL_bms_write_points,
    )
    totm = AsyncModbusDevice(
        device_id="totm_01",
        config=ModbusTcpConfig(host=TOTM_HOST, port=5020),
        slave_id=3,
        read_points=PM335_pm_read_points,
    )
    acm1 = AsyncModbusDevice(
        device_id="acm_01",
        config=ModbusTcpConfig(host=ACM_HOST, port=5020),
        slave_id=4,
        read_points=SE4900_pm_read_points,
    )
    acm2 = AsyncModbusDevice(
        device_id="acm_02",
        config=ModbusTcpConfig(host=ACM_HOST, port=5020),
        slave_id=5,
        read_points=SE4900_pm_read_points,
    )
    acm3 = AsyncModbusDevice(
        device_id="acm_03",
        config=ModbusTcpConfig(host=ACM_HOST, port=5020),
        slave_id=6,
        read_points=SE4900_pm_read_points,
    )
    dcm = AsyncModbusDevice(
        device_id="dcm_01",
        config=ModbusTcpConfig(host=ACM_HOST, port=5020),
        slave_id=7,
        read_points=VAW_dcm_read_points,
    )
    rio = AsyncModbusDevice(
        device_id="rio_01",
        config=ModbusTcpConfig(host=RIO_HOST, port=5020),
        slave_id=8,
        read_points=ET7x00_rio_read_points,
    )
    return pcs, bms, totm, acm1, acm2, acm3, dcm, rio


# ─── MongoDB 策略切換輪詢 ──────────────────────────────────────────────────────

async def poll_strategy_switch(
    db,
    controller: SystemController,
    mdemand_strategy: MDemandLShiftStrategy,
    db_equipment_pcs: dict,
    db_equipment_bess: dict,
) -> None:
    """
    每秒輪詢 site_control collection，同步策略參數與模式切換。
    同步邏輯對應 Referance.py handle_strategy_parameter()。
    """
    logger.info("Strategy switch polling started")
    while True:
        try:
            # ── 讀取 site_control ──
            site_doc = await db["site_control"].find_one({}, sort=[("time", -1)])

            # ── 讀取設備控制（PCS / BMS）──
            pcs_ctrl = await db["eqpt_control"].find_one(
                {"ID": str(db_equipment_pcs["_id"])}, sort=[("time", -1)]
            )
            bess_ctrl = await db["eqpt_control"].find_one(
                {"ID": str(db_equipment_bess["_id"])}, sort=[("time", -1)]
            )

            # 無設備控制文件 → 停機保護
            if pcs_ctrl is None or bess_ctrl is None:
                logger.warning("No equipment control doc found, forcing stop strategy")
                await controller.set_base_mode("stop")
                await asyncio.sleep(1)
                continue

            # 設備關閉 → 停機保護
            if pcs_ctrl.get("control") == 0 or bess_ctrl.get("control") == 0:
                logger.warning("Equipment control=0, forcing stop strategy")
                await controller.set_base_mode("stop")
                await asyncio.sleep(1)
                continue

            # ── 同步策略參數 ──
            if site_doc:
                mode_str = site_doc.get("mode", "stop")

                # PQ 模式參數更新
                pq_p = float(site_doc.get("pq_p", site_doc.get("p", 0)))
                pq_q = float(site_doc.get("pq_q", site_doc.get("q", 0)))
                await controller._mode_manager.update_mode_strategy(
                    "pq",
                    PQModeStrategy(PQModeConfig(p=pq_p, q=pq_q)),
                )

                # MDemandLShift 參數更新（從 site_doc 重建 MDemandLShiftParam）
                param = _build_mdemand_param(site_doc, bess_capacity=BESS_CAPACITY_KWH)
                mdemand_strategy.set_parameter(param)

                # 模式切換
                if mode_str == "pq":
                    await controller.set_base_mode("pq")
                elif mode_str in ("mdemand_lshift", "MDemand_LShift"):
                    await controller.set_base_mode("mdemand_lshift")
                else:
                    # stop / schedule / unknown → 停機
                    await controller.set_base_mode("stop")
            else:
                await controller.set_base_mode("stop")

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception(f"Strategy switch polling error: {exc}")

        await asyncio.sleep(1)


def _build_mdemand_param(site_doc: dict, bess_capacity: float) -> MDemandLShiftParam:
    """從 site_control MongoDB 文件重建 MDemandLShiftParam。"""
    # system_limit 欄位
    sys_limit = site_doc.get("system_limit", {})
    # mdemand_lshift 欄位（直接或巢狀）
    md = site_doc.get("mdemand_lshift", site_doc)

    return MDemandLShiftParam(
        # 業務參數
        contract_capacity=float(md.get("contract_capacity", 0)),
        PeakLoad_reserve=float(md.get("PeakLoad_reserve", md.get("peak_load_reserve", 0))),
        dead_zone_soc=float(md.get("dead_zone_soc", 0)),
        # 系統限制
        min_soc=float(sys_limit.get("min_soc", md.get("min_soc", 10))),
        max_soc=float(sys_limit.get("max_soc", md.get("max_soc", 90))),
        min_p=float(sys_limit.get("min_p", md.get("min_p", -2400))),
        max_p=float(sys_limit.get("max_p", md.get("max_p", 2400))),
        bess_capacity=bess_capacity,
        gc_interval=1,
    )


# ─── BMS 設備控制輪詢 ──────────────────────────────────────────────────────────

async def poll_bms_control(db, bms: AsyncModbusDevice, bms_id: str) -> None:
    """
    每秒輪詢 eqpt_control collection，處理 BMS 開關與 fault clear。
    對應 Referance.py handle_strategy_parameter() 中的 bess_control 邏輯。
    """
    logger.info("BMS control polling started")
    while True:
        try:
            bess_ctrl = await db["eqpt_control"].find_one(
                {"ID": bms_id}, sort=[("time", -1)]
            )
            if bess_ctrl:
                # BMS 開/關
                if bess_ctrl.get("control") == 1:
                    await bms.write("BMS_on_off", 2)   # 2 = ON
                elif bess_ctrl.get("control") == 0:
                    await bms.write("BMS_on_off", 3)   # 3 = OFF

                # Fault clear（寫 1 再寫 0）
                if bess_ctrl.get("reset") == 1:
                    await bms.write("BMS_fault_clear", 1)
                    await asyncio.sleep(0.5)
                    await bms.write("BMS_fault_clear", 0)
                    await db["eqpt_control"].insert_one({
                        "ID": bms_id,
                        "control": bess_ctrl.get("control", 0),
                        "reset": 0,
                        "seen": True,
                        "time": datetime.now(),
                    })

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception(f"BMS control polling error: {exc}")

        await asyncio.sleep(1)


# ─── GC 心跳上傳 ──────────────────────────────────────────────────────────────

async def poll_gc_heartbeat(db) -> None:
    """
    每 5 秒寫入 pc_info collection，對應 Referance.py handle_heartbeat()。
    """
    logger.info("GC heartbeat polling started")
    while True:
        try:
            await db["pc_info"].insert_one({"time": datetime.now()})
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception(f"GC heartbeat error: {exc}")

        await asyncio.sleep(5)


# ─── 主程式 ──────────────────────────────────────────────────────────────────

async def main() -> None:
    stop_event = asyncio.Event()
    mongo_client = None
    uploader: MongoBatchUploader | None = None

    try:
        # ── MongoDB 連線 ──
        mongo_client = AsyncIOMotorClient(
            MONGO_URI,
            maxPoolSize=50,
            minPoolSize=0,
            socketTimeoutMS=20000,
            connectTimeoutMS=20000,
            retryWrites=False,
        )
        db = mongo_client[MONGO_DB_NAME]

        # ── 查詢設備 DB 文件（用於 DataUploadManager ID 與 eqpt_control 查詢）──
        db_equipment_pcs = await db["equipment"].find_one({"type": "pcs"}) or {}
        db_equipment_bess = await db["equipment"].find_one({"type": "bms"}) or {}

        # ── 建立設備實例 ──
        pcs, bms, totm, acm1, acm2, acm3, dcm, rio = _build_devices()

        # ── DeviceRegistry ──
        registry = DeviceRegistry()
        registry.register(pcs, traits=["pcs"])
        registry.register(bms, traits=["bms"])
        registry.register(totm, traits=["totm"])
        registry.register(acm1, traits=["acm"])
        registry.register(acm2, traits=["acm"])
        registry.register(acm3, traits=["acm"])
        registry.register(dcm, traits=["dcm"])
        registry.register(rio, traits=["rio"])

        # ── ContextMappings ──
        # soc: bms.soc → context.soc
        # current_load: totm.p → context.extra["current_load"]（策略內取負號）
        # pcs_status_raw: pcs.mode1 → context.extra["pcs_status_raw"]
        # bms_power_on: bms.BMS_power_on → context.extra["bms_power_on"]
        context_mappings = [
            ContextMapping(point_name="soc",          context_field="soc",                  trait="bms"),
            ContextMapping(point_name="p",            context_field="extra.current_load",   trait="totm"),
            ContextMapping(point_name="mode1",        context_field="extra.pcs_status_raw", trait="pcs"),
            ContextMapping(point_name="BMS_power_on", context_field="extra.bms_power_on",   trait="bms"),
        ]

        # ── CommandMappings ──
        # p_target → pcs.PQ_p_ref, q_target → pcs.PQ_q_ref
        command_mappings = [
            CommandMapping(command_field="p_target", point_name="PQ_p_ref", trait="pcs"),
            CommandMapping(command_field="q_target", point_name="PQ_q_ref", trait="pcs"),
        ]

        # ── SystemControllerConfig（含 BMS 心跳）──
        # BMS 心跳：0-15 遞增，1 秒週期，對應 Referance._EMS_HB_update()
        sc_config = SystemControllerConfig(
            context_mappings=context_mappings,
            command_mappings=command_mappings,
            heartbeat_mappings=[
                HeartbeatMapping(
                    point_name="BMS_heartbeat",
                    trait="bms",
                    mode=HeartbeatMode.INCREMENT,
                    increment_max=15,
                )
            ],
            heartbeat_interval=1.0,
        )

        # ── SystemController ──
        controller = SystemController(registry, sc_config)
        mdemand_strategy = MDemandLShiftStrategy()

        controller.register_mode("stop",            StopStrategy(),                       ModePriority.SCHEDULE)
        controller.register_mode("pq",              PQModeStrategy(PQModeConfig(0, 0)),   ModePriority.SCHEDULE)
        controller.register_mode("mdemand_lshift",  mdemand_strategy,                     ModePriority.SCHEDULE)
        await controller.set_base_mode("stop")

        # ── MongoDB 上傳器 ──
        uploader = MongoBatchUploader(db)
        uploader.start()

        data_manager = DataUploadManager(uploader)
        data_manager.subscribe(pcs,   collection_name="pcs")
        data_manager.subscribe(bms,   collection_name="bms")
        data_manager.subscribe(totm,  collection_name="acm")
        data_manager.subscribe(acm1,  collection_name="acm")
        data_manager.subscribe(acm2,  collection_name="acm")
        data_manager.subscribe(acm3,  collection_name="acm")
        data_manager.subscribe(dcm,   collection_name="dcm")
        data_manager.subscribe(rio,   collection_name="rio")

        # ── OS 訊號處理（systemd / SIGTERM）──
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except (NotImplementedError, AttributeError):
                # Windows 或不支援訊號時忽略
                pass

        # ── 啟動設備與控制器 ──
        logger.info("Starting all devices...")
        await asyncio.gather(*(dev.start() for dev in (pcs, bms, totm, acm1, acm2, acm3, dcm, rio)))

        bms_id = str(db_equipment_bess.get("_id", ""))

        logger.info("Starting SystemController...")
        async with controller:
            await asyncio.gather(
                _wait_for_stop(stop_event),
                poll_strategy_switch(db, controller, mdemand_strategy, db_equipment_pcs, db_equipment_bess),
                poll_bms_control(db, bms, bms_id),
                poll_gc_heartbeat(db),
            )

    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.exception(f"GC main error: {exc}")
    finally:
        stop_event.set()
        logger.info("Shutting down...")

        # 停止所有設備
        for dev in ("pcs", "bms", "totm", "acm1", "acm2", "acm3", "dcm", "rio"):
            obj = locals().get(dev)
            if obj is not None:
                try:
                    await obj.stop()
                except Exception:
                    pass

        # 停止 MongoDB 上傳器
        if uploader is not None:
            try:
                await uploader.stop()
            except Exception:
                pass

        # 關閉 MongoDB 連線
        if mongo_client is not None:
            try:
                mongo_client.close()
            except Exception:
                pass

        logger.info("GC shutdown complete")


async def _wait_for_stop(stop_event: asyncio.Event) -> None:
    """等待停止訊號後取消所有 gather 任務。"""
    await stop_event.wait()
    # 取消父 gather 中的其他任務
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
