# ============================================================
# gc_main_mongo.py — GC_mini_test 整合 MongoDB 上傳版本
#
# 架構說明：
#   AsyncModbusDevice (pcs_01)
#       ↓ events (read_complete / disconnected)
#   UnifiedDeviceManager
#       ├── DeviceManager        — 設備讀取生命週期管理
#       └── DataUploadManager    — 自動上傳讀取資料至 MongoDB
#           └── MongoBatchUploader
#               └── Motor → MongoDB collection "pcs_data"
#
# 上傳時機：每次 read_complete 事件觸發後自動排入 queue，
#           由 MongoBatchUploader 每 5 秒或累積 50 筆批次寫入。
#
# MongoDB 文件格式範例：
#   {
#     "device_id": "pcs_01",
#     "timestamp": <datetime>,
#     "active_power": 12.3,
#     "soc": 75.0,
#     "fault_code": 0
#   }
# ============================================================

import asyncio
from device import *
from influx_lib import dualDBUnifiedConfig, dualDBUnifiedDeviceManager, InfluxBatchUploader
from influxdb_client_3 import InfluxDBClient3

from csp_lib.core import get_logger, set_level
from csp_lib.equipment.device import AsyncModbusDevice, DeviceConfig
from csp_lib.manager.unified import UnifiedConfig, UnifiedDeviceManager
from csp_lib.modbus import ModbusTcpConfig, PymodbusTcpClient
from csp_lib.mongo import MongoBatchUploader, MongoConfig, UploaderConfig, create_mongo_client


# ============================================================
# 日誌設定
# ============================================================

logger = get_logger("gc_mini_test")
set_level("info")

# ============================================================
# MongoDB 連線設定（請依實際環境修改）
# ============================================================

MONGO_CONFIG = MongoConfig(
    host="localhost",       # MongoDB 主機位址
    port=27017,             # MongoDB 連接埠
    # username="your_user", # 若有帳號驗證請取消註解
    # password="your_pass",
    # auth_source="admin",
)

MONGO_DATABASE = "demo"   # 資料庫名稱
MONGO_PCS_COLLECTION = "pcs"        # PCS 設備資料 collection 名稱

# ============================================================
# MongoBatchUploader 設定
# ============================================================
# flush_interval        : 每 5 秒定期批次寫入一次
# batch_size_threshold  : 累積 50 筆時立即觸發寫入（不等 5 秒）
# max_queue_size        : queue 上限 5000 筆，超過則丟棄最舊資料
# max_retry_count       : 單批寫入失敗最多重試 3 次

UPLOADER_CONFIG = UploaderConfig(
    flush_interval=10,
    batch_size_threshold=100,
    max_queue_size=5000,
    max_retry_count=3,
)

# ============================================================
# 主程式
# ============================================================


async def main():
    # ----------------------------------------------------------
    # 1. 建立 DB 連線
    # ----------------------------------------------------------
    logger.info("建立 DB 連線...")
    mongo_client = create_mongo_client(MONGO_CONFIG)
    mongo_db = mongo_client[MONGO_DATABASE]
    # 啟動批次上傳器（需在 asyncio event loop 內呼叫 start()）
    uploader = MongoBatchUploader(mongo_db, config=UPLOADER_CONFIG)
    uploader.start()

    influx_db = InfluxDBClient3(host="http://localhost:8181", token="apiv3_usT1NxnSLHezkpmfZXfdrZwo-tmgk_JUhtDygx4A4NiuZD0z23LSoPi1MbvKKurue-ZLRGCXKBJHeMuneloZQA", org="my-org", database="demo")
    influx_uploader = InfluxBatchUploader(influx_db=influx_db, config=UPLOADER_CONFIG)
    influx_uploader.start()

    logger.info(
        f"BatchUploader 已啟動 "
        f"(flush_interval={UPLOADER_CONFIG.flush_interval}s, "
        f"batch_threshold={UPLOADER_CONFIG.batch_size_threshold})"
    )
    # ----------------------------------------------------------
    # 2. 建立 Modbus 設備
    # ----------------------------------------------------------
    client = PymodbusTcpClient(ModbusTcpConfig(host="127.0.0.1", port=5020))
    config = DeviceConfig(
        device_id="pcs_01",
        unit_id=2,
        address_offset=0,       # 部分 PLC 用 1-based addressing（offset=1）
        read_interval=1.0,      # 每 1 秒讀取一次 → 每分鐘約 60 筆入 queue
        disconnect_threshold=5, # 連續 5 次失敗後標記為斷線
    )

    device = AsyncModbusDevice(
        config=config,
        client=client,
        always_points=pcs_always_points,
        write_points=pcs_write_points,
        alarm_evaluators=pcs_alarm_evaluators,
    )


    # ----------------------------------------------------------
    # 3. 設定 UnifiedDeviceManager（帶 MongoDB 上傳）
    # ----------------------------------------------------------
    unified_config = dualDBUnifiedConfig(
        mongo_uploader=uploader,    # 傳入後自動啟用 DataUploadManager
        influx_uploader=influx_uploader,       # 傳入後自動啟用 InfluxBatchUploader
        # alarm_repository=None,    # 若需告警持久化，傳入 MongoDB AlarmRepository
        # command_repository=None,  # 若需指令歷史記錄，傳入 MongoDB CommandRepository
        # redis_client=None,        # 若需狀態同步至 Redis，傳入 RedisClient
    )

    manager = dualDBUnifiedDeviceManager(unified_config)

    # 註冊設備並指定 MongoDB collection 名稱
    # DataUploadManager 會自動訂閱 read_complete / disconnected 事件
    manager.register(device, collection_name=MONGO_PCS_COLLECTION)

    logger.info(f"UnifiedDeviceManager 配置完成: {manager!r}")
    logger.info(f"  data_manager (MongoDB 上傳) 啟用: {manager.data_manager is not None}")

    # ----------------------------------------------------------
    # 4. 保留原有的 console 事件處理器（debug 輸出用）
    # ----------------------------------------------------------
    async def on_value_change(payload):
        print(f"[{payload.device_id}] {payload.point_name}: {payload.old_value} -> {payload.new_value}")

    async def on_alarm_triggered(payload):
        event = payload.alarm_event
        print(f"[ALARM] {payload.device_id}: {event.alarm.code} ({event.alarm.level.name})")

    async def on_alarm_cleared(payload):
        event = payload.alarm_event
        print(f"[CLEAR] {payload.device_id}: {event.alarm.code}")

    async def on_disconnected(payload):
        print(f"[DISCONNECT] {payload.device_id}: {payload.reason} (failures={payload.consecutive_failures})")

    cancel_vc    = device.on("value_change",     on_value_change)
    cancel_alarm = device.on("alarm_triggered",  on_alarm_triggered)
    cancel_clear = device.on("alarm_cleared",    on_alarm_cleared)
    cancel_dc    = device.on("disconnected",     on_disconnected)

    # ----------------------------------------------------------
    # 5. 啟動管理器並持續運行
    # ----------------------------------------------------------
    try:
        async with manager:
            logger.info("UnifiedDeviceManager 已啟動，開始讀取設備並上傳至 MongoDB。")
            logger.info(f"  資料將寫入: {MONGO_DATABASE}.{MONGO_PCS_COLLECTION}")
            logger.info("Press Ctrl+C to stop.")
            power = 0.0
            result = await manager.device_manager.all_devices[0].write("BMS_on_off", 1, verify=True)  # 開啟設備（測試寫入指令）
            logger.info(f"寫入 BMS_on_off=1 結果: {result.status.value}")
            while True:
                await asyncio.sleep(1)
                if manager.is_running:
                    result = await manager.device_manager.all_devices[0].write("p_set", power)  # 測試寫入指令
                    logger.info(f"寫入 p_set={power:.1f} 結果: {result.status.value}")
                    power += 1.0
                    if power > 10.0:
                        power = -10.0

    finally:
        # ----------------------------------------------------------
        # 6. 清理：flush 剩餘資料、關閉連線、取消事件監聽
        # ----------------------------------------------------------
        logger.info("停止 MongoBatchUploader，flush 剩餘資料...")
        await uploader.stop()

        cancel_vc()
        cancel_alarm()
        cancel_clear()
        cancel_dc()

        mongo_client.close()
        logger.info("MongoDB 連線已關閉。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
