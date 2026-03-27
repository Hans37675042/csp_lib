
from dataclasses import dataclass
from typing import Any, Optional
import asyncio, time
from influxdb_client_3 import InfluxDBClient3

from csp_lib.core import get_logger, set_level
from csp_lib.mongo import MongoBatchUploader
from csp_lib.mongo.config import UploaderConfig
from csp_lib.mongo.queue import BatchQueue
from csp_lib.equipment.device import AsyncModbusDevice
from csp_lib.manager.data import DataUploadManager as DU
from csp_lib.manager.unified import UnifiedConfig, UnifiedDeviceManager

logger = get_logger("gc_mini_test")

@dataclass
class WriteResult:
    success: bool
    inserted_count: int = 0
    error_message: Optional[str] = None

class influxWriter():
    def __init__(self, influx_db: InfluxDBClient3) -> None:
        self._influx_db = influx_db

    async def write_batch(self, collection_name: str, documents: list[dict[str, Any]]) -> WriteResult:
        if not documents:
            return WriteResult(success=True, inserted_count=0)
        try:
            logger.debug(f"InfluxWriter: 寫入 {len(documents)} 筆至 '{collection_name}'")
            records = []
            for doc in documents:
                time = doc.pop("timestamp")
                record = {"measurement": collection_name, "fields": doc, "time": time}
                records.append(record)

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda:self._influx_db.write(records, "demo"))
            logger.debug(f"InfluxWriter: 成功寫入 {len(records)} 筆至 '{collection_name}'")
            return WriteResult(success=True, inserted_count=len(records))

        except Exception as e:
            error_msg = f"寫入 '{collection_name}' 失敗: {e}"
            logger.error(f"InfluxWriter: {error_msg}")
            return WriteResult(success=False, error_message=error_msg)

class InfluxBatchUploader(MongoBatchUploader):
    def __init__(self, influx_db: InfluxDBClient3, config: Optional[UploaderConfig] = None) -> None:
        self._config = config or UploaderConfig()
        self._writer = influxWriter(influx_db)
        self._queues: dict[str, BatchQueue] = {}
        self._retry_counts: dict[str, int] = {}  # collection_name -> retry count
        self._stop_event = asyncio.Event()
        self._flush_task: Optional[asyncio.Task[None]] = None
        self._flush_event = asyncio.Event()

    async def enqueue(self, collection_name: str, document: dict[str, Any]) -> None:
        await super().enqueue(collection_name, document)  # 確保 queue 存在
        if self._queues[collection_name].size_sync() >= self._config.batch_size_threshold:
            logger.debug(f"InfluxBatchUploader: '{collection_name}' size: {self._queues[collection_name].size_sync()}達到批次閾值:{self._config.batch_size_threshold}，觸發立即上傳")
            self._flush_event.set()  # 觸發立即 flush
    
    async def _flush_loop(self) -> None:
        """定期檢查並上傳所有 collection 的資料"""
        next_time = time.monotonic()
        while not self._stop_event.is_set():
            try:
                stop_task = asyncio.ensure_future(self._stop_event.wait())
                flush_task = asyncio.ensure_future(self._flush_event.wait())

                loop_time = time.monotonic()
                next_time += self._config.flush_interval
                if next_time > loop_time:
                    timeout = next_time - loop_time
                else:
                    while next_time < loop_time:
                        next_time += self._config.flush_interval
                    timeout = 0

                done, pending = await asyncio.wait(
                    [stop_task, flush_task],
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=timeout,
                )
                for t in pending:
                    t.cancel()
                    
                if not done: # timeout，定期 flush 所有 queue
                    await self.flush_all()
                elif stop_task in done: # stop_event 被設定，結束迴圈前 flush 所有資料
                    await self.flush_all()
                elif flush_task in done: # flush_event 被設定，flush queue 滿出的資料
                    for name, queue in list(self._queues.items()):
                        if queue.size_sync() >= self._config.batch_size_threshold:
                            await self._flush_collection(name)

            except asyncio.CancelledError: # 結束前 flush 所有資料
                await self.flush_all()
                raise

            except Exception as e:
                logger.error(f"MongoBatchUploader: flush loop 錯誤: {e}")
                await asyncio.sleep(1)
            
            finally:
                self._flush_event.clear()
                stop_task.cancel()
                flush_task.cancel()
                

        
class DataUploadManager(DU):
    def __init__(self, uploader: MongoBatchUploader | InfluxBatchUploader) -> None:
        super().__init__(uploader)

@dataclass
class dualDBUnifiedConfig(UnifiedConfig):
    influx_uploader: InfluxBatchUploader | None = None

class dualDBUnifiedDeviceManager(UnifiedDeviceManager):
    def __init__(self, config: dualDBUnifiedConfig):
        super().__init__(config)
        self._data_manager_influx: DataUploadManager | None = (
            DataUploadManager(config.influx_uploader) if config.influx_uploader else None
        )
        logger.debug(
            f"UnifiedDeviceManager 初始化: "
            f"alarm={self._alarm_manager is not None}, "
            f"command={self._command_manager is not None}, "
            f"data_mongo={self._data_manager is not None}, "
            f"data_influx={self._data_manager_influx is not None}, "
            f"state={self._state_manager is not None}, "
            f"statistics={self._statistics_manager is not None}"
        )

    def _subscribe_all(self, device: AsyncModbusDevice, collection_name: str | None) -> None:
        super()._subscribe_all(device, collection_name)
        if self._data_manager_influx and collection_name:
            self._data_manager_influx.subscribe(device, collection_name)

    @property
    def data_manager_influx(self) -> DataUploadManager | None:
        """資料上傳管理器（可能為 None）"""
        return self._data_manager_influx
    








    
async def main_direct_write():
    influx_db = InfluxDBClient3(host="http://localhost:8181", token="apiv3_usT1NxnSLHezkpmfZXfdrZwo-tmgk_JUhtDygx4A4NiuZD0z23LSoPi1MbvKKurue-ZLRGCXKBJHeMuneloZQA", org="my-org", database="demo")
    print("連線完成:", influx_db)
    influx_db.write(record={"measurement": "test_measurement1", "fields": {"value1": 42, "value2": 43}}, database="demo")
    print("寫入完成")
    print("查詢結果:", influx_db.query('SELECT * FROM "test_measurement1" WHERE time >= now() - interval \'1 hour\'', language="sql"))
    config = UploaderConfig()
    InfluxBatchUploader(influx_db=influx_db, config=config)

UPLOADER_CONFIG = UploaderConfig(
    flush_interval=1,
    batch_size_threshold=100,
    max_queue_size=5000,
    max_retry_count=3,
)

async def main():

    influx_db = InfluxDBClient3(host="http://localhost:8181", token="apiv3_usT1NxnSLHezkpmfZXfdrZwo-tmgk_JUhtDygx4A4NiuZD0z23LSoPi1MbvKKurue-ZLRGCXKBJHeMuneloZQA", org="my-org", database="demo")
    print("連線完成:", influx_db)
    uploader = InfluxBatchUploader(influx_db, config=UPLOADER_CONFIG)
    uploader.start()
    logger.info(
        f"MongoBatchUploader 已啟動 "
        f"(flush_interval={UPLOADER_CONFIG.flush_interval}s, "
        f"batch_threshold={UPLOADER_CONFIG.batch_size_threshold})"
    )

if __name__ == "__main__":
    asyncio.run(main())