# 實作計畫：GC_mini_test/LIB — 本地 InfluxDB 擴充（不修改 csp_lib）

## 目標

在現有 MongoDB 之外，同時將所有設備資料上傳至 InfluxDB v3，兩者並存。
**不修改 `csp_lib` 任何檔案**，改在本地 `LIB/` 建立擴充模組。

---

## 最終目錄結構

```
examples/GC_mini_test/
├── device.py                      （原有，不動）
├── gc_main.py                     （原有，不動）
├── gc_main_mongo.py               （原有，不動）
├── gc_main_dual.py                （新建：MongoDB + InfluxDB 並行版本）
└── LIB/
    ├── __init__.py                （空白）
    ├── influx/
    │   ├── __init__.py
    │   ├── config.py
    │   ├── client.py
    │   └── uploader.py
    └── manager.py
```

---

## 架構設計

### 繼承關係

```
csp_lib（不修改）              GC_mini_test/LIB/（新建）
──────────────────────         ────────────────────────────────
UnifiedConfig          ←──     DualUnifiedConfig（繼承，加 influx_uploader）
UnifiedDeviceManager   ←──     DualUnifiedDeviceManager（繼承，加第二個 DataUploadManager）
DataUploadManager      ←──     第二個實例，注入 InfluxBatchUploader（duck typing）
```

### 關鍵原理：為何不需修改 DataUploadManager

`DataUploadManager` 對 uploader 只呼叫兩個方法：
- `uploader.register_collection(name: str)`
- `uploader.enqueue(name: str, document: dict)`

`InfluxBatchUploader` 實作相同介面，可直接傳入（duck typing）。

### Document → InfluxDB Point 轉換規則

```
document = {
    "device_id": "pcs_01",        → .tag("device_id", "pcs_01")    ← tag（索引，字串）
    "timestamp": datetime(...),    → .time(datetime)                 ← 時間戳
    "active_power": 12.3,          → .field("active_power", 12.3)   ← field（數值）
    "soc": 75.0,                   → .field("soc", 75.0)
    "fault_code": None,            → 略過（InfluxDB 不支援 null）
}
```

斷線記錄（全 None）→ InfluxDB 自動略過整筆，MongoDB 仍正常寫入 null 記錄。

---

## 各檔案實作說明

### `LIB/influx/config.py`

```python
from __future__ import annotations
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class InfluxConfig:
    host: str = "http://localhost:8086"
    token: str = ""
    database: str = "csp"


@dataclass(frozen=True)
class InfluxUploaderConfig:
    flush_interval: int = 5              # 定期 flush 間隔（秒）
    batch_size_threshold: int = 100      # 累積此筆數立即 flush
    max_queue_size: int = 10000          # queue 上限
    max_retry_count: int = 3             # 最大重試次數
    tag_fields: tuple[str, ...] = field(default_factory=lambda: ("device_id",))
    # tag_fields: 這些欄位轉為 InfluxDB tag（有索引），其餘轉為 field
```

### `LIB/influx/client.py`

```python
from influxdb_client_3 import InfluxDBClient3
from LIB.influx.config import InfluxConfig


def create_influx_client(config: InfluxConfig) -> InfluxDBClient3:
    return InfluxDBClient3(
        host=config.host,
        token=config.token,
        database=config.database,
    )
```

### `LIB/influx/uploader.py`（核心）

需實作：
- `_document_to_point(measurement, document, tag_fields) -> Point | None`
  - tag_fields 欄位 → `.tag(k, str(v))`
  - `"timestamp"` 欄位 → `.time(v)`
  - 其餘非 None 的 `int/float/str/bool` → `.field(k, v)`
  - 無任何有效 field → 回傳 `None`（斷線記錄）

- `_MeasurementQueue`：每個 measurement 一個 `deque[Point]` + `asyncio.Lock`
  - `enqueue(point)`, `drain() -> list[Point]`, `restore(points)`, `size_sync()`

- `InfluxBatchUploader`：
  - `register_collection(measurement)` — 建立 queue
  - `enqueue(measurement, document)` — 轉換後入 queue
  - `start()` / `stop()` — 啟動/停止 flush loop
  - `flush_all()` — 強制 flush 全部
  - `_flush_measurement(name)` — 從 queue drain → `loop.run_in_executor(None, client.write, points)`
    > ⚠️ `influxdb_client_3.write()` 是**同步**方法，必須透過 executor 呼叫以避免阻塞 event loop

### `LIB/manager.py`

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING

from csp_lib.manager.unified import UnifiedConfig, UnifiedDeviceManager
from csp_lib.manager.data import DataUploadManager

if TYPE_CHECKING:
    from LIB.influx.uploader import InfluxBatchUploader


@dataclass
class DualUnifiedConfig(UnifiedConfig):
    """繼承 UnifiedConfig，新增 influx_uploader 可選欄位"""
    influx_uploader: InfluxBatchUploader | None = None


class DualUnifiedDeviceManager(UnifiedDeviceManager):
    """繼承 UnifiedDeviceManager，新增 InfluxDB 並行上傳"""

    def __init__(self, config: DualUnifiedConfig) -> None:
        super().__init__(config)
        self._influx_data_manager: DataUploadManager | None = (
            DataUploadManager(config.influx_uploader)  # type: ignore[arg-type]
            if config.influx_uploader
            else None
        )

    def _subscribe_all(self, device, collection_name) -> None:
        super()._subscribe_all(device, collection_name)
        if self._influx_data_manager and collection_name:
            self._influx_data_manager.subscribe(device, collection_name)

    @property
    def influx_data_manager(self) -> DataUploadManager | None:
        return self._influx_data_manager
```

### `gc_main_dual.py`（新範例主程式）

在 `gc_main_mongo.py` 基礎上修改：

1. 新增 import：
```python
from LIB.influx import InfluxConfig, InfluxUploaderConfig, InfluxBatchUploader, create_influx_client
from LIB.manager import DualUnifiedConfig, DualUnifiedDeviceManager
```

2. 建立 InfluxDB 連線：
```python
INFLUX_CONFIG = InfluxConfig(
    host="http://localhost:8086",
    token="your_token_here",
    database="csp",
)
influx_client = create_influx_client(INFLUX_CONFIG)
influx_uploader = InfluxBatchUploader(influx_client, config=InfluxUploaderConfig(
    flush_interval=1,
    batch_size_threshold=100,
))
influx_uploader.start()
```

3. 換用 Dual manager：
```python
unified_config = DualUnifiedConfig(
    mongo_uploader=uploader,          # 原有 MongoDB
    influx_uploader=influx_uploader,  # 新增 InfluxDB
)
manager = DualUnifiedDeviceManager(unified_config)
```

4. `finally` 區塊補充：
```python
await influx_uploader.stop()
influx_client.close()
```

---

## 安裝套件

```bash
uv pip install influxdb3-python
```

---

## InfluxDB v3 Windows 自架

```powershell
# 1. 下載 influxdb3-core Windows zip
#    https://docs.influxdata.com/influxdb3/core/get-started/

# 2. 啟動服務
.\influxd3.exe serve --object-store file --data-dir ./influx_data

# 3. 建立 database
influxdb3 create database csp

# 4. 建立 token（記下 token 填入 InfluxConfig）
influxdb3 create token --permission all --database csp

# 5. 查詢驗證
influxdb3 query --database csp "SELECT * FROM pcs LIMIT 10"
```

---

## 驗證步驟

1. 啟動 InfluxDB v3
2. 設定 `gc_main_dual.py` 中的 token
3. 執行：`python gc_main_dual.py`
4. 確認：
   - MongoDB `demo.pcs` 有資料（原有行為不變）
   - InfluxDB `csp.pcs` measurement 有資料
   - 斷線時 MongoDB 有 null 記錄；InfluxDB 無該筆（正常，None 欄位略過）
