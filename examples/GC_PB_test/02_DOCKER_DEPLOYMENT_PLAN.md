# GC_PB_test Docker 化部署計畫

## Context

[examples/GC_PB_test/](./) 是 7 支獨立 Python process 的分散式控制範例（sim + strategy + 5 device runner），透過 Redis 做 state 同步與 pub/sub、MongoDB 做 command repository。目前 script 在本機直接跑，host/port 硬編碼 `localhost`，且 5 支 `device_*_main.py` 彼此差異只在常數、重複度高。

本計畫的兩個目標：

1. **快速部署**：`docker compose up -d` 一鍵啟動整組 container 與現有 Redis 容器相容；配合本機 Mongo 與 `python sim.py` 即可跑通全鏈路。
2. **單一 image、config 切換現場**：「會依現場變動」的東西（設備組成、point 對應、策略 mapping）外部化到 YAML，同一份 image 跑測試 / A 廠 / B 廠，不 rebuild。

---

## 設計決策總表

| 決策項 | 方案 |
|------|------|
| Docker artifact 位置 | 全部集中 [docker_test/](./docker_test/)（新增子目錄），不污染 example 根目錄 |
| `sim.py` | **零改動**、留在 host 上 `python sim.py` 直跑，不容器化 |
| Redis | 進 compose 管理（取代獨立 `csp-redis`，volume 名稱沿用 `csp-redis-data` 延續資料） |
| MongoDB | **留在 host**，容器透過 `host.docker.internal:27017` 連回 |
| Modbus sim | 容器透過 `host.docker.internal:5020` 連 host 上的 `sim.py` |
| 5 支 device_*_main.py | 收斂成 1 支 generic `device_main.py`，env var 決定角色 |
| strategy mapping | 外部化到 [docker_test/config/site_sim.yaml](./docker_test/config/site_sim.yaml)，換站 = mount 不同 YAML |

### 為何策略 brain 不拆多 container

| 看似該拆的理由 | 為何用 config 解決即可 |
|---|---|
| A 廠只有 PCS+BMS、B 廠含 Solar | `context_maps` YAML 差異，image 同一份 |
| 想同時跑 PQ 模式與 Droop 模式 | 兩個 `modes.entries`，靠 `ModePriority` 仲裁，同一 controller |
| 熱切換策略 | `register_mode()` runtime 重註冊；或 `docker compose restart strategy` |
| SOC 保護要獨立運作 | 它本來就跟策略耦合，跨 container 反而 race |

真正需要拆 container 的是 **跨實體機 HA**，那是 [01_REDUNDANCY_PLAN.md](./01_REDUNDANCY_PLAN.md) 的 etcd 路線，不在本計畫範圍。

---

## 最終檔案結構

```
examples/GC_PB_test/
├── 02_DOCKER_DEPLOYMENT_PLAN.md        ← 本文件
├── docker_test/                        ← 本次新增（所有 Docker 相關）
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── .dockerignore
│   ├── .env.example
│   ├── README.md                       ← 部署操作指南
│   └── config/
│       └── site_sim.yaml               ← 預設站型（模擬環境）
│       # 未來現場站自行複製 site_A.yaml / site_B.yaml ...
│
├── device.py                           ← 修改：末尾加 POINT_SETS registry
├── device_main.py                      ← 新建：generic device runner
├── env_config.py                       ← 新建：env var helper
├── strategy_main.py                    ← 改寫：讀 YAML
├── sim.py                              ← 零改動
├── redis_listener.py                   ← 零改動
├── ems_cli.py                          ← 零改動
├── strategy.py                         ← 零改動
├── protection.py                       ← 零改動
├── main.py                             ← 零改動（legacy，保留）
├── test_strategies.py                  ← 零改動
│
├── device_pcs_main.py                  ← 刪除（改用 device_main.py）
├── device_bms_main.py                  ← 刪除
├── device_acm_main.py                  ← 刪除
├── device_solar_main.py                ← 刪除
└── device_load_main.py                 ← 刪除
```

---

## 整體架構

```
  ┌─────────── host（Windows Docker Desktop）──────────┐
  │                                                    │
  │   python sim.py    (Modbus server, 127.0.0.1:5020) │
  │   mongod           (:27017)                        │
  │                                                    │
  │   ┌──── docker-compose (workdir: docker_test) ──┐  │
  │   │                                              │ │
  │   │  redis  (service name: redis, :6379 → host) │ │
  │   │                                              │ │
  │   │  pcs_01 / bms_01 / acm_01 / solar_01 /       │ │
  │   │  load_01          image: csp-gc-pb:latest   │ │
  │   │  strategy         同一 image，mount YAML    │ │
  │   │                                              │ │
  │   │  csp-net (bridge)                            │ │
  │   │  extra_hosts: host.docker.internal→host-gw   │ │
  │   └──────────────────────────────────────────────┘ │
  │                                                    │
  │   容器→Modbus: host.docker.internal:5020            │
  │   容器→Mongo:  host.docker.internal:27017           │
  │   容器→Redis:  redis:6379  (compose 內網)           │
  └────────────────────────────────────────────────────┘
```

**只 1 個 image**（`csp-gc-pb:latest`），**7 個 compose service**：redis + 5 device + strategy。

---

## 一、程式碼改動（`examples/GC_PB_test/` 根目錄）

### 1.1 新建 `env_config.py`

```python
import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB   = os.getenv("MONGO_DB", "demo")

def modbus_target(device_id: str) -> tuple[str, int]:
    """每個 device 可獨立覆寫 Modbus 目標；預設走 SIM_HOST/SIM_PORT。"""
    key = device_id.upper()
    host = os.getenv(f"{key}_MODBUS_HOST", os.getenv("SIM_HOST", "localhost"))
    port = int(os.getenv(f"{key}_MODBUS_PORT", os.getenv("SIM_PORT", "5020")))
    return host, port
```

預設值仍為 `localhost`，**非容器環境以 `python xxx.py` 直跑完全相容**。

### 1.2 修改 `device.py` — 末尾加 POINT_SETS registry

既有 `pcs_read_points`、`bms_read_points` 等全數保留，末尾新增：

```python
POINT_SETS = {
    "pcs":   {"read": pcs_read_points,   "write": pcs_write_points},
    "bms":   {"read": bms_read_points,   "write": []},
    "meter": {"read": meter_read_points, "write": []},
    "solar": {"read": solar_read_points, "write": []},
    "load":  {"read": load_read_points,  "write": []},
}
```

未來新增廠牌（e.g. Delta 與 SMA 兩種 PCS 共用此 repo）只需再加一筆 entry，e.g. `"pcs_sma"`，**不必改 device_main.py**。

### 1.3 新建 `device_main.py`（取代 5 支 device_*_main.py）

```python
import asyncio, os

from device import POINT_SETS
from env_config import (
    REDIS_HOST, REDIS_PORT, MONGO_HOST, MONGO_PORT, MONGO_DB, modbus_target,
)

from csp_lib.core import get_logger
from csp_lib.redis import RedisClient, RedisConfig
from csp_lib.mongo import create_mongo_client, MongoConfig
from csp_lib.mongo.config import UploaderConfig
from csp_lib.mongo.uploader import MongoBatchUploader
from csp_lib.equipment.device import AsyncModbusDevice, DeviceConfig
from csp_lib.manager import UnifiedConfig, UnifiedDeviceManager
from csp_lib.manager.command import MongoCommandRepository
from csp_lib.modbus import ModbusTcpConfig, PymodbusTcpClient
from csp_lib.integration.distributed import RemoteSiteConfig, RemoteSiteRunner


DEVICE_ID     = os.environ["DEVICE_ID"]
UNIT_ID       = int(os.environ["UNIT_ID"])
TRAIT         = os.environ["TRAIT"]
POINT_SET     = os.environ["POINT_SET"]
SITE_ID       = os.getenv("SITE_ID", f"site_{DEVICE_ID}")
READ_INTERVAL = float(os.getenv("READ_INTERVAL", "1"))

logger = get_logger(f"DEV_{TRAIT.upper()}")


async def main() -> None:
    points = POINT_SETS[POINT_SET]
    host, port = modbus_target(DEVICE_ID)

    dev_kwargs = {
        "config": DeviceConfig(device_id=DEVICE_ID, unit_id=UNIT_ID, read_interval=READ_INTERVAL),
        "client": PymodbusTcpClient(ModbusTcpConfig(host=host, port=port)),
        "always_points": points["read"],
    }
    if points["write"]:
        dev_kwargs["write_points"] = points["write"]
    device = AsyncModbusDevice(**dev_kwargs)

    mongo_db = create_mongo_client(MongoConfig(host=MONGO_HOST, port=MONGO_PORT))[MONGO_DB]
    mongo_uploader = MongoBatchUploader(mongo_db, UploaderConfig(
        flush_interval=1, batch_size_threshold=100, max_queue_size=10000, max_retry_count=3,
    ))

    redis_client = RedisClient.from_config(RedisConfig(host=REDIS_HOST, port=REDIS_PORT))
    await redis_client.connect()

    manager = UnifiedDeviceManager(UnifiedConfig(
        batch_uploader=mongo_uploader,
        redis_client=redis_client,
        command_repository=MongoCommandRepository(mongo_db, "commands"),
    ))
    manager.register(device, TRAIT)

    runner = RemoteSiteRunner(
        config=RemoteSiteConfig(site_id=SITE_ID, device_ids=[DEVICE_ID]),
        unified_manager=manager,
        redis_client=redis_client,
    )

    try:
        async with device:
            mongo_uploader.start()
            async with runner:
                logger.info(f"[{SITE_ID}] running as {DEVICE_ID} ({TRAIT}) → modbus {host}:{port}")
                await asyncio.Event().wait()
    finally:
        await redis_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
```

**新增第二台 PCS** = compose 多一個 service（env 指定 `DEVICE_ID=pcs_02, UNIT_ID=11, PCS_02_MODBUS_HOST=192.168.1.20`），**0 個 .py 改動**。

### 1.4 改寫 `strategy_main.py` — 讀 YAML

```python
import asyncio, os
from datetime import datetime, timedelta

import yaml
from strategy import PQ_ramp_Time_ModeConfig, PQ_ramp_Time_ModeStrategy
from redis_listener import EMSCommandListener
from env_config import REDIS_HOST, REDIS_PORT

from csp_lib.core import get_logger
from csp_lib.redis import RedisClient, RedisConfig
from csp_lib.integration import DeviceRegistry, SystemController, SystemControllerConfig
from csp_lib.controller.strategies import StopStrategy
from csp_lib.controller.system import DynamicSOCProtection, ModePriority, SOCProtectionConfig
from csp_lib.integration.distributed import DistributedConfig, DistributedController, RemoteSiteConfig

logger = get_logger("STRAT")
SITE_CONFIG = os.getenv("SITE_CONFIG", "docker_test/config/site_sim.yaml")


def _default_pq_config():
    now = datetime.now()
    return PQ_ramp_Time_ModeConfig(
        p_start=0.0, p_end=0.0, q=0.0,
        start_time=now, end_time=now + timedelta(seconds=1),
    )

# 白名單，避免 YAML 被注入任意 class 名
STRATEGY_REGISTRY = {
    "StopStrategy": lambda: StopStrategy(),
    "PQ_ramp_Time_ModeStrategy": lambda: PQ_ramp_Time_ModeStrategy(_default_pq_config()),
}
PRIORITY_REGISTRY = {"SCHEDULE": ModePriority.SCHEDULE, "MANUAL": ModePriority.MANUAL}


def build_controller_config(cfg: dict) -> SystemControllerConfig:
    b = SystemControllerConfig.builder()
    for m in cfg.get("context_maps", []):
        b.map_context(point_name=m["point"], target=m["target"], device_id=m["device_id"])
    for m in cfg.get("command_maps", []):
        b.map_command(field=m["field"], point_name=m["point"], device_id=m["device_id"])
    if soc := cfg.get("protection", {}).get("soc"):
        b.protect(DynamicSOCProtection(SOCProtectionConfig(**soc)))
    if cfg.get("auto_stop", True):
        b.auto_stop(enabled=True)
    return b.build()


async def main() -> None:
    with open(SITE_CONFIG) as f:
        cfg = yaml.safe_load(f)

    redis_client = RedisClient.from_config(RedisConfig(host=REDIS_HOST, port=REDIS_PORT))
    await redis_client.connect()

    sys_ctrl = SystemController(DeviceRegistry(), build_controller_config(cfg))
    dist_cfg = DistributedConfig(
        sites=[RemoteSiteConfig(site_id=s["site_id"], device_ids=s["devices"]) for s in cfg["sites"]],
        trait_device_map=cfg["trait_device_map"],
        poll_interval=cfg.get("poll_interval", 1.0),
        system_alarm_on_device_offline=cfg.get("system_alarm_on_device_offline", True),
    )
    dist_ctrl = DistributedController(dist_cfg, sys_ctrl, redis_client)

    for e in cfg["modes"]["entries"]:
        dist_ctrl.register_mode(
            e["name"], STRATEGY_REGISTRY[e["type"]](),
            PRIORITY_REGISTRY[e["priority"]], e["desc"],
        )
    await dist_ctrl.set_base_mode(cfg["modes"]["base"])

    ems = EMSCommandListener(redis_client=redis_client, controller=dist_ctrl, ramp_mode_name="pq_mode")
    await ems.start()
    try:
        async with dist_ctrl:
            logger.info(f"Strategy running (config: {SITE_CONFIG})")
            await asyncio.Event().wait()
    finally:
        await ems.stop()
        await redis_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
```

### 1.5 `pyproject.toml`

[../../pyproject.toml](../../pyproject.toml) 的 `[project] dependencies` 加 `pyyaml>=6.0`（strategy_main 需要）。

### 1.6 `sim.py` → 零改動

維持原本 `SIM_HOST, SIM_PORT = "127.0.0.1", 5020`。部署時以 `python sim.py` 在 host 直接跑，容器透過 `host.docker.internal:5020` 連入。

### 1.7 刪除 5 支 device_*_main.py

- `device_pcs_main.py` / `device_bms_main.py` / `device_acm_main.py` / `device_solar_main.py` / `device_load_main.py`
- git history 保留，隨時可找回。

---

## 二、Docker artifact（全部在 `docker_test/`）

### 2.1 `docker_test/Dockerfile`

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.13-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 先複製 metadata，最大化 pip layer 快取
COPY pyproject.toml README.md ./
COPY csp_lib ./csp_lib

RUN pip install --upgrade pip && \
    pip install ".[redis,mongo,modbus]" pyyaml

# 複製 example code（sim.py 雖一起進去，但容器不執行它）
COPY examples/GC_PB_test ./examples/GC_PB_test

WORKDIR /app/examples/GC_PB_test
```

**故意不設 `CMD`**：每個 service 於 compose 以 `command:` 明確指定。

### 2.2 `docker_test/.dockerignore`

```
**/__pycache__
**/*.pyc
.git
.venv
dist
build
*.egg-info
tests
docs
.vscode
.idea
examples/GC_PB_test/docker_test
```

最後一行避免 docker_test 自身被複製進 image。

### 2.3 `docker_test/docker-compose.yml`

```yaml
# workdir: examples/GC_PB_test/docker_test/
# build context: 往上三層到 repo 根目錄

x-device-common: &device-common
  image: csp-gc-pb:latest
  depends_on: [redis]
  restart: unless-stopped
  command: ["python", "device_main.py"]
  networks: [csp-net]
  extra_hosts:
    - "host.docker.internal:host-gateway"   # Linux 需要；Windows/Mac 忽略無害

services:
  # ===== Redis 進 compose =====
  redis:
    image: redis:7-alpine
    container_name: csp-redis
    restart: always
    ports: ["6379:6379"]                    # 對 host 暴露方便 redis-cli debug
    volumes: [csp-redis-data:/data]         # volume 名稱延續既有 csp-redis-data
    command: redis-server --appendonly yes
    networks: [csp-net]

  # ===== 5 個 device（同一 image，差 env） =====
  pcs_01:
    <<: *device-common
    container_name: csp-pcs-01
    environment:
      DEVICE_ID: pcs_01
      UNIT_ID: "10"
      TRAIT: pcs
      POINT_SET: pcs
      REDIS_HOST: redis
      MONGO_HOST: host.docker.internal
      PCS_01_MODBUS_HOST: ${PCS_01_MODBUS_HOST:-host.docker.internal}
      PCS_01_MODBUS_PORT: ${PCS_01_MODBUS_PORT:-5020}

  bms_01:
    <<: *device-common
    container_name: csp-bms-01
    environment:
      DEVICE_ID: bms_01
      UNIT_ID: "20"
      TRAIT: bms
      POINT_SET: bms
      REDIS_HOST: redis
      MONGO_HOST: host.docker.internal
      BMS_01_MODBUS_HOST: ${BMS_01_MODBUS_HOST:-host.docker.internal}
      BMS_01_MODBUS_PORT: ${BMS_01_MODBUS_PORT:-5020}

  acm_01:
    <<: *device-common
    container_name: csp-acm-01
    environment:
      DEVICE_ID: acm_01
      UNIT_ID: "1"
      TRAIT: meter
      POINT_SET: meter
      REDIS_HOST: redis
      MONGO_HOST: host.docker.internal
      ACM_01_MODBUS_HOST: ${ACM_01_MODBUS_HOST:-host.docker.internal}
      ACM_01_MODBUS_PORT: ${ACM_01_MODBUS_PORT:-5020}

  solar_01:
    <<: *device-common
    container_name: csp-solar-01
    environment:
      DEVICE_ID: solar_01
      UNIT_ID: "30"
      TRAIT: solar
      POINT_SET: solar
      REDIS_HOST: redis
      MONGO_HOST: host.docker.internal
      SOLAR_01_MODBUS_HOST: ${SOLAR_01_MODBUS_HOST:-host.docker.internal}
      SOLAR_01_MODBUS_PORT: ${SOLAR_01_MODBUS_PORT:-5020}

  load_01:
    <<: *device-common
    container_name: csp-load-01
    environment:
      DEVICE_ID: load_01
      UNIT_ID: "40"
      TRAIT: load
      POINT_SET: load
      REDIS_HOST: redis
      MONGO_HOST: host.docker.internal
      LOAD_01_MODBUS_HOST: ${LOAD_01_MODBUS_HOST:-host.docker.internal}
      LOAD_01_MODBUS_PORT: ${LOAD_01_MODBUS_PORT:-5020}

  # ===== Strategy =====
  strategy:
    build:
      context: ../../..                     # repo 根目錄
      dockerfile: examples/GC_PB_test/docker_test/Dockerfile
    image: csp-gc-pb:latest                 # 5 個 device 共用此 tag
    container_name: csp-strategy
    depends_on: [redis, pcs_01, bms_01, acm_01, solar_01, load_01]
    restart: unless-stopped
    command: ["python", "strategy_main.py"]
    networks: [csp-net]
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      REDIS_HOST: redis
      SITE_CONFIG: /app/examples/GC_PB_test/docker_test/config/${SITE_CONFIG_FILE:-site_sim.yaml}
    volumes:
      # 改 YAML 不用 rebuild image
      - ./config:/app/examples/GC_PB_test/docker_test/config:ro

volumes:
  csp-redis-data:

networks:
  csp-net:
    driver: bridge
```

**設計要點**：
- 只有 `strategy` 服務帶 `build:`，compose 會 build 一次產出 `csp-gc-pb:latest`，其他 5 個 device 以 `image:` 引用。
- YAML anchor `&device-common` 把 5 個 device 共用欄位抽出；每個 device 只差 `environment` 與 `container_name`。
- `extra_hosts: host.docker.internal:host-gateway` 讓 Linux 也支援 host.docker.internal；Windows/Mac 原本就支援。
- config 以 bind mount 覆寫 image 裡的版本 → 改 YAML 只要 `docker compose restart strategy`。

### 2.4 `docker_test/config/site_sim.yaml`

```yaml
# 站型：模擬環境（與原 strategy_main.py 硬編碼行為一致）
sites:
  - {site_id: site_pcs,   devices: [pcs_01]}
  - {site_id: site_bms,   devices: [bms_01]}
  - {site_id: site_acm,   devices: [acm_01]}
  - {site_id: site_solar, devices: [solar_01]}
  - {site_id: site_load,  devices: [load_01]}

trait_device_map:
  pcs:   [pcs_01]
  bms:   [bms_01]
  meter: [acm_01]
  solar: [solar_01]
  load:  [load_01]

context_maps:
  - {device_id: bms_01,   point: soc,           target: soc}
  - {device_id: pcs_01,   point: q_actual,      target: extra.pcs_q}
  - {device_id: acm_01,   point: active_power,  target: extra.meter_power}
  - {device_id: acm_01,   point: frequency,     target: extra.frequency}
  - {device_id: acm_01,   point: voltage_a,     target: extra.voltage}
  - {device_id: solar_01, point: ac_power,      target: extra.solar_power}
  - {device_id: load_01,  point: p_actual,      target: extra.load_power}

command_maps:
  - {device_id: pcs_01, field: p_target, point: p_setpoint}
  - {device_id: pcs_01, field: q_target, point: q_setpoint}

protection:
  soc: {soc_high: 95.0, soc_low: 5.0, warning_band: 5.0}

modes:
  base: stop
  entries:
    - {name: stop,    type: StopStrategy,              priority: SCHEDULE, desc: "停止模式"}
    - {name: pq_mode, type: PQ_ramp_Time_ModeStrategy, priority: MANUAL,   desc: "時間 ramp PQ 模式"}

poll_interval: 1.0
auto_stop: true
system_alarm_on_device_offline: true
```

### 2.5 `docker_test/.env.example`

```dotenv
# 選擇站型
SITE_CONFIG_FILE=site_sim.yaml

# 預設所有 device 的 Modbus 都指向 host sim (host.docker.internal:5020)
# 現場部署改為真實 IP：
# PCS_01_MODBUS_HOST=192.168.1.10
# BMS_01_MODBUS_HOST=192.168.1.11
# ACM_01_MODBUS_HOST=192.168.1.12
# SOLAR_01_MODBUS_HOST=192.168.1.13
# LOAD_01_MODBUS_HOST=192.168.1.14
```

### 2.6 `docker_test/README.md`

精簡操作指南，內容涵蓋：
- 前置（host 端）：停原生 Redis、啟 sim.py、確認 Mongo
- 啟停 compose 指令
- 換 YAML、加 device 範例
- 驗證指令
- 常見錯誤排查

---

## 三、快速部署流程

### 3.1 前置（host 端）

```bash
# 1. 停主機原生 Redis，避免 port 6379 衝突
net stop Redis

# 2. 下架舊獨立 csp-redis（若有），volume 保留
docker stop csp-redis 2>/dev/null && docker rm csp-redis 2>/dev/null

# 3. 開一個 terminal 跑 sim.py
cd examples/GC_PB_test
python sim.py
# 應看到 [SIM] listening on 127.0.0.1:5020

# 4. 確認 host 上的 Mongo
mongosh --eval "db.runCommand({ping: 1})"
```

### 3.2 啟動 Docker stack

```bash
cd examples/GC_PB_test/docker_test
docker compose up -d --build

# 檢查
docker compose ps
docker compose logs -f strategy
```

### 3.3 現場部署（真實硬體）

```bash
cd examples/GC_PB_test/docker_test
cp .env.example .env
# 編輯 .env：改 SITE_CONFIG_FILE + 各設備真實 IP
cp config/site_sim.yaml config/site_A.yaml
# 編輯 site_A.yaml 調整設備組成

docker compose up -d
```

### 3.4 換 YAML（不換 image）

```bash
vim config/site_A.yaml
docker compose restart strategy
```

### 3.5 新增第二台 PCS

在 `docker-compose.yml` 加：

```yaml
  pcs_02:
    <<: *device-common
    container_name: csp-pcs-02
    environment:
      DEVICE_ID: pcs_02
      UNIT_ID: "11"
      TRAIT: pcs
      POINT_SET: pcs
      REDIS_HOST: redis
      MONGO_HOST: host.docker.internal
      PCS_02_MODBUS_HOST: ${PCS_02_MODBUS_HOST:-192.168.1.20}
      PCS_02_MODBUS_PORT: ${PCS_02_MODBUS_PORT:-5020}
```

`site_A.yaml` 加：
```yaml
sites:
  - {site_id: site_pcs_02, devices: [pcs_02]}
trait_device_map:
  pcs: [pcs_01, pcs_02]
```

```bash
docker compose up -d pcs_02
docker compose restart strategy
```

**整趟零 .py 改動。**

---

## 四、實現效果對照

| 情境 | 舊做法（5 支獨立 .py） | 新做法（generic + YAML） |
|------|------|------|
| 新增第二台 PCS | 複製 device_pcs_main.py → device_pcs_02_main.py 改常數 | compose 加 10 行 service |
| 新廠牌 PCS（register 不同） | 改 device.py + 改 device_pcs_main.py import | device.py 新增一筆 `POINT_SETS["pcs_sma"]` |
| A 廠沒 Solar | 改 strategy_main.py 移除 solar map | 改用 site_A.yaml（少幾行 map） |
| 測試 ↔ 現場切換 | 改 source code | 改 `.env` 的 SITE_CONFIG_FILE + MODBUS_HOST |
| Image rebuild 次數 | 每次現場變動 | 幾乎為零 |

---

## 五、驗證步驟

1. **sim / Mongo 都在 host 跑**：
   ```bash
   netstat -an | grep 5020    # sim listening
   netstat -an | grep 27017   # mongo listening
   ```

2. **Redis 容器通、device 有寫 state**：
   ```bash
   docker exec -it csp-redis redis-cli KEYS 'device:*:state'
   # 預期 5 個 key
   docker exec -it csp-redis redis-cli HGETALL device:bms_01:state
   ```

3. **容器能連到 host 的 sim**：
   ```bash
   docker compose logs pcs_01 | grep modbus
   # 應出現 "running as pcs_01 (pcs) → modbus host.docker.internal:5020"，無 ConnectionRefused
   ```

4. **Mongo 從容器寫入成功**：
   ```bash
   mongosh demo --eval "db.commands.stats()"
   ```

5. **YAML 生效**：修改 `site_sim.yaml` 砍掉 solar 那行 `context_maps` → `docker compose restart strategy` → log 反映少一個 mapping。

6. **EMS 命令測試**（ems_cli.py 在 host 跑）：
   ```bash
   cd examples/GC_PB_test
   python ems_cli.py          # 預設連 localhost:6379 剛好對到 compose 曝露的 port
   ```

---

## 六、關鍵檔案總覽

| 動作 | 檔案 |
|------|------|
| 新建 | `examples/GC_PB_test/env_config.py` |
| 新建 | `examples/GC_PB_test/device_main.py` |
| 新建 | `examples/GC_PB_test/docker_test/Dockerfile` |
| 新建 | `examples/GC_PB_test/docker_test/docker-compose.yml` |
| 新建 | `examples/GC_PB_test/docker_test/.dockerignore` |
| 新建 | `examples/GC_PB_test/docker_test/.env.example` |
| 新建 | `examples/GC_PB_test/docker_test/README.md` |
| 新建 | `examples/GC_PB_test/docker_test/config/site_sim.yaml` |
| 修改 | `examples/GC_PB_test/device.py`（末尾加 POINT_SETS） |
| 改寫 | `examples/GC_PB_test/strategy_main.py`（讀 YAML） |
| 修改 | [../../pyproject.toml](../../pyproject.toml)（加 pyyaml） |
| 零改動 | `examples/GC_PB_test/sim.py` |
| 零改動 | `redis_listener.py` / `ems_cli.py` / `strategy.py` / `protection.py` / `main.py` |
| 刪除 | `device_pcs_main.py` / `device_bms_main.py` / `device_acm_main.py` / `device_solar_main.py` / `device_load_main.py` |

---

## 七、風險與注意事項

- **sim.py 必須先在 host 啟動**：否則 5 個 device 容器都會 retry 連 5020。`docker_test/README.md` 會明顯標示這點。
- **host.docker.internal 跨平台**：Windows/Mac Docker Desktop 內建支援；Linux 需 `extra_hosts: host-gateway`（compose v3.x 支援）。本計畫已加入。
- **Redis volume 遷移**：`csp-redis-data` volume 名稱沿用，compose `up` 自動接手舊資料。若先前以 anonymous volume 跑則資料會遺失 → 先 `docker volume ls` 確認。
- **Mongo 留 host 的連線**：host 上 MongoDB 預設 `bindIp=127.0.0.1`，容器看到的 host-gateway 不是 127.0.0.1，可能無法連。若出現連線失敗，需把 `bindIp` 加 `0.0.0.0` 或對應 docker bridge IP（預設 `172.17.0.1` 一類）。`docker_test/README.md` 會寫排錯步驟。
- **YAML 安全**：用 `yaml.safe_load` + 白名單 `STRATEGY_REGISTRY`，避免任意 class 反序列化。不要改用 `yaml.load`。
- **pyyaml 相依**：加到 core dependencies 會讓所有 csp_lib 使用者都裝 pyyaml（約 200KB），影響極小。若要嚴格隔離，可另開 `[config]` extras 僅供範例使用，但需同步更新文件。
- **跨主機 HA**：compose 單機 bridge 不涵蓋跨實體機 failover。那條路線走 [01_REDUNDANCY_PLAN.md](./01_REDUNDANCY_PLAN.md) 的 etcd + leader election，是獨立 initiative。
- **Health check**：目前依 `depends_on` 單純啟動順序。若 strategy 啟動時 device 尚未 publish state，可能看到 offline alarm；必要時加 `healthcheck` + `condition: service_healthy`。本次不做。

---

## 八、與既有文件關係

- [00_Docker Redis Setup.md](./00_Docker%20Redis%20Setup.md)：只跑 Redis 單容器的情境文件。本計畫取代它作為「完整 stack」的部署參考，但 00 仍保留作為 Redis 單獨部署的精簡版。
- [01_REDUNDANCY_PLAN.md](./01_REDUNDANCY_PLAN.md)：Active-Standby HA 設計（etcd leader election + 雙機部署）。本計畫是它的**單機前置**：先把單機完整跑通，再擴到多機 HA。
