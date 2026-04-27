# GC_PB_test 拆分架構加入多主機 Active-Standby 備援 — 計畫書（v2）

> 本文件僅為設計計畫，尚未實作。
> **v2 變更**：選主後端改為 **etcd**，最大化復用 `csp_lib.cluster` 現有原語；放棄跨主機優先機制。

## Context

`examples/GC_PB_test/` 已將原本的 `main.py` 拆分為 1 個 strategy process + 5 個 device process + 1 個 Modbus 模擬器，全數透過 Redis 解耦。目前任一 process 掛掉即無自動恢復，Redis 斷線也無重連。

目標：讓 strategy 與每個 device 都能在兩台主機上以 Active-Standby 方式部署，任一端崩潰或整台主機掛掉時，另一台自動接管，並根據 device 關鍵性分級降級。為**實際部署**準備，交付 docker-compose + verify 腳本 + README。

### 已鎖定的決策（v2）

| 項目 | 決策 |
|---|---|
| 選主後端 | **etcd**（透過 [csp_lib.cluster.LeaderElector](../../csp_lib/cluster/election.py)） |
| Redis HA | 本期不做；client 層預留 retry/reconnect，Sentinel 之後改 env 一行切換 |
| 位置訓果 | **先到先得**（放棄跨主機優先，接受 `txn_put_if_not_exists` 原生行為） |
| Instance 命名 | 邏輯 `device_id` 不變；`ClusterConfig.instance_id = f"{device_id}#{host_id}"` |
| 降級策略 | `pcs_01/bms_01/acm_01` CRITICAL → 全掛則停機；`solar_01/load_01` NON_CRITICAL → 全掛則降級 |
| **不修改 csp_lib** | 全部新增邏輯留在 `examples/GC_PB_test/` 底下 |

### csp_lib.cluster 與當前拆分架構的不匹配

`csp_lib.cluster.ClusterController` 假設**單體 process 內含 `UnifiedDeviceManager`**（controller.py:97-190 的 leader 流程會 `await unified_manager.start()`）。使用者拆分架構下 strategy process 不直接連設備，因此**不能直接使用 `ClusterController`**，但可借用其內部原語。

---

## 架構圖

```
                  ┌────────────────────────────────────────┐
                  │               etcd cluster             │
                  │  /csp/strategy/election                │
                  │  /csp/device/pcs_01/election           │
                  │  /csp/device/bms_01/election           │
                  │  /csp/device/acm_01/election           │
                  │  /csp/device/solar_01/election         │
                  │  /csp/device/load_01/election          │
                  └────────────▲───────────────▲───────────┘
                               │               │
            ┌──────────────────┴──┐       ┌────┴───────────────┐
            │                     │       │                    │
            │                ┌────┴───────┴────┐               │
            │                │      Redis      │               │
            │                │ device:{id}:*   │               │
            │                │ cluster:{ns}:*  │               │
            │                │ gc:command chan │               │
            │                └────▲───────────▲┘               │
            │                     │           │                │
    ┌───────┴───────────┐         │           │      ┌─────────┴─────────┐
    │  Host A (HOST=A)  │         │           │      │  Host B (HOST=B)  │
    │  strategy#A  STBY │─────────┘           └──────│  strategy#B  ACT  │
    │  pcs_01#A    ACT  │◄─── sim.py :5020 ────────►│  pcs_01#B    STBY │
    │  bms_01#A    STBY │    (只有 leader 開 TCP)   │  bms_01#B    ACT  │
    │  acm_01#A    ACT  │                           │  acm_01#B    STBY │
    │  solar_01#A  STBY │                           │  solar_01#B  ACT  │
    │  load_01#A   ACT  │                           │  load_01#B   STBY │
    └───────────────────┘                           └───────────────────┘
```

強制規則：只有 ACTIVE 才會 (a) 打開 Modbus 連線、(b) 寫 `device:{id}:state`、(c) publish command result。STANDBY instance 只持有 `LeaderElector`，其他資源完全未建構。

---

## csp_lib 復用對應表

| 計畫需求 | csp_lib 對應（**直接復用**） | 檔案 |
|---|---|---|
| Leader election + lease + keepalive + demotion | `LeaderElector` + `ClusterConfig` + `EtcdConfig` | [csp_lib/cluster/election.py:31](../../csp_lib/cluster/election.py#L31), [csp_lib/cluster/config.py](../../csp_lib/cluster/config.py) |
| Leader 狀態 → Redis（mode state / protection / commands） | `ClusterStatePublisher` | [csp_lib/cluster/sync.py:70](../../csp_lib/cluster/sync.py#L70) |
| Follower 讀 Redis 狀態（若要熱備） | `ClusterStateSubscriber` + `ClusterSnapshot` | [csp_lib/cluster/sync.py](../../csp_lib/cluster/sync.py) |
| Follower virtual context（若要 dry-run 策略） | `VirtualContextBuilder` | [csp_lib/cluster/context.py](../../csp_lib/cluster/context.py) |
| 既有 offline 偵測 + system_alarm | `DeviceStateSubscriber` + `DistributedConfig.system_alarm_on_device_offline` | [csp_lib/integration/distributed/subscriber.py](../../csp_lib/integration/distributed/subscriber.py) |
| 告警 → 停機兜底 | `auto_stop_on_alarm` | [csp_lib/integration/system_controller.py:113](../../csp_lib/integration/system_controller.py#L113) |
| 分級降級 integration point | `builder.alarm_mode_per_device(on_alarm=...)` | [csp_lib/integration/system_controller.py:327](../../csp_lib/integration/system_controller.py#L327) |
| 重試與斷路器 | `CircuitBreaker`, `RetryPolicy` | [csp_lib/core/resilience.py:26-126](../../csp_lib/core/resilience.py#L26-L126) |
| 生命週期 | `AsyncLifecycleMixin` | [csp_lib/core/lifecycle.py](../../csp_lib/core/lifecycle.py) |
| Redis Sentinel（未來切換） | `RedisConfig.sentinels` + `is_sentinel_mode` | [csp_lib/redis/config.py:65-86](../../csp_lib/redis/config.py#L65-L86) |

---

## 新增模組（皆置於 `examples/GC_PB_test/`）

### `instance_identity.py`
- `resolve_host_id()`：讀 `HOST_ID` env，否則 `socket.gethostname()`
- `build_instance_id(logical_id: str, host_id: str) -> str`：回傳 `f"{logical_id}#{host_id}"`，供 `ClusterConfig.instance_id`

### `pubsub_resilient.py`
EMS pub/sub 自動重訂閱。csp_lib 沒有提供此原語。
- `ResilientSubscriber(AsyncLifecycleMixin)`：
  - 外層無限迴圈訂閱、內層 `async for msg in pubsub.listen()`
  - 例外 → `CircuitBreaker` + `RetryPolicy` 退避後重新訂閱
- 取代 `redis_listener.py` 目前的裸訂閱

### `redis_resilient.py`
- `build_redis_client(env)`：統一從 env 建 `RedisClient`；支援 `REDIS_URL` 或 `REDIS_SENTINELS`/`REDIS_MASTER`。未來切 Sentinel 只改 env
- `connect_with_retry(client, retry, breaker)`：重試 connect

### `device_runner_ha.py`
包住每個 device process 的 HA 執行器。**每個 device process 各自 instantiate 一個 `LeaderElector`**。
- `DeviceSpec(device_id, site_id, unit_id, trait, criticality, read_points, write_points)`
- `HADeviceRunner.run()`：
  1. 建立 Redis client（retry）
  2. 加入 instance roster：`SADD device:{id}:instances {instance_id}` + 背景每 2s `EXPIRE 6`；結束時 `SREM`
  3. 建 `ClusterConfig(instance_id=f"{device_id}#{host_id}", election_key=f"/csp/device/{device_id}/election", etcd=EtcdConfig(endpoints=[etcd_endpoint]))`
  4. 建 `LeaderElector(config, on_elected=_become_active, on_demoted=_become_standby)`
  5. 啟動 elector；`await asyncio.Event().wait()` 直到 SIGTERM
- `_become_active`：建 `AsyncModbusDevice` + `UnifiedDeviceManager` + `RemoteSiteRunner` + `MongoBatchUploader`；依序 `__aenter__`（這裡才打開 Modbus + publish state）
- `_become_standby`：反序 teardown，`asyncio.wait_for(..., lease_ttl*0.6)`；超時 → `os._exit(1)` 讓容器重啟（避免 Modbus socket 重疊）

### `strategy_runner_ha.py`
仿 `ClusterController` 的簡化版，移除 `UnifiedDeviceManager` 依賴。
- `HAStrategyRunner`：
  1. 建 Redis client（retry）
  2. 建 `ClusterConfig(instance_id=f"strategy#{host_id}", election_key="/csp/strategy/election", etcd=EtcdConfig(...))`
  3. 建 `LeaderElector(config, on_elected=_promote, on_demoted=_demote)`
  4. **Follower 模式**：不啟動 `DistributedController`、不訂 `EMSCommandListener`（純待命）
  5. `on_elected (_promote)`：
     - 建 `SystemController` + `DistributedController` + `EMSCommandListener`（由 `ResilientSubscriber` 驅動 `_handle`）
     - `await dist_controller.__aenter__()`; `ems_listener.start()`
     - （可選）建 `ClusterStatePublisher(config, redis, mode_manager=sc.mode_manager, protection_guard=sc.protection_guard, get_last_command=..., get_auto_stop=...)`；啟動之
  6. `on_demoted (_demote)`：
     - 先停 `ClusterStatePublisher`、`ems_listener`
     - `dist_controller.__aexit__()`

### `criticality.py`
```
DEVICE_CRITICALITY = {
    "pcs_01": "critical",  "bms_01": "critical",  "acm_01": "critical",
    "solar_01": "non_critical",  "load_01": "non_critical",
}
```
`read_criticality(device_id, env)` — env `DEVICE_CRITICALITY` 覆蓋預設，供 docker-compose 調整

### （不再需要）
- ❌ `redis_lease.py`（改用 csp_lib etcd `LeaderElector`）
- ❌ `leader_election.py`（csp_lib 已提供）

---

## 既有檔案修改

### 5 個 `device_{pcs,bms,acm,solar,load}_main.py`
- `main()` 簡化為約 10 行：組 `DeviceSpec` + `resolve_host_id()` + `ClusterConfig` → `HADeviceRunner(...).run()`
- 原本頂層的 `redis_client.connect()`、`AsyncModbusDevice(...)`、`MongoBatchUploader` 等建構**全部搬進 `HADeviceRunner._become_active`**，只在 leader 時才建

### `strategy_main.py`
- 保留 `SystemControllerConfig.builder()` 鏈（第 41–59 行）不動，但：
  - **新增**：改用 `builder.alarm_mode_per_device(on_alarm=_degradation_handler)`（[csp_lib/integration/system_controller.py:327](../../csp_lib/integration/system_controller.py#L327) 的 builder method；注意不是直接設 `alarm_mode="per_device"`）
  - `_degradation_handler` 查 `DEVICE_CRITICALITY[device_id]`：`critical` → `await controller.push_override("stop")`；`non_critical` → `logger.warning` + 設 degraded flag
- 把第 37–107 行包進 `HAStrategyRunner._promote`

### `redis_listener.py`
- 把每筆訊息的處理拆成 `EMSCommandListener._handle(msg: str)`，讓 `ResilientSubscriber` 可注入
- 保留 `start/stop` API 向下相容（docstring 標記 deprecated）

### `csp_lib` — 不動
本期不修改 csp_lib。若要把跨主機優先機制加回來，是後續的獨立任務（會動 `LeaderElector`）。

### 新增 Python 依賴
`pip install "csp0924_lib[cluster]"`（含 `etcetra` etcd client）。必須在 requirements 更新，否則 `from csp_lib.cluster import ...` 會 ImportError（見 [csp_lib/cluster/__init__.py:18-21](../../csp_lib/cluster/__init__.py#L18-L21)）。

---

## etcd + Redis Key Schema

### etcd keys（由 csp_lib.cluster 管理）

| Key | 用途 | TTL |
|---|---|---|
| `/csp/strategy/election` | strategy 選主 | `ClusterConfig.lease_ttl=10` |
| `/csp/device/{device_id}/election` | 每個 device 獨立選主（5 個 key） | 10s |

### Redis keys

| Key | 類型 | 寫入方 | 用途 | TTL |
|---|---|---|---|---|
| `cluster:default:leader` | STRING (JSON) | `ClusterStatePublisher` | strategy leader 身份廣播 | `state_ttl=30s` |
| `cluster:default:mode_state` | HASH | `ClusterStatePublisher` | base_modes / overrides / effective_mode | 30s |
| `cluster:default:protection_state` | HASH | `ClusterStatePublisher` | 保護規則觸發 | 30s |
| `channel:cluster:default:leader_change` | PUB/SUB | `ClusterStatePublisher` | leader 變更通知 | — |
| `device:{device_id}:instances` | SET | `HADeviceRunner` roster | 活躍 instance 清單（監控用） | EXPIRE 6s |
| `device:{id}:state` / `:online` / `:alarms` | 既有 | 只有 ACTIVE device 寫 | 既有 | 既有 |
| `gc:command`, `commands:{site}:write/result` | PUB/SUB | 既有 | 既有 | — |

---

## docker-compose.yml

部署在單一 Docker 主機模擬兩台邏輯 host（`HOST_ID` env 區分）。

Services：
- `etcd`（`bitnami/etcd:3.5`，單節點開發；實際生產至少 3 節點）
  - env `ALLOW_NONE_AUTHENTICATION=yes`, `ETCD_ADVERTISE_CLIENT_URLS=http://etcd:2379`
  - 健康檢查 `etcdctl endpoint health`
- `redis`（`redis:7-alpine`，healthcheck `redis-cli ping`，named volume）
- `mongo`（`mongo:7`，named volume）
- `sim`（Modbus 模擬器，`restart: unless-stopped`）
- **10 個 device 容器**：`{pcs,bms,acm,solar,load}_{a,b}`；env：
  - `HOST_ID=hostA|hostB`
  - `REDIS_URL=redis://redis:6379`
  - `ETCD_ENDPOINT=etcd:2379`
  - `SIM_HOST=sim`
  - `DEVICE_CRITICALITY`
- **2 個 strategy 容器**：`strategy_a`、`strategy_b`
- 全部 `restart: unless-stopped`
- 單一 bridge network

---

## 各情境恢復行為

| 情境 | 偵測時間 | 恢復時間 | 過渡狀態 |
|---|---|---|---|
| ACTIVE device crash | etcd lease TTL 10s（keepalive 間隔 `ttl/3 ≈ 3.3s`） | peer FOLLOWER 的 `_wait_for_leader_loss` poll 週期（`lease_ttl/2 = 5s`）偵測到 key 消失後立即 campaign | `device:{id}:online=0` 最多 ~10s；critical 可能短暫觸發 alarm |
| ACTIVE strategy crash | 10s | peer 重建 `SystemController` + `DistributedController` + `EMSCommandListener`，~10–15s（含 `failover_grace_period=2s`） | 期間無命令發出；device 持續送狀態 |
| 主機斷電（帶走 1 strategy + 5 device） | 10s | 倖存主機 6 個並行選主，立即得手 | 10s 視窗；critical alarm 可能暫態觸發 |
| etcd 短暫不可用 | keepalive 失敗重試 `max_keepalive_failures=3` | 超出則 leader self-fence → demotion → 兩 host 重新 campaign | 視為短暫分區 |
| Redis blip 5s | `CircuitBreaker` 跳、state publish 失敗 | `ResilientSubscriber` 退避重訂 pub/sub；狀態寫入下一個週期補齊 | EMS 命令可能丟失 |
| 兩個 CRITICAL 實例都掛 | `device:pcs_01:online=0` | — | `on_device_alarm` callback → `push_override("stop")`；既有 `auto_stop_on_alarm=True` 兜底 |
| 兩個 NON-CRITICAL 實例都掛 | 同上 | — | `on_device_alarm` callback → log + degraded flag；不停機 |

---

## 驗證計畫（`verify.sh`）

前置：`docker compose up -d`

1. **Device failover**：`docker kill GC_PB_test-pcs_a-1`；12s 內 `etcdctl get /csp/device/pcs_01/election` 值應從 `pcs_01#hostA@...` 變成 `pcs_01#hostB@...`；`redis-cli monitor | grep 'device:pcs_01:state'` 繼續更新
2. **Strategy failover**：`docker kill strategy_a`（若為 leader）；12s 內 `ems_cli set_mode pq_mode` 應成功；`redis-cli get cluster:default:leader` 的 `instance_id` 變成 `strategy#hostB`
3. **etcd 短暫故障**：`docker pause etcd && sleep 15 && docker unpause etcd`；驗證兩 host 收斂到單一 leader（`etcdctl get /csp/strategy/election` 只返回一筆），過程中沒有 dual-leader
4. **Redis blip**：`docker pause redis && sleep 5 && docker unpause redis`；驗證 `ResilientSubscriber` 自動重訂，EMS 命令在 blip 後恢復可用
5. **Both NON-CRITICAL down**：`docker kill solar_a solar_b`；驗證無 stop override 推入、log 出現 `NON_CRITICAL solar_01 offline — degraded` warning
6. **Both CRITICAL down**：`docker kill pcs_a pcs_b`；驗證 stop override 已推入

---

## Open Risks

- **etcd 是新增 SPOF**：單節點 etcd 掛掉 → 全系統選主癱瘓。本期開發用單節點；生產必須 3 節點 cluster
- **Modbus 單一連線重疊**：losing leader 須在 `lease_ttl * 0.6 ≈ 6s` 內釋放。超時後 `os._exit(1)` 讓容器重啟，避免卡住導致 peer 無法連
- **EMS pub/sub 在 strategy failover 期間丟訊息**：後續可改 Redis Streams + consumer group，**本期接受**
- **初始部署 active 固定在先啟動的 host**：先到先得意味著 host A 先啟動就會拿下所有 lease。若要平衡，需手動 restart 某些 process 觸發重選
- **etcd 與 Redis 時鐘不同步**：etcd 管 election lease，Redis 管狀態 TTL，兩者各自時間基準。`state_ttl=30s` 比 `lease_ttl=10s` 寬鬆，可容忍偏移
- **`ClusterController` 原生功能未用到**：本計畫只借 csp_lib 原語，未來若想接上 `ClusterStateSubscriber` 做 follower 熱備，可在 Phase 7 加回

---

## 關鍵檔案清單

**新增**
- [examples/GC_PB_test/instance_identity.py](instance_identity.py)
- [examples/GC_PB_test/pubsub_resilient.py](pubsub_resilient.py)
- [examples/GC_PB_test/redis_resilient.py](redis_resilient.py)
- [examples/GC_PB_test/device_runner_ha.py](device_runner_ha.py)
- [examples/GC_PB_test/strategy_runner_ha.py](strategy_runner_ha.py)
- [examples/GC_PB_test/criticality.py](criticality.py)
- [examples/GC_PB_test/docker-compose.yml](docker-compose.yml)
- [examples/GC_PB_test/verify.sh](verify.sh)

**修改**
- [examples/GC_PB_test/strategy_main.py](strategy_main.py)
- [examples/GC_PB_test/device_pcs_main.py](device_pcs_main.py)
- [examples/GC_PB_test/device_bms_main.py](device_bms_main.py)
- [examples/GC_PB_test/device_acm_main.py](device_acm_main.py)
- [examples/GC_PB_test/device_solar_main.py](device_solar_main.py)
- [examples/GC_PB_test/device_load_main.py](device_load_main.py)
- [examples/GC_PB_test/redis_listener.py](redis_listener.py)
- `requirements.txt` / `pyproject.toml`：加入 `csp0924_lib[cluster]` extra

**復用（不修改）**
- [csp_lib/cluster/election.py:31](../../csp_lib/cluster/election.py#L31) — `LeaderElector`
- [csp_lib/cluster/config.py](../../csp_lib/cluster/config.py) — `ClusterConfig`, `EtcdConfig`
- [csp_lib/cluster/sync.py:70](../../csp_lib/cluster/sync.py#L70) — `ClusterStatePublisher`（可選）
- [csp_lib/cluster/context.py](../../csp_lib/cluster/context.py) — `VirtualContextBuilder`（可選，本期不用）
- [csp_lib/core/resilience.py:26-126](../../csp_lib/core/resilience.py#L26-L126) — `CircuitBreaker`, `RetryPolicy`
- [csp_lib/core/lifecycle.py](../../csp_lib/core/lifecycle.py) — `AsyncLifecycleMixin`
- [csp_lib/integration/system_controller.py:327](../../csp_lib/integration/system_controller.py#L327) — `builder.alarm_mode_per_device()`
- [csp_lib/integration/distributed/subscriber.py](../../csp_lib/integration/distributed/subscriber.py) — 既有 offline detection
- [csp_lib/redis/config.py:65-86](../../csp_lib/redis/config.py#L65-L86) — Sentinel config（未來切換用）

---

## 實施順序（每階段都可停）

1. **Phase 1 — Identity + Redis 韌性**（~0.5 天）
   新增 `instance_identity.py`、`redis_resilient.py`、`pubsub_resilient.py`。在既有 `strategy_main.py` 套 `ResilientSubscriber`（還沒 election）。成果：pub/sub 扛得住 Redis blip
2. **Phase 2 — 接上 csp_lib.cluster 原語**（~0.5 天）
   安裝 `csp0924_lib[cluster]`；啟動 etcd；寫單元測試驗證兩個 process 能正確選主 `/csp/test/election`
3. **Phase 3 — HA device runner**（~1 天）
   新增 `device_runner_ha.py`；先改 `device_pcs_main.py` 一個；兩 process + 單 sim 驗證 failover；成功後複製到其他四個
4. **Phase 4 — HA strategy runner**（~0.5 天）
   新增 `strategy_runner_ha.py`；移植 `strategy_main.py` 的建構邏輯到 `_promote`；（可選）接上 `ClusterStatePublisher`
5. **Phase 5 — Criticality + 分級降級**（~0.5 天）
   新增 `criticality.py`、`strategy_main.py` 改用 `builder.alarm_mode_per_device` + callback
6. **Phase 6 — docker-compose + verify.sh + README**（~1 天，含 etcd 配置）
7. **Phase 7（延後）** — 接上 `ClusterStateSubscriber` 讓 follower 熱備；啟用 Redis Sentinel；EMS 改 Redis Streams；若需要跨主機優先，於此階段擴充 csp_lib `LeaderElector`
