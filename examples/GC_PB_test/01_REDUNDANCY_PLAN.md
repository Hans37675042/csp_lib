# GC_PB_test 拆分架構加入多主機 Active-Standby 備援 — 計畫書

> 本文件僅為設計計畫，尚未實作。實作前請再次檢視各 Phase 範圍與驗證方式。

## Context

`examples/GC_PB_test/` 已將原本的 `main.py` 拆分為 1 個 strategy process + 5 個 device process + 1 個 Modbus 模擬器，全數透過 Redis 解耦。目前任一 process 掛掉即無自動恢復，Redis 斷線也無重連。

目標：讓 strategy 與每個 device 都能在兩台主機上以 Active-Standby 方式部署，任一端崩潰或整台主機掛掉時，另一台自動接管，並根據 device 關鍵性分級降級。為**實際部署**準備，交付 docker-compose + verify 腳本 + README。

### 已鎖定的決策

| 項目 | 決策 |
|---|---|
| 選主後端 | Redis Lock（SET NX EX + Lua compare-renew/compare-release） |
| Redis HA | 本期不做；client 層預留 retry/reconnect，Sentinel 之後一行切換 |
| 位置訓果 | 跨主機優先（priority sleep bonus） |
| Instance 命名 | 邏輯 `device_id` 不變；另帶 `instance_id = f"{device_id}#{host_id}"` |
| 降級策略 | `pcs_01/bms_01/acm_01` CRITICAL → 全掛則停機；`solar_01/load_01` NON_CRITICAL → 全掛則降級 |

---

## 架構圖

```
            ┌──────────────────────────────────────────────────────────────┐
            │                           Redis                              │
            │  lease:device:{device_id}  = "{instance_id}|{host_id}|{ts}"  │
            │  lease:strategy            = "{instance_id}|{host_id}|{ts}"  │
            │  device:{id}:instances (SET, periodic EXPIRE 6s)             │
            │  device:{id}:state / :online / :alarms   (既有)              │
            │  channel: gc:command, commands:{site}:write/result           │
            └───────▲───────────────────────────────────────────▲──────────┘
                    │                                           │
        ┌───────────┴───────────┐                 ┌─────────────┴──────────┐
        │  Host A (HOST_ID=A)   │                 │  Host B (HOST_ID=B)    │
        │  strategy#A   STANDBY │                 │  strategy#B   ACTIVE   │
        │  pcs_01#A     ACTIVE  │◄── sim.py :5020 ──► pcs_01#B     STANDBY │
        │  bms_01#A     STANDBY │   (只有 leader  │  bms_01#B     ACTIVE   │
        │  acm_01#A     ACTIVE  │    持有 TCP)    │  acm_01#B     STANDBY  │
        │  solar_01#A   STANDBY │                 │  solar_01#B   ACTIVE   │
        │  load_01#A    ACTIVE  │                 │  load_01#B    STANDBY  │
        └───────────────────────┘                 └────────────────────────┘
```

強制規則：只有 ACTIVE 才會 (a) 打開 Modbus 連線、(b) 寫 `device:{id}:state`、(c) publish command result。STANDBY 僅訂閱 leadership 狀態。

---

## 新增模組（皆置於 `examples/GC_PB_test/`）

### `instance_identity.py`
- `resolve_host_id()`：讀 `HOST_ID` env，否則 `socket.gethostname()`
- `InstanceIdentity(host_id, logical_id, role)`：`instance_id` property = `f"{logical_id}#{host_id}"`

### `redis_lease.py`
Redis 單一擁有者 lease 原語。
- `LeaseConfig(key, ttl_seconds=10, renew_interval=3.0, acquire_poll=1.0)`
- `RedisLease`：
  - `try_acquire()` — `SET NX EX`，值為 `{instance_id}|{host_id}|{epoch}`
  - `renew()` — Lua: `if GET==self then PEXPIRE else return 0`
  - `release()` — Lua: compare-and-del
  - `read_current()` → `LeaseHolder(instance_id, host_id, acquired_at) | None`
- 以 `CircuitBreaker`（`csp_lib/core/resilience.py:26-106`）包 Redis 呼叫

### `leader_election.py`
- `LeaderElector(AsyncLifecycleMixin)`：
  - state machine `FOLLOWER ↔ LEADER`
  - FOLLOWER 輪詢：計算 priority sleep = `(cross_host_bonus if current.host_id == self.host_id else 0) + uniform(0, 1)`；睡完再 `try_acquire`。這就是「跨主機優先」的實作點
  - LEADER：每 `renew_interval` 秒 renew；失敗即 `on_lose_leader()` → FOLLOWER
  - 回呼：`on_become_leader: Callable[[], Awaitable[None]]`、`on_lose_leader`
- 重用 `csp_lib/core/lifecycle.py` 的 `AsyncLifecycleMixin`

### `pubsub_resilient.py`
- `ResilientSubscriber(AsyncLifecycleMixin)`：外層無限迴圈訂閱、內層 `async for msg in pubsub.listen()`；例外 → `CircuitBreaker` + `RetryPolicy` 退避後重新訂閱
- 取代 `redis_listener.py` 目前裸訂閱的不足

### `redis_resilient.py`
- `build_redis_client(env)`：統一從 env 建 `RedisClient`；支援 `REDIS_URL` 或 `REDIS_SENTINELS`/`REDIS_MASTER`。未來切 Sentinel 只改 env
- `connect_with_retry(client, retry, breaker)`：重試 connect

### `device_runner_ha.py`
統一五個 device 入口的 HA 執行器。
- `DeviceSpec(device_id, site_id, unit_id, trait, criticality, read_points, write_points)`
- `HADeviceRunner.run()`：
  1. 建立 Redis client（retry）
  2. 加入 instance roster：`SADD device:{id}:instances {instance_id}` + 背景每 2s `EXPIRE 6`；結束時 `SREM`
  3. 建 `LeaderElector`，callbacks：
     - `on_become_leader`：建 `AsyncModbusDevice` + `UnifiedDeviceManager` + `RemoteSiteRunner`，依序 `__aenter__`（這裡才打開 Modbus + publish state）
     - `on_lose_leader`：反序 teardown，`asyncio.wait_for(..., ttl*0.6)`；超時 → `os._exit(1)` 讓容器重啟（避免 Modbus socket 重疊）

### `strategy_runner_ha.py`
- `HAStrategyRunner`：
  - `on_become_leader`：建 `SystemController` → `DistributedController` → `EMSCommandListener`（由 `ResilientSubscriber` 驅動 `_handle`）；`dist_controller.__aenter__()`；`ems_listener.start()`
  - `on_lose_leader`：先停 `ems_listener`，再 `dist_controller.__aexit__()`；STANDBY 期間完全不跑 `DistributedController`，避免雙重 `set_mode`

### `criticality.py`
```
DEVICE_CRITICALITY = {
    "pcs_01": "critical",  "bms_01": "critical",  "acm_01": "critical",
    "solar_01": "non_critical",  "load_01": "non_critical",
}
```
`read_criticality(device_id, env)` — env `DEVICE_CRITICALITY` 覆蓋預設，供 docker-compose 調整

---

## 既有檔案修改

### 5 個 `device_{pcs,bms,acm,solar,load}_main.py`
- `main()` 簡化為約 10 行：組 `DeviceSpec` + `InstanceIdentity` → `HADeviceRunner(...).run()`
- 原本頂層的 `redis_client.connect()`、`AsyncModbusDevice(...)`、`MongoBatchUploader` 等建構**全部搬進 `HADeviceRunner.on_become_leader`**，只在 leader 時才建

### `strategy_main.py`
- 保留 `SystemControllerConfig.builder()` 鏈（第 41–59 行）不動，但：
  - **新增**：改用 `builder.alarm_mode_per_device(on_alarm=_degradation_handler)`（見 `csp_lib/integration/system_controller.py:327` 的 builder method；注意不是直接設 `alarm_mode="per_device"`）
  - `_degradation_handler` 查 `DEVICE_CRITICALITY[device_id]`：`critical` → `await controller.push_override("stop")`；`non_critical` → `logger.warning` + 設 degraded flag
- 把第 37–107 行包進 `HAStrategyRunner.on_become_leader`

### `redis_listener.py`
- 把每筆訊息的處理拆成 `EMSCommandListener._handle(msg: str)`，讓 `ResilientSubscriber` 可注入
- 保留 `start/stop` API 向下相容（docstring 標記 deprecated）

### `csp_lib` 不動
日後可把 `redis_lease.py` + `leader_election.py` 抽升到 `csp_lib/integration/leader/`，本期不做

---

## Redis Key 與 Schema

| Key | 類型 | 寫入方 | 用途 | TTL |
|---|---|---|---|---|
| `lease:device:{device_id}` | STRING | `RedisLease` | device 選主 | 10s / renew 3s |
| `lease:strategy` | STRING | `RedisLease` | strategy 選主 | 10s / renew 3s |
| `device:{device_id}:instances` | SET | `HADeviceRunner` roster | 監控用（多少實例活著） | EXPIRE 6s |
| `device:{id}:state` / `:online` / `:alarms` | 既有 | 只有 ACTIVE device 寫 | 既有 | 既有 |
| `gc:command`, `commands:{site}:write/result` | PUB/SUB | — | 既有 | — |

Lease 值格式：`"{instance_id}|{host_id}|{acquired_at_epoch}"`，供 `read_current()` 解析做跨主機評分

---

## docker-compose.yml

部署在單一 Docker 主機模擬兩台邏輯 host（`HOST_ID` env 區分）。

Services：
- `redis`（`redis:7-alpine`，healthcheck `redis-cli ping`，named volume）
- `mongo`（`mongo:7`，named volume）
- `sim`（Modbus 模擬器，`restart: unless-stopped`）
- **10 個 device 容器**：`{pcs,bms,acm,solar,load}_{a,b}`；env `HOST_ID=hostA|hostB`、`REDIS_URL=redis://redis:6379`、`SIM_HOST=sim`、`DEVICE_CRITICALITY`
- **2 個 strategy 容器**：`strategy_a`、`strategy_b`
- 全部 `restart: unless-stopped`
- 單一 bridge network

---

## 各情境恢復行為

| 情境 | 偵測時間 | 恢復時間 | 過渡狀態 |
|---|---|---|---|
| ACTIVE device crash | lease TTL 10s | peer 下一輪 poll（同主機需多等 `cross_host_bonus`，但 peer 在他主機所以快） | `device:{id}:online=0` 最多 ~10s；critical 可能短暫觸發 alarm |
| ACTIVE strategy crash | 10s | peer 重建 controller，~10s | 期間無命令發出；device 持續送狀態 |
| 主機斷電（帶走 1 strategy + 5 device） | 10s | 倖存主機 6 個並行選主（無跨主機對手，立即得手） | 10s 視窗；critical alarm 可能暫態觸發 |
| Redis blip 5s | `CircuitBreaker` 跳、renew 失敗 | 回復後 lease 全過期 → 跨主機偏好主導下一輪 | 見 Open Risks |
| 兩個 CRITICAL 實例都掛 | `device:pcs_01:online=0` | — | `on_device_alarm` callback → `push_override("stop")`；既有 `auto_stop_on_alarm=True`（`system_controller.py:113`）為兜底 |
| 兩個 NON-CRITICAL 實例都掛 | 同上 | — | `on_device_alarm` callback → log + degraded flag；不停機 |

---

## 驗證計畫（`verify.sh`）

前置：`docker compose up -d`

1. **Device failover**：`docker kill GC_PB_test-pcs_a-1`；12s 內 `redis-cli get lease:device:pcs_01` 值應含 `hostB`
2. **Redis blip**：`docker pause redis && sleep 5 && docker unpause redis`；往後 30s 每秒 scan `lease:*` 驗證每把鎖只有一位持有者（無 split-brain）
3. **Both NON-CRITICAL down**：`docker kill solar_a solar_b`；驗證無 stop override 推入、log 出現 degraded warning
4. **Both CRITICAL down**：`docker kill pcs_a pcs_b`；驗證 stop override 已推入
5. **Strategy failover**：`docker kill strategy_a`（若為 leader）；12s 內 `ems_cli set_mode pq_mode` 應成功，由 strategy_b 處理

---

## Open Risks

- **Redis blip > TTL**：若 Redis 凍結 > 10s，兩 host 可能競速到不同 replica（未來 Sentinel 情境）。緩解：fencing token — 在每次 `device:{id}:state` 寫入帶 `acquired_at`；陳舊寫入被下游拒絕。**本期延後至 Sentinel 階段**
- **Clock skew**：renew 3s / TTL 10s 容忍 ~7s；所有過期以 Redis server 時鐘為準（`SET EX` 是 server-side），本機時鐘偏移不影響
- **Modbus 單一連線重疊**：losing leader 須在 6s 內釋放。超時後 `os._exit(1)` 讓容器重啟，避免卡住導致 peer 無法連
- **EMS pub/sub 在 strategy failover 期間丟訊息**：後續可改 Redis Streams + consumer group，**本期接受**
- **跨主機 priority sleep 的退化情況**：只剩一台主機運作時，同主機 standby 要多等 `cross_host_bonus` 才能接手。`cross_host_bonus` 要可調

---

## 關鍵檔案清單（實作時將接觸）

**新增**
- [examples/GC_PB_test/instance_identity.py](instance_identity.py)
- [examples/GC_PB_test/redis_lease.py](redis_lease.py)
- [examples/GC_PB_test/leader_election.py](leader_election.py)
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

**重用（不修改）**
- [csp_lib/core/resilience.py:26-126](../../csp_lib/core/resilience.py#L26-L126) — `CircuitBreaker`, `RetryPolicy`
- [csp_lib/core/lifecycle.py](../../csp_lib/core/lifecycle.py) — `AsyncLifecycleMixin`
- [csp_lib/integration/system_controller.py:327](../../csp_lib/integration/system_controller.py#L327) — `builder.alarm_mode_per_device()`
- [csp_lib/integration/distributed/subscriber.py](../../csp_lib/integration/distributed/subscriber.py) — 既有 offline detection
- [csp_lib/redis/config.py:65-86](../../csp_lib/redis/config.py#L65-L86) — Sentinel config（未來切換用）

---

## 實施順序（每階段都可停）

1. **Phase 1 — Identity + Redis 韌性**（~0.5 天）
   新增 `instance_identity.py`、`redis_resilient.py`、`pubsub_resilient.py`。在既有 `strategy_main.py` 套 `ResilientSubscriber`（還沒 election）。成果：pub/sub 扛得住 Redis blip
2. **Phase 2 — Lease + Election**（~1 天）
   新增 `redis_lease.py`、`leader_election.py`。本機起兩個 process 競爭 `lease:test` 做單元驗證
3. **Phase 3 — HA device runner**（~1 天）
   新增 `device_runner_ha.py`；先改 `device_pcs_main.py` 一個；兩 process + 單 sim 驗證 failover；成功後複製到其他四個
4. **Phase 4 — HA strategy runner**（~0.5 天）
5. **Phase 5 — Criticality + 分級降級**（~0.5 天）
   新增 `criticality.py`、`strategy_main.py` 改用 `alarm_mode_per_device` + callback
6. **Phase 6 — docker-compose + verify.sh + README**（~0.5 天）
7. **Phase 7（延後）** — 把 `redis_lease` / `leader_election` 抽升到 `csp_lib/integration/leader/`；啟用 Sentinel；EMS 改 Redis Streams
