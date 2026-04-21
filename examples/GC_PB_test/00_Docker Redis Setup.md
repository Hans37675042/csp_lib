# Docker Redis Setup

## Context

執行 [device_acm_main.py](device_acm_main.py) 時出現：

```
redis.exceptions.ResponseError: wrong number of arguments for 'hset' command
```

**根因**：本機 Redis server 為 **3.0.504**（Windows MSOpenTech 舊移植），不支援多欄位 HSET（4.0 才加入）。而 [../../csp_lib/redis/client.py:363-379](../../csp_lib/redis/client.py#L363-L379) 的 `hset(name, mapping=...)` 會送出多欄位 HSET，被 3.0 server 拒絕。

**解法**：以 Docker 部署 Redis 7，不需修改任何 csp_lib 程式碼（連線仍為 `localhost:6379`）。

**預期結果**：`device_acm_main.py` 執行時 `StateSyncManager._on_read_complete` 能成功將 13 個量測欄位寫入 Redis Hash，`事件處理失敗: event=read_complete` warning 消失。

---

## 步驟 1：安裝 Docker Desktop（Windows）

1. 下載：`https://www.docker.com/products/docker-desktop/`
2. 執行安裝程式，勾選 **Use WSL 2 instead of Hyper-V**（預設）。
3. 安裝完成後**重新登入 Windows**（或重開機）讓 Docker 群組生效。
4. 啟動 Docker Desktop，等右下角鯨魚圖示穩定（不再動畫）。
5. 於 bash 驗證：
   ```bash
   docker --version
   docker info
   ```
   `docker info` 應顯示 `Server: Docker Engine` 段落。

若 WSL2 尚未啟用，安裝程式會引導；手動方式：系統管理員 PowerShell 執行 `wsl --install` 後重開機。

---

## 步驟 2：停止舊的原生 Redis（避免 port 衝突）

```bash
net stop Redis                # 若以 Windows service 安裝
# 或到「工作管理員」結束 redis-server.exe
```

驗證 port 6379 已釋出：
```bash
netstat -ano | grep 6379      # 應無輸出
```

---

## 步驟 3：以單一 `docker run` 指令啟動 Redis 7

```bash
docker run -d \
  --name csp-redis \
  --restart=always \
  -p 6379:6379 \
  -v csp-redis-data:/data \
  redis:7-alpine \
  redis-server --appendonly yes
```

| 參數 | 用途 |
|------|------|
| `--name csp-redis` | 之後 `docker stop/start csp-redis` 用此名稱 |
| `--restart=always` | Docker Desktop 開啟時自動復活 |
| `-p 6379:6379` | 對應 csp_lib 預設 port，範例不用改 |
| `-v csp-redis-data:/data` | named volume，`docker volume ls` 可查；容器刪掉資料仍在 |
| `--appendonly yes` | 啟用 AOF 持久化，避免 crash 丟資料 |
| `redis:7-alpine` | image 約 40MB，比官方完整版小 |

---

## 步驟 4：驗證 Redis 7 正常

```bash
docker exec -it csp-redis redis-cli INFO server | head -20
# redis_version:7.x.x 應出現

docker exec -it csp-redis redis-cli HSET test:hset a 1 b 2 c 3
# (integer) 3        ← 多欄位 HSET 成功
docker exec -it csp-redis redis-cli DEL test:hset
```

---

## 步驟 5：建立 `docker-compose.yml`（團隊／版控用）

於專案根目錄建立 [../../docker-compose.yml](../../docker-compose.yml)：

```yaml
services:
  redis:
    image: redis:7-alpine
    container_name: csp-redis
    restart: always
    ports:
      - "6379:6379"
    volumes:
      - csp-redis-data:/data
    command: redis-server --appendonly yes

  # 預留：未來將本機 MongoDB 一併容器化時解除註解
  # mongo:
  #   image: mongo:7
  #   container_name: csp-mongo
  #   restart: always
  #   ports:
  #     - "27017:27017"
  #   volumes:
  #     - csp-mongo-data:/data/db

volumes:
  csp-redis-data:
  # csp-mongo-data:
```

**從單一容器切換到 compose**：
```bash
docker stop csp-redis && docker rm csp-redis
docker compose up -d
```
因為 volume 名稱相同（`csp-redis-data`），資料會延續。

---

## 步驟 6：端對端驗證 csp_lib

1. 確認 Redis 已跑：`docker ps` 可見 `csp-redis` Up。
2. 於專案根目錄執行：
   ```bash
   .venv/Scripts/python examples/GC_PB_test/device_acm_main.py
   ```
3. 預期：
   - 不再出現 `wrong number of arguments for 'hset' command`。
   - log 持續顯示 Modbus 讀取，但無 `事件處理失敗: event=read_complete` warning。
4. 另開 bash 檢查 Redis 實際寫入：
   ```bash
   docker exec -it csp-redis redis-cli HGETALL device:acm_01:state
   # 列出 voltage_a/b/c、current_a/b/c、active_power ... 共 13 欄

   docker exec -it csp-redis redis-cli TTL device:acm_01:state
   # 回傳正數秒數（state_ttl）
   ```

---

## 關鍵檔案

| 檔案 | 角色 | 需修改？ |
|------|------|---------|
| [../../csp_lib/redis/client.py:363](../../csp_lib/redis/client.py#L363) | `hset()` wrapper，問題症狀點 | 否（server 升級後即修復） |
| [../../csp_lib/redis/config.py](../../csp_lib/redis/config.py) | Redis host/port 預設值 | 否（維持 localhost:6379） |
| [device_acm_main.py:40](device_acm_main.py#L40) | 觸發錯誤的範例 | 否 |
| [../../docker-compose.yml](../../docker-compose.yml) | 團隊使用 | **新建** |

---

## 風險與注意事項

- **port 6379 衝突**：若原生 Redis service 未停止，`docker run` 會失敗並報 `bind: address already in use`。先做步驟 2。
- **Docker Desktop 未啟動**：容器即使設 `restart=always`，Docker Desktop 沒開就不會跑。建議於 Settings → General 勾選 **Start Docker Desktop when you sign in**。
- **firewall / TLS**：本機開發無 auth、無 TLS，與 [../../csp_lib/redis/config.py](../../csp_lib/redis/config.py) 預設一致。若未來開 auth，需同步更新容器 `command` 與 csp_lib config。
- **資料遺失**：`docker volume rm csp-redis-data` 會永久刪除資料，執行前務必確認。

---

## 後續可考慮（本次不做）

1. 將 MongoDB 一併納入 compose（步驟 5 已留註解位）。
2. 讓 csp_lib 從環境變數讀取 Redis host/port，方便容器化 csp_lib 本身時切換。
3. 為 csp_lib 加舊版 Redis fallback（偵測到 < 4.0 自動改用 HMSET 或逐欄位 HSET）—— 只有真的需要支援 Redis 3 時才值得做。
