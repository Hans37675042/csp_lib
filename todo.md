[STUDY] 研究以下功能:
- core
- equipment
- controller
- integration
- manager

[TODO] 效能監控、可觀測性設計、錯誤追蹤整合等企業級需求

[BUG] MongoBatchUploader.batch_size_threshold 無法即時觸發
- 問題：enqueue() 只將資料入隊，不做任何通知；_flush_loop 靠輪詢，
  每隔 flush_interval 才檢查一次閾值，threshold 的「立即觸發」語意失效。
- 根因：缺少事件通知機制，閾值觸發退化為週期性輪詢。
- 修正：新增 _flush_event，enqueue 達閾值後立即 set()，
  _flush_loop 改用 asyncio.wait() 同時監聽 stop/flush_event/timeout。
- 相關檔案：csp_lib/mongo/uploader.py（enqueue、_flush_loop）
  已修正於：examples/GC_mini_test/influx_lib.py（InfluxBatchUploader）

[IMPROVE] MongoBatchUploader._flush_loop 週期漂移（execute time shift）
- 問題：原用 asyncio.wait_for(..., timeout=flush_interval)，每次 flush 完成後
  才重新計時，實際週期 = flush_interval + flush 執行時間，長期累積漂移。
- 根因：使用相對延遲而非絕對時間追蹤。
- 修正：迴圈外記錄 next_time（絕對時間點），每輪動態計算剩餘 timeout；
  若已超過 next_time 則追趕至下一對齊點（timeout=0）立即補跑。
- 相關檔案：csp_lib/mongo/uploader.py（_flush_loop）
  已修正於：examples/GC_mini_test/influx_lib.py（InfluxBatchUploader._flush_loop）

[IMPROVE] 時間錨定方式需要加入超時後立即補跑機制，避免因 flush 執行時間過長導致後續 flush 繼續延遲。

[] 多機分配功率時，若個台設備斷線、告警，會排除該設備?
