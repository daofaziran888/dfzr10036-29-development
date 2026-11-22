"""检查点服务：断点恢复时触发水位重置和回填任务。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CheckpointService(BaseModuleService):
    MODULE = "checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["检查点编号", "数据流", "保存位点", "保存时间", "恢复状态", "丢失行数", "回放代价", "恢复耗时"]
    REQUIRED_FIELDS = ["检查点编号", "数据流", "保存位点"]
    STATUS_ORDER = ["已保存", "恢复中", "已恢复", "恢复失败"]
    ACTION_RULES = {"触发恢复": "恢复中", "确认恢复": "已恢复", "标记失败": "恢复失败"}
    NEGATIVE_ACTIONS = []

    def resolve_batch_recovery_scope(self, batch_no: str) -> dict[str, Any]:
        """断点恢复根据同批分流明细确认单流/整组恢复范围。

        数据流链路: commit_batch_detail -> checkpoint
        """
        from app.services.commit_batch import CommitBatchService

        return CommitBatchService().resolve_recovery_scope(batch_no)

    def trigger_watermark_reset_and_backfill(self, entry_id: int) -> dict[str, Any]:
        """断点恢复后触发水位重置和回填任务。

        数据流链路: checkpoint -> watermark_mgr + backfill_run
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        tag = str(entry.get("数据流", ""))
        results: dict[str, Any] = {"watermark": None, "backfill": None}

        if entry.get("status") == "已恢复":
            # 重置水位
            for wm in store.rows("watermark_mgr"):
                if str(wm.get("数据流", "")) == tag:
                    wm["当前位点"] = str(entry.get("保存位点", ""))
                    wm["status"] = "推进中"
                    wm["pending"] = True
                    wm["abnormal"] = False
                    results["watermark"] = wm
                    break

            # 如果有丢失行数，创建回填任务
            loss = int(entry.get("丢失行数", 0))
            if loss > 0:
                bf_rows = store.rows("backfill_run")
                new_bf_id = max((int(r.get("id", 0)) for r in bf_rows), default=0) + 1
                backfill = {
                    "id": new_bf_id,
                    "任务编号": f"BACK-{new_bf_id:04d}",
                    "目标表": tag,
                    "起始位点": str(entry.get("保存位点", "")),
                    "截止位点": "latest_binlog",
                    "回填模式": "批量并行回填",
                    "并行粒度": "按分区",
                    "当前进度": "0%",
                    "回填速率": "0 eps",
                    "status": "回填中",
                    "pending": True,
                    "abnormal": False,
                }
                bf_rows.append(backfill)
                results["backfill"] = backfill

        return results