"""延迟监控服务：采集延迟数据后级联告警和健康评估。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LagMonitorService(BaseModuleService):
    MODULE = "lag_monitor"
    ENTITY = "延迟指标"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["指标编号", "数据流", "源端位点", "处理位点", "延迟秒数", "告警阈值", "最近采样", "采样间隔"]
    REQUIRED_FIELDS = ["指标编号", "数据流", "源端位点"]
    STATUS_ORDER = ["采集中", "正常", "延迟偏高", "严重滞后"]
    ACTION_RULES = {"启动采集": "正常", "标记偏高": "延迟偏高", "标记滞后": "严重滞后"}
    NEGATIVE_ACTIONS = []

    def compute_lag_from_watermark_and_checkpoint(self, data_stream: str) -> dict[str, Any]:
        """从watermark_mgr和checkpoint中读取位点数据计算延迟。

        数据流链路: watermark_mgr + checkpoint -> lag_monitor
        """
        result: dict[str, Any] = {
            "data_stream": data_stream,
            "watermark": None,
            "checkpoint": None,
            "lag_seconds": 0,
            "status_override": None,
        }

        for wm in store.rows("watermark_mgr"):
            if str(wm.get("数据流", "")) == data_stream:
                result["watermark"] = wm.get("当前位点")
                if wm.get("status") == "水位延迟":
                    result["lag_seconds"] = int(wm.get("延迟行数", 0))
                    result["status_override"] = "严重滞后"
                break

        for cp in store.rows("checkpoint"):
            if str(cp.get("数据流", "")) == data_stream:
                result["checkpoint"] = cp.get("保存位点")
                result["save_time"] = cp.get("保存时间")
                break

        return result

    def read_batch_lag_metrics(self, batch_no: str) -> dict[str, Any]:
        """延迟监控读取分流进度批次明细，逐条生成可比较/暂不可比指标。

        数据流链路: commit_batch_detail -> lag_monitor
        只采用批次明细里各流自己的日志坐标，缺位/失败流不以其他来源弥补。
        """
        from app.services.commit_batch import CommitBatchService

        return CommitBatchService().build_lag_metrics(batch_no)

    def escalate_to_alert_and_health(self, entry_id: int) -> dict[str, Any] | None:
        """延迟达到告警阈值后，触发告警规则和健康评估。

        数据流链路: lag_monitor -> alert_rule + stream_health
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        tag = str(entry.get("数据流", ""))
        results: dict[str, Any] = {"alert": None, "health": None}

        if entry.get("status") in ("严重滞后", "延迟偏高"):
            # 创建告警
            alert_rows = store.rows("alert_rule")
            new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
            severity = "P1-紧急" if entry.get("status") == "严重滞后" else "P2-警告"
            alert = {
                "id": new_alert_id,
                "策略编号": f"ALER-{new_alert_id:04d}",
                "监控指标": "实时延迟",
                "告警条件": f"延迟 > {entry.get('告警阈值', '30s')}",
                "告警级别": severity,
                "通知渠道": "飞书群+电话",
                "聚合窗口": "1min",
                "静默时段": "无",
                "触发次数": 5,
                "status": "已触发",
                "pending": True,
                "abnormal": True,
            }
            alert_rows.append(alert)
            results["alert"] = alert

        # 触发健康评估
        health_rows = store.rows("stream_health")
        new_health_id = max((int(r.get("id", 0)) for r in health_rows), default=0) + 1
        score = 45 if entry.get("status") == "严重滞后" else 72
        health = {
            "id": new_health_id,
            "评估编号": f"STRE-{new_health_id:04d}",
            "数据流": tag,
            "健康分数": score,
            "风险项": "高延迟+水位堆积",
            "修复建议": "扩容处理节点+扩大乱序窗口",
            "评估时间": "2026-11-18 10:40:00",
            "下次评估": "2026-11-18 10:45:00",
            "评估状态": "待修复",
            "status": "需修复",
            "pending": True,
            "abnormal": True,
        }
        health_rows.append(health)
        results["health"] = health

        return results