"""分流进度批次路由：整组接纳、幂等重送、延迟指标与恢复范围。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.commit_batch import CommitBatchService

router = APIRouter(prefix="/api/commit-batches", tags=["分流进度批次"])

service = CommitBatchService()


class StreamPosition(BaseModel):
    数据流: str
    日志坐标: int | str | None = None


class BatchPayload(BaseModel):
    批次编号: str
    流位点: list[StreamPosition] = Field(default_factory=list)
    # 模拟写入中断的数据流（如支付流），用于部分失败演练
    失败注入: list[str] = Field(default_factory=list)


@router.post("/accept")
def accept_batch(payload: BatchPayload) -> dict[str, Any]:
    """整组接纳一个多流批次；同批次编号重送自动走幂等/补齐路径。"""
    return service.accept_batch(
        payload.批次编号,
        [item.model_dump() for item in payload.流位点],
        fail_streams=payload.失败注入,
    )


@router.get("/{batch_no}/lag-metrics")
def batch_lag_metrics(batch_no: str) -> dict[str, Any]:
    """延迟监控：按批次明细逐条生成可比较/暂不可比指标。"""
    return service.build_lag_metrics(batch_no)


@router.get("/{batch_no}/recovery-scope")
def batch_recovery_scope(batch_no: str) -> dict[str, Any]:
    """断点恢复：确认同批明细可用于单流或整组恢复的范围。"""
    return service.resolve_recovery_scope(batch_no)


@router.get("/{batch_no}/details")
def batch_details(batch_no: str) -> dict[str, Any]:
    """查看一个批次的全部分流明细与整批核验依据。"""
    batch = service._find_batch(batch_no)
    if batch is None:
        return {"ok": False, "message": f"批次 {batch_no} 不存在"}
    return {
        "ok": True,
        "批次": batch,
        "明细": service._details_of(batch_no),
    }
