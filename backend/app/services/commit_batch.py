"""分流进度批次服务：多流提交位点的整组接纳与批次明细管理。

设计约束（对应业务题面）：
1. 同一个批次一次性接纳多条数据流的提交位点，批量写入分流进度批次明细，
   不把共享恢复点拆成多次单流提交。
2. 每条流的日志坐标独立校验：坐标缺失只能记为"暂不可比"，不能用别的来源
   弥补缺失位点，也不能把缺位流当成已经进入本轮检查点。
3. 同一批次重复提交不产生重复检查点（幂等）。
4. 部分执行失败时保留各流实际写入结果与整批核验依据（不做伪回滚）。
5. 延迟监控基于批次明细逐条生成"可比较/暂不可比"指标。
6. 断点恢复依据同批明细确认可用于单流或整组恢复的范围。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 明细写入状态
WRITE_OK = "已写入"
WRITE_FAILED = "写入失败"
WRITE_PENDING = "待补写"

# 坐标可比较性
COMPARABLE = "可比较"
INCOMPARABLE = "暂不可比"

# 批次整体状态
BATCH_ACCEPTED = "已接纳"        # 全部流写入成功且坐标齐全
BATCH_PARTIAL = "部分写入"       # 至少一流写入成功，但存在失败或缺位
BATCH_REJECTED = "接纳失败"      # 没有任何一路流成功写入
BATCH_CLOSED = "整组就绪"        # 补齐重送后全部流写入成功且坐标齐全


class CommitBatchService(BaseModuleService):
    MODULE = "commit_batch"
    ENTITY = "分流进度批次"
    OPERATION_TERM = "接纳"
    LIST_FIELDS = [
        "批次编号", "提交轮次", "数据流数量", "已写入流数", "写入失败流数",
        "暂不可比流数", "整批核验依据", "接纳时间",
    ]
    REQUIRED_FIELDS = ["批次编号", "数据流数量"]
    STATUS_ORDER = [BATCH_ACCEPTED, BATCH_PARTIAL, BATCH_REJECTED, BATCH_CLOSED]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _next_id(self, module: str) -> int:
        return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1

    @staticmethod
    def _find_batch(batch_no: str) -> dict[str, Any] | None:
        for batch in store.rows("commit_batch"):
            if str(batch.get("批次编号", "")) == batch_no:
                return batch
        return None

    @staticmethod
    def _details_of(batch_no: str) -> list[dict[str, Any]]:
        return [
            row for row in store.rows("commit_batch_detail")
            if str(row.get("批次编号", "")) == batch_no
        ]

    @staticmethod
    def _validate_coordinate(stream: str, coordinate: Any) -> tuple[bool, str]:
        """逐流独立校验日志坐标。只认本流随批次提交的坐标，不查任何其他来源。

        返回 (是否可比较, 说明)。坐标缺失/空白/非数值一律判为暂不可比。
        """
        if coordinate is None or not str(coordinate).strip():
            return False, f"{stream} 未随批次提供日志坐标，位点暂不可比"
        try:
            int(str(coordinate).strip())
        except (TypeError, ValueError):
            return False, f"{stream} 日志坐标「{coordinate}」无法解析为可比较位点"
        return True, f"{stream} 日志坐标 {coordinate} 校验通过"

    def _refresh_batch_summary(self, batch: dict[str, Any]) -> None:
        """根据明细实际写入结果重算批次汇总与整批核验依据。"""
        details = self._details_of(str(batch["批次编号"]))
        ok = [d for d in details if d.get("写入状态") == WRITE_OK]
        failed = [d for d in details if d.get("写入状态") == WRITE_FAILED]
        pending = [d for d in details if d.get("写入状态") == WRITE_PENDING]
        incomparable = [d for d in details if d.get("可比较性") == INCOMPARABLE]

        batch["数据流数量"] = len(details)
        batch["已写入流数"] = len(ok)
        batch["写入失败流数"] = len(failed)
        batch["暂不可比流数"] = len(incomparable)

        basis = [
            f"整组接纳{len(details)}路流（明细逐流独立校验，未拆分共享恢复点）",
            f"已写入{len(ok)}路：{'、'.join(d['数据流'] for d in ok) or '无'}",
        ]
        if failed:
            basis.append(
                f"写入失败{len(failed)}路（保留实际结果，未回滚）："
                f"{'、'.join(d['数据流'] for d in failed)}"
            )
        if pending:
            basis.append(
                f"待补写{len(pending)}路：{'、'.join(d['数据流'] for d in pending)}"
            )
        if incomparable:
            names = "、".join(
                f"{d['数据流']}(缺位)" if d.get("写入状态") != WRITE_FAILED
                else f"{d['数据流']}(失败)"
                for d in incomparable
            )
            basis.append(f"暂不可比{len(incomparable)}路，不纳入共同进度核对：{names}")
        comparable_ok = [d for d in ok if d.get("可比较性") == COMPARABLE]
        basis.append(f"当前可参与共同恢复点核对 {len(comparable_ok)}/{len(details)} 路")
        batch["整批核验依据"] = "；".join(basis)

        if not ok:
            batch["status"] = BATCH_REJECTED
        elif failed or pending or incomparable:
            batch["status"] = BATCH_PARTIAL
        else:
            # 全部写入成功且坐标齐全：首轮接纳为「已接纳」，重送补齐后为「整组就绪」
            batch["status"] = BATCH_CLOSED if int(batch.get("提交轮次", 1)) > 1 else BATCH_ACCEPTED
        batch["pending"] = batch["status"] in (BATCH_PARTIAL, BATCH_REJECTED)
        batch["abnormal"] = bool(failed or pending)

    def _write_one_detail(
        self, batch_no: str, stream: str, coordinate: Any, *, force_fail: bool
    ) -> dict[str, Any]:
        """单流写入一条批次明细。坐标校验与写入结果相互独立、如实记录。"""
        comparable, note = self._validate_coordinate(stream, coordinate)
        detail = {
            "id": self._next_id("commit_batch_detail"),
            "批次编号": batch_no,
            "数据流": stream,
            "日志坐标": None if coordinate is None or not str(coordinate).strip()
            else str(coordinate).strip(),
            "可比较性": COMPARABLE if comparable else INCOMPARABLE,
            "坐标校验说明": note,
            "写入状态": WRITE_OK,
            "写入说明": "",
            "写入时间": self._now(),
            "检查点编号": None,
            "status": "生效中",
            "pending": False,
            "abnormal": False,
        }
        # 失败注入：用于模拟支付流写入中断等部分失败场景
        if force_fail:
            detail["写入状态"] = WRITE_FAILED
            detail["写入说明"] = "写入中断，未生成本流检查点"
            detail["status"] = "异常"
            detail["abnormal"] = True
            detail["pending"] = True
            detail["写入时间"] = self._now()
        elif not comparable:
            # 坐标缺位：明细已登记但位点待补，不进入本轮检查点
            detail["写入状态"] = WRITE_PENDING
            detail["写入说明"] = "明细已登记，坐标缺位故不纳入本轮检查点，等待补齐重送"
            detail["pending"] = True
        else:
            detail["写入说明"] = "写入成功并纳入本轮检查点"
        store.rows("commit_batch_detail").append(detail)
        return detail

    def _create_checkpoint_for(self, batch_no: str, detail: dict[str, Any]) -> dict[str, Any]:
        """为一条可比较且写入成功的明细创建检查点；缺位/失败流不创建。"""
        cp_rows = store.rows("checkpoint")
        cp_id = self._next_id("checkpoint")
        checkpoint = {
            "id": cp_id,
            "status": "已保存",
            "pending": True,
            "abnormal": False,
            "检查点编号": f"CK-B{batch_no}-{detail['数据流']}",
            "数据流": detail["数据流"],
            "保存位点": str(detail["日志坐标"]),
            "保存时间": self._now(),
            "恢复状态": "就绪-批次明细可溯源",
            "丢失行数": 0,
            "回放代价": "预估1.5s",
            "恢复耗时": "未触发",
            "来源批次": batch_no,
            "来源明细": detail["id"],
        }
        cp_rows.append(checkpoint)
        detail["检查点编号"] = checkpoint["检查点编号"]
        return checkpoint

    # ------------------------------------------------------------------
    # 核心入口：整组接纳（含幂等重送/补齐）
    # ------------------------------------------------------------------
    def accept_batch(
        self,
        batch_no: str,
        streams: list[dict[str, Any]],
        *,
        fail_streams: list[str] | None = None,
    ) -> dict[str, Any]:
        """整组接纳一个多流批次。

        参数:
            batch_no: 批次编号，相同编号重送走幂等/补齐路径。
            streams: [{"数据流": ..., "日志坐标": ...}, ...]，坐标可缺省。
            fail_streams: 模拟写入中断的数据流名单。

        幂等规则:
            - 批次不存在：整组接纳，逐流写明细、逐流建检查点。
            - 批次已存在：不新增重复明细/检查点；仅对"写入失败/待补写"的流，
              在本次坐标可比较时补写并补建检查点；已成功的流保持原结果。
        """
        fail_streams = fail_streams or []
        existing = self._find_batch(batch_no)

        if existing is not None:
            return self._reconcile_batch(existing, streams, fail_streams)

        # ---- 首轮整组接纳 ----
        batch = {
            "id": self._next_id("commit_batch"),
            "批次编号": batch_no,
            "提交轮次": 1,
            "数据流数量": len(streams),
            "已写入流数": 0,
            "写入失败流数": 0,
            "暂不可比流数": 0,
            "整批核验依据": "",
            "接纳时间": self._now(),
            "status": BATCH_PARTIAL,
            "pending": True,
            "abnormal": False,
        }
        store.rows("commit_batch").append(batch)

        created_checkpoints: list[dict[str, Any]] = []
        detail_results: list[dict[str, Any]] = []
        for item in streams:
            stream = str(item.get("数据流", "")).strip()
            coordinate = item.get("日志坐标")
            detail = self._write_one_detail(
                batch_no, stream, coordinate, force_fail=stream in fail_streams
            )
            detail_results.append(detail)
            # 只有"写入成功 + 坐标可比较"的流才进入本轮检查点
            if detail["写入状态"] == WRITE_OK and detail["可比较性"] == COMPARABLE:
                created_checkpoints.append(self._create_checkpoint_for(batch_no, detail))

        self._refresh_batch_summary(batch)
        return {
            "ok": batch["status"] != BATCH_REJECTED,
            "idempotent": False,
            "批次编号": batch_no,
            "批次状态": batch["status"],
            "提交轮次": 1,
            "明细": detail_results,
            "新建检查点": created_checkpoints,
            "整批核验依据": batch["整批核验依据"],
        }

    def _reconcile_batch(
        self,
        batch: dict[str, Any],
        streams: list[dict[str, Any]],
        fail_streams: list[str],
    ) -> dict[str, Any]:
        """同一批次重送：幂等保留成功流，仅补写失败/待补写流。"""
        batch_no = str(batch["批次编号"])
        batch["提交轮次"] = int(batch.get("提交轮次", 1)) + 1
        details = self._details_of(batch_no)
        indexed = {str(d["数据流"]): d for d in details}

        new_checkpoints: list[dict[str, Any]] = []
        rewritten: list[dict[str, Any]] = []
        skipped: list[str] = []

        for item in streams:
            stream = str(item.get("数据流", "")).strip()
            coordinate = item.get("日志坐标")
            old = indexed.get(stream)

            if old is None:
                # 批次里新增的流：直接补一条明细
                detail = self._write_one_detail(
                    batch_no, stream, coordinate, force_fail=stream in fail_streams
                )
                rewritten.append(detail)
                if detail["写入状态"] == WRITE_OK and detail["可比较性"] == COMPARABLE:
                    new_checkpoints.append(self._create_checkpoint_for(batch_no, detail))
                continue

            if old.get("写入状态") == WRITE_OK and old.get("检查点编号"):
                # 已成功且已建检查点：幂等跳过，绝不产生重复检查点
                skipped.append(stream)
                continue

            # 失败/待补写流：用本次提交重新独立校验坐标
            comparable, note = self._validate_coordinate(stream, coordinate)
            old["坐标校验说明"] = note
            if not comparable:
                old["可比较性"] = INCOMPARABLE
                old["日志坐标"] = None
                old["写入状态"] = WRITE_PENDING
                old["写入说明"] = "重送坐标仍缺位，保持待补写，不生成检查点"
                old["pending"] = True
                rewritten.append(old)
                continue

            if stream in fail_streams:
                old["可比较性"] = COMPARABLE
                old["日志坐标"] = str(coordinate).strip()
                old["写入状态"] = WRITE_FAILED
                old["写入说明"] = "重送仍写入中断"
                old["abnormal"] = True
                old["pending"] = True
                rewritten.append(old)
                continue

            old["可比较性"] = COMPARABLE
            old["日志坐标"] = str(coordinate).strip()
            old["写入状态"] = WRITE_OK
            old["写入说明"] = f"第{batch['提交轮次']}轮补写成功并补建检查点"
            old["写入时间"] = self._now()
            old["status"] = "生效中"
            old["abnormal"] = False
            old["pending"] = False
            new_checkpoints.append(self._create_checkpoint_for(batch_no, old))
            rewritten.append(old)

        self._refresh_batch_summary(batch)
        return {
            "ok": True,
            "idempotent": True,
            "批次编号": batch_no,
            "批次状态": batch["status"],
            "提交轮次": batch["提交轮次"],
            "幂等跳过流": skipped,
            "补写明细": rewritten,
            "新建检查点": new_checkpoints,
            "整批核验依据": batch["整批核验依据"],
        }

    # ------------------------------------------------------------------
    # 延迟监控：逐条明细生成可比较/暂不可比指标
    # ------------------------------------------------------------------
    def build_lag_metrics(self, batch_no: str) -> dict[str, Any]:
        """基于分流进度批次明细逐条产出延迟指标。

        - 可比较且已写入：生成可比较指标，处理位点直接采用本流明细坐标，
          不从水位/其他流等任何来源补数。
        - 写入失败：指标暂不可用（写入中断，无处理位点可读）。
        - 坐标缺位：指标暂不可比（不伪造位点）。
        """
        batch = self._find_batch(batch_no)
        if batch is None:
            return {"ok": False, "message": f"批次 {batch_no} 不存在", "指标": []}

        metrics: list[dict[str, Any]] = []
        for detail in self._details_of(batch_no):
            stream = str(detail["数据流"])
            base = {
                "数据流": stream,
                "批次编号": batch_no,
                "来源明细": detail["id"],
            }
            if detail.get("写入状态") == WRITE_FAILED:
                metrics.append({
                    **base,
                    "指标状态": "暂不可用",
                    "可比较性": INCOMPARABLE,
                    "源端位点": None,
                    "处理位点": None,
                    "说明": "该流批次写入中断，延迟监控读不到本流处理位点，不以其他来源顶替",
                })
                continue
            if detail.get("可比较性") == INCOMPARABLE:
                metrics.append({
                    **base,
                    "指标状态": "暂不可比",
                    "可比较性": INCOMPARABLE,
                    "源端位点": None,
                    "处理位点": None,
                    "说明": "该流日志坐标缺位，暂不参与共同进度核对",
                })
                continue
            coordinate = str(detail["日志坐标"])
            metrics.append({
                **base,
                "指标状态": "可比较",
                "可比较性": COMPARABLE,
                "源端位点": coordinate,
                "处理位点": coordinate,
                "延迟秒数": 0,
                "说明": f"依据批次明细坐标 {coordinate} 生成，未引用其他来源",
            })

        comparable_count = sum(1 for m in metrics if m["可比较性"] == COMPARABLE)
        return {
            "ok": True,
            "批次编号": batch_no,
            "指标": metrics,
            "可比较流数": comparable_count,
            "暂不可比流数": len(metrics) - comparable_count,
        }

    # ------------------------------------------------------------------
    # 断点恢复：单流 / 整组恢复范围
    # ------------------------------------------------------------------
    def resolve_recovery_scope(self, batch_no: str) -> dict[str, Any]:
        """依据同批明细确认可用于单流或整组恢复的范围。"""
        batch = self._find_batch(batch_no)
        if batch is None:
            return {"ok": False, "message": f"批次 {batch_no} 不存在"}

        single: list[dict[str, Any]] = []
        excluded: list[dict[str, Any]] = []
        for detail in self._details_of(batch_no):
            item = {
                "数据流": detail["数据流"],
                "日志坐标": detail["日志坐标"],
                "检查点编号": detail.get("检查点编号"),
                "写入状态": detail["写入状态"],
                "可比较性": detail["可比较性"],
            }
            if (
                detail.get("写入状态") == WRITE_OK
                and detail.get("可比较性") == COMPARABLE
                and detail.get("检查点编号")
            ):
                single.append(item)
            else:
                reason = "写入失败" if detail.get("写入状态") == WRITE_FAILED else "日志坐标缺位"
                excluded.append({**item, "排除原因": reason})

        group_ready = len(excluded) == 0 and len(single) > 0
        return {
            "ok": True,
            "批次编号": batch_no,
            "批次状态": batch["status"],
            "单流可恢复": single,
            "整组可恢复": group_ready,
            "整组恢复点": (
                {item["数据流"]: item["日志坐标"] for item in single}
                if group_ready else None
            ),
            "排除流": excluded,
            "核验依据": batch.get("整批核验依据", ""),
        }


class CommitBatchDetailService(CommitBatchService):
    """明细行的通用查询/登记骨架；业务写入仍由 CommitBatchService 内部完成。"""

    MODULE = "commit_batch_detail"
    ENTITY = "分流进度批次明细"
    LIST_FIELDS = [
        "批次编号", "数据流", "日志坐标", "可比较性", "写入状态",
        "坐标校验说明", "写入说明", "检查点编号", "写入时间",
    ]
    REQUIRED_FIELDS = ["批次编号", "数据流"]
