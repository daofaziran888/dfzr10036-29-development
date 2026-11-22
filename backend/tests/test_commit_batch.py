"""分流进度批次整组接纳场景演练。

运行方式（在 backend 目录下）：
    .venv/bin/python -I tests/test_commit_batch.py

覆盖题面场景：
1. 批次含订单流 110、支付流 84、退款流缺位坐标，受理时支付流写入中断；
2. 延迟监控能否读到订单流，退款/支付各为何种状态；
3. 断点恢复能否先单独使用订单流 110；
4. 同批次重复提交不产生重复检查点；
5. 补齐退款坐标重送后，三路共同恢复各自使用的实际位置。
"""
from __future__ import annotations

import sys
from pathlib import Path

# 允许用 `python -I tests/test_commit_batch.py` 直接运行
BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.checkpoint import CheckpointService  # noqa: E402
from app.services.commit_batch import (  # noqa: E402
    BATCH_PARTIAL,
    COMPARABLE,
    INCOMPARABLE,
    WRITE_FAILED,
    WRITE_OK,
    WRITE_PENDING,
    CommitBatchService,
)
from app.services.lag_monitor import LagMonitorService  # noqa: E402
from app.store import store  # noqa: E402

BATCH_NO = "BATCH-MULTI-01"
ORDER = "order_flow"
PAYMENT = "payment_flow"
REFUND = "refund_flow"

svc = CommitBatchService()
lag_svc = LagMonitorService()
cp_svc = CheckpointService()


def reset_store() -> None:
    store.rows("commit_batch").clear()
    store.rows("commit_batch_detail").clear()
    # 清掉演练可能写入 checkpoint 表的批次检查点，保留种子数据
    store._tables["checkpoint"] = [
        row for row in store.rows("checkpoint") if "来源批次" not in row
    ]


def detail_of(stream: str) -> dict:
    rows = [d for d in svc._details_of(BATCH_NO) if d["数据流"] == stream]
    assert rows, f"{stream} 明细缺失"
    return rows[0]


def batch_checkpoints() -> list[dict]:
    return [cp for cp in store.rows("checkpoint") if cp.get("来源批次") == BATCH_NO]


def checkpoint_streams() -> set[str]:
    return {cp["数据流"] for cp in batch_checkpoints()}


def section(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def test_scenario() -> None:
    reset_store()

    # ---------------------------------------------------------------
    section("步骤1 整组接纳：订单110 / 支付84 / 退款缺位，支付写入中断")
    # ---------------------------------------------------------------
    first_payload = [
        {"数据流": ORDER, "日志坐标": 110},
        {"数据流": PAYMENT, "日志坐标": 84},
        {"数据流": REFUND, "日志坐标": None},
    ]
    result = svc.accept_batch(
        BATCH_NO, first_payload, fail_streams=[PAYMENT]
    )
    assert result["ok"] is True
    assert result["idempotent"] is False
    assert result["批次状态"] == BATCH_PARTIAL

    order_d = detail_of(ORDER)
    payment_d = detail_of(PAYMENT)
    refund_d = detail_of(REFUND)

    # 订单流：写入成功、坐标可比较、生成检查点 110
    assert order_d["写入状态"] == WRITE_OK
    assert order_d["可比较性"] == COMPARABLE
    assert str(order_d["日志坐标"]) == "110"
    assert order_d["检查点编号"] == f"CK-B{BATCH_NO}-{ORDER}"

    # 支付流：坐标本身可比较，但写入中断，不生成检查点
    assert payment_d["写入状态"] == WRITE_FAILED
    assert payment_d["检查点编号"] is None
    assert "中断" in payment_d["写入说明"]

    # 退款流：坐标缺位 → 暂不可比、待补写，绝不用其他来源补位点，不进检查点
    assert refund_d["写入状态"] == WRITE_PENDING
    assert refund_d["可比较性"] == INCOMPARABLE
    assert refund_d["日志坐标"] is None
    assert refund_d["检查点编号"] is None

    # 整批只有订单流一路检查点
    assert len(batch_checkpoints()) == 1
    assert checkpoint_streams() == {ORDER}
    print(f"批次状态：{result['批次状态']}")
    print(f"整批核验依据：{result['整批核验依据']}")
    print(f"批次检查点：{[cp['检查点编号'] + '@' + cp['保存位点'] for cp in batch_checkpoints()]}")

    # ---------------------------------------------------------------
    section("步骤2 延迟监控：逐条明细生成可比较/暂不可比指标")
    # ---------------------------------------------------------------
    metrics_pack = lag_svc.read_batch_lag_metrics(BATCH_NO)
    assert metrics_pack["ok"] is True
    metrics = {m["数据流"]: m for m in metrics_pack["指标"]}
    assert metrics_pack["可比较流数"] == 1
    assert metrics_pack["暂不可比流数"] == 2

    # 订单流可读，且处理位点只来自本流明细坐标 110
    assert metrics[ORDER]["指标状态"] == "可比较"
    assert metrics[ORDER]["处理位点"] == "110"
    assert metrics[ORDER]["源端位点"] == "110"

    # 支付流写入中断 → 暂不可用，位点为 None，不允许拿别的来源顶替
    assert metrics[PAYMENT]["指标状态"] == "暂不可用"
    assert metrics[PAYMENT]["处理位点"] is None
    assert metrics[PAYMENT]["源端位点"] is None

    # 退款流缺位 → 暂不可比，监控不得把它当成已进入本轮检查点
    assert metrics[REFUND]["指标状态"] == "暂不可比"
    assert metrics[REFUND]["处理位点"] is None
    for m in metrics_pack["指标"]:
        print(f"  {m['数据流']}: {m['指标状态']} | 处理位点={m['处理位点']} | {m['说明']}")

    # ---------------------------------------------------------------
    section("步骤3 断点恢复：先单独使用订单流 110，整组恢复尚不可用")
    # ---------------------------------------------------------------
    scope = cp_svc.resolve_batch_recovery_scope(BATCH_NO)
    assert scope["ok"] is True
    assert scope["整组可恢复"] is False
    assert scope["整组恢复点"] is None
    single = {item["数据流"]: item for item in scope["单流可恢复"]}
    assert set(single) == {ORDER}
    assert single[ORDER]["日志坐标"] == "110"
    excluded = {item["数据流"]: item for item in scope["排除流"]}
    assert excluded[PAYMENT]["排除原因"] == "写入失败"
    assert excluded[REFUND]["排除原因"] == "日志坐标缺位"
    print(f"单流可恢复：{[(k, v['日志坐标']) for k, v in single.items()]}")
    print(f"排除流：{[(k, v['排除原因']) for k, v in excluded.items()]}")

    # ---------------------------------------------------------------
    section("步骤4 同批次原样重送：幂等，不产生重复检查点")
    # ---------------------------------------------------------------
    before_ids = {cp["id"] for cp in batch_checkpoints()}
    again = svc.accept_batch(
        BATCH_NO, first_payload, fail_streams=[PAYMENT]
    )
    assert again["idempotent"] is True
    assert again["提交轮次"] == 2
    assert again["幂等跳过流"] == [ORDER]
    after_ids = {cp["id"] for cp in batch_checkpoints()}
    assert before_ids == after_ids, "重复提交产生了重复检查点"
    assert len(batch_checkpoints()) == 1
    # 退款仍缺位、支付仍中断，批次维持部分写入
    assert svc._find_batch(BATCH_NO)["status"] == BATCH_PARTIAL
    print(f"幂等跳过：{again['幂等跳过流']}；检查点数量仍为 {len(batch_checkpoints())}")

    # ---------------------------------------------------------------
    section("步骤5 补齐退款坐标并重送（支付恢复可写）：三路共同恢复")
    # ---------------------------------------------------------------
    final_payload = [
        {"数据流": ORDER, "日志坐标": 110},
        {"数据流": PAYMENT, "日志坐标": 84},
        {"数据流": REFUND, "日志坐标": 57},
    ]
    final = svc.accept_batch(BATCH_NO, final_payload)  # 不再注入失败
    assert final["idempotent"] is True
    assert final["提交轮次"] == 3
    # 订单流依旧幂等跳过；支付补写、退款补写各自补建检查点
    assert final["幂等跳过流"] == [ORDER]
    assert len(batch_checkpoints()) == 3, "整组应恰好三路各一个检查点"
    assert checkpoint_streams() == {ORDER, PAYMENT, REFUND}

    final_scope = cp_svc.resolve_batch_recovery_scope(BATCH_NO)
    assert final_scope["整组可恢复"] is True
    assert final_scope["排除流"] == []
    assert final_scope["整组恢复点"] == {
        ORDER: "110",
        PAYMENT: "84",
        REFUND: "57",
    }
    print("三路共同恢复实际使用位置：")
    for stream, pos in final_scope["整组恢复点"].items():
        print(f"  {stream} -> {pos}")

    # 最终延迟监控三条全部可比较，且各用各流明细坐标
    final_metrics = lag_svc.read_batch_lag_metrics(BATCH_NO)
    assert final_metrics["可比较流数"] == 3
    assert final_metrics["暂不可比流数"] == 0
    by_stream = {m["数据流"]: m for m in final_metrics["指标"]}
    assert by_stream[ORDER]["处理位点"] == "110"
    assert by_stream[PAYMENT]["处理位点"] == "84"
    assert by_stream[REFUND]["处理位点"] == "57"

    # 明细里保留了每一路的实际写入轨迹（部分失败历史可追溯）
    assert detail_of(PAYMENT)["写入说明"].startswith("第3轮补写成功")
    assert detail_of(REFUND)["检查点编号"] == f"CK-B{BATCH_NO}-{REFUND}"


def test_all_streams_fail_is_rejected() -> None:
    section("附加用例 全部流写入失败 → 接纳失败，无任何检查点")
    reset_store()
    result = svc.accept_batch(
        "BATCH-REJECT-01",
        [{"数据流": ORDER, "日志坐标": 1}, {"数据流": PAYMENT, "日志坐标": 2}],
        fail_streams=[ORDER, PAYMENT],
    )
    from app.services.commit_batch import BATCH_REJECTED

    assert result["批次状态"] == BATCH_REJECTED
    assert result["新建检查点"] == []
    assert len(svc._details_of("BATCH-REJECT-01")) == 2  # 失败结果仍如实保留
    print(f"批次状态：{result['批次状态']}；明细保留 {len(result['明细'])} 条失败记录")


def test_non_numeric_coordinate_is_incomparable() -> None:
    section("附加用例 非法坐标文本 → 暂不可比，不进检查点")
    reset_store()
    svc.accept_batch(
        "BATCH-BAD-COORD-01",
        [{"数据流": ORDER, "日志坐标": "unknown_position"}],
    )
    d = svc._details_of("BATCH-BAD-COORD-01")[0]
    assert d["可比较性"] == INCOMPARABLE
    assert d["检查点编号"] is None
    print(f"坐标校验说明：{d['坐标校验说明']}")


def test_generic_resource_endpoint_still_works() -> None:
    section("附加用例 通用资源路由的列表/登记骨架对新模块可用")
    items, total = svc.list_entries(page=1, size=10)
    assert total == len(store.rows("commit_batch"))
    entry, missing = svc.create_entry(
        {"批次编号": "BATCH-GENERIC-01", "数据流数量": 2}
    )
    assert not missing and entry is not None
    print(f"通用登记返回：{entry['批次编号']} / status={entry['status']}")


def main() -> None:
    test_scenario()
    test_all_streams_fail_is_rejected()
    test_non_numeric_coordinate_is_incomparable()
    test_generic_resource_endpoint_still_works()
    print("\n全部断言通过 ✔")


if __name__ == "__main__":
    main()
