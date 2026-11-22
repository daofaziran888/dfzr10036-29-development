"""分流进度批次 HTTP 端到端演练：只走真实 FastAPI 接口，不直接碰服务层。

运行（backend 目录下，服务已启动）：
    .venv/bin/python -I tests/http_commit_batch_demo.py
"""
from __future__ import annotations

import json
import time
import urllib.request

BASE = "http://127.0.0.1:8000/api"
# 每次运行使用独立批次号，避免内存仓库中历史批次触发幂等路径
BATCH_NO = f"BATCH-HTTP-{int(time.time()) % 1000000:06d}"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def show(title: str, obj: dict) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)
    print(json.dumps(obj, ensure_ascii=False, indent=2))


# 1. 首轮整组接纳：订单110 / 支付84 / 退款缺位，支付写入中断
r1 = call("POST", "/commit-batches/accept", {
    "批次编号": BATCH_NO,
    "流位点": [
        {"数据流": "order_flow", "日志坐标": 110},
        {"数据流": "payment_flow", "日志坐标": 84},
        {"数据流": "refund_flow", "日志坐标": None},
    ],
    "失败注入": ["payment_flow"],
})
show("① 整组接纳（支付中断、退款缺位）", {
    "批次状态": r1["批次状态"],
    "提交轮次": r1["提交轮次"],
    "新建检查点": [c["检查点编号"] for c in r1["新建检查点"]],
    "整批核验依据": r1["整批核验依据"],
})

# 2. 延迟监控读批次明细
r2 = call("GET", f"/commit-batches/{BATCH_NO}/lag-metrics")
show("② 延迟监控指标（应只有订单流可比较）", r2)

# 3. 断点恢复范围
r3 = call("GET", f"/commit-batches/{BATCH_NO}/recovery-scope")
show("③ 断点恢复范围（单流订单110可用，整组不可用）", {
    "整组可恢复": r3["整组可恢复"],
    "单流可恢复": r3["单流可恢复"],
    "排除流": r3["排除流"],
})

# 4. 原样重送：幂等
r4 = call("POST", "/commit-batches/accept", {
    "批次编号": BATCH_NO,
    "流位点": [
        {"数据流": "order_flow", "日志坐标": 110},
        {"数据流": "payment_flow", "日志坐标": 84},
        {"数据流": "refund_flow", "日志坐标": None},
    ],
    "失败注入": ["payment_flow"],
})
show("④ 原样重送（幂等，检查点不重复）", {
    "提交轮次": r4["提交轮次"],
    "幂等跳过流": r4["幂等跳过流"],
    "新建检查点": r4["新建检查点"],
    "批次状态": r4["批次状态"],
})

# 5. 补齐退款坐标、支付恢复，重送 → 三路共同恢复
r5 = call("POST", "/commit-batches/accept", {
    "批次编号": BATCH_NO,
    "流位点": [
        {"数据流": "order_flow", "日志坐标": 110},
        {"数据流": "payment_flow", "日志坐标": 84},
        {"数据流": "refund_flow", "日志坐标": 57},
    ],
})
show("⑤ 补齐重送（整组就绪）", {
    "提交轮次": r5["提交轮次"],
    "幂等跳过流": r5["幂等跳过流"],
    "新建检查点": [c["检查点编号"] for c in r5["新建检查点"]],
    "批次状态": r5["批次状态"],
})

r6 = call("GET", f"/commit-batches/{BATCH_NO}/recovery-scope")
show("⑥ 三路共同恢复实际位置", r6["整组恢复点"])

r7 = call("GET", f"/commit-batches/{BATCH_NO}/details")
show("⑦ 批次明细（保留各路实际写入轨迹）", [
    {
        "数据流": d["数据流"], "日志坐标": d["日志坐标"],
        "可比较性": d["可比较性"], "写入状态": d["写入状态"],
        "检查点编号": d["检查点编号"], "写入说明": d["写入说明"],
    }
    for d in r7["明细"]
])

# 8. 通用资源路由回归：新模块可分页查询
r8 = call("GET", "/commit_batch?page=1&size=10")
show("⑧ 通用资源路由回归 /api/commit_batch", {
    "total": r8["total"],
    "批次编号": [i["批次编号"] for i in r8["items"]],
})

# 9. 原有模块回归
r9 = call("GET", "/checkpoint?page=1&size=100")
batch_cps = [i for i in r9["items"] if i.get("来源批次") == BATCH_NO]
show("⑨ 检查点表回归（批次检查点恰好3条、无重复）", {
    "种子+批次检查点总数": r9["total"],
    "本批检查点": [(c["数据流"], c["保存位点"]) for c in batch_cps],
})

# 10. 不存在批次的容错
r10 = call("GET", "/commit-batches/NO-SUCH-BATCH/recovery-scope")
show("⑩ 不存在批次容错", r10)

# 断言
assert r1["批次状态"] == "部分写入"
assert r2["可比较流数"] == 1 and r2["暂不可比流数"] == 2
assert r3["整组可恢复"] is False
assert r4["幂等跳过流"] == ["order_flow"] and r4["新建检查点"] == []
assert r6["整组恢复点"] == {
    "order_flow": "110", "payment_flow": "84", "refund_flow": "57"}
assert len(batch_cps) == 3
assert r10["ok"] is False
print("\nHTTP 端到端全部断言通过 ✔")
