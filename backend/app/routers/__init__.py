"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import commit_batch, resource

# 固定前缀的批次路由需要排在通用 /api/{module} 资源路由之前，
# 避免被动态路径抢先匹配。
ROUTERS = [commit_batch, resource]
