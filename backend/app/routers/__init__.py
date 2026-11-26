"""路由汇总：专用批次恢复接口先注册，通用资源路由随后兜底。"""
from __future__ import annotations

from app.routers import batch_intake, resource

ROUTERS = [batch_intake, resource]
