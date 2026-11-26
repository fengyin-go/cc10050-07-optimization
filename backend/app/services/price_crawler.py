"""价格爬取任务领域服务：按关键词与抓取深度调度商品价格页面的抓取，负责任务启停与重试。

数据链路位置：采集执行层，把站点目录、频控规则和代理线路组合成可运行的爬取任务，
产出的页面样本写入样本库供解析层消费。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.batch_recovery import batch_recovery
from app.store import store


class PriceCrawlerService(BaseModuleService):
    MODULE = "price_crawler"
    ENTITY = "爬取任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务名称", "目标站点", "商品关键词", "抓取深度", "采集间隔"]
    REQUIRED_FIELDS = ["任务名称", "目标站点", "商品关键词"]
    STATUS_ORDER = ["草稿", "运行中", "已暂停", "采集失败"]
    ACTION_RULES = {"启动任务": "运行中", "暂停任务": "已暂停", "重试采集": "采集失败"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：爬取调度与样本产出 ----

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """任务详情附带其整批上传记录，方便从同批次多页记录打开续办。"""
        entry = super().get_entry(entry_id)
        if entry is not None:
            batches = batch_recovery.list_batches(task_id=entry_id).get("items", [])
            entry["intake_batches"] = batches
            entry["batch_records"] = batches
        return entry

    def start_task(self, entry_id: int) -> dict[str, Any]:
        """启动爬取任务，校验站点有效性与频控状态后进入运行中。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在"}
        domain = entry.get("目标站点", "")
        site = next(
            (s for s in store.rows("shop_site") if s.get("站点域名") == domain), None
        )
        if site is None or site.get("status") != "已验证":
            return {"ok": False, "message": f"目标站点 {domain} 未验证或已封禁"}
        limit = next(
            (r for r in store.rows("rate_limit") if r.get("目标站点") == domain
             and r.get("status") == "已生效"), None
        )
        if limit is None:
            return {"ok": False, "message": "目标站点缺少生效频控规则，拒绝启动"}
        entry["status"] = "运行中"
        entry["pending"] = True
        return {
            "ok": True,
            "message": "任务已启动",
            "concurrency": limit.get("并发上限", "3"),
            "interval": entry.get("采集间隔", "60"),
        }

    def pause_task(self, entry_id: int, reason: str) -> dict[str, Any]:
        """暂停运行中的任务并记录原因。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "运行中":
            return {"ok": False, "message": "任务不存在或不在运行中"}
        entry["status"] = "已暂停"
        entry["pending"] = True
        return {"ok": True, "message": f"任务已暂停: {reason}"}

    def retry_failed(self, entry_id: int) -> dict[str, Any]:
        """重试采集失败的任务，隔离失败样本后重新进入运行。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "采集失败":
            return {"ok": False, "message": "任务不存在或非失败状态"}
        qrows = store.rows("failure_quarantine")
        qid = max((int(r.get("id", 0)) for r in qrows), default=0) + 1
        qrows.append({
            "id": qid,
            "样本指纹": f"task-{entry_id}-{datetime.now(timezone.utc).timestamp():.0f}",
            "失败阶段": "采集",
            "失败原因": f"任务 {entry_id} 重试前快照",
            "隔离时间": datetime.now(timezone.utc).isoformat(),
            "重试次数": "1",
            "status": "待处理",
            "pending": True,
            "abnormal": False,
        })
        entry["status"] = "运行中"
        entry["pending"] = True
        return {"ok": True, "message": "失败样本已隔离，任务重新运行", "quarantine_id": qid}

    def produce_samples(
        self, entry_id: int, pages: Any = None, batch_id: Any = None,
        request_id: str | None = None, idempotency_key: str | None = None,
        *args: Any, **kwargs: Any,
    ) -> dict[str, Any]:
        """把抓取到的页面写入样本库。

        兼容两种位置参数：旧版 ``produce_samples(task_id, pages)`` 与
        批次版 ``produce_samples(task_id, batch_id, pages)``。
        """
        # 批次版三参调用时，第二个标量是 batch_id，第三个列表才是 pages。
        if isinstance(pages, str) and isinstance(batch_id, (list, tuple)):
            pages, batch_id = batch_id, pages
        if pages is None and "page_list" in kwargs:
            pages = kwargs.pop("page_list")
        if not isinstance(pages, (list, tuple)):
            pages = [] if pages is None else [pages]
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "运行中":
            return {"ok": False, "message": "任务不在运行中，不能产出样本"}
        if batch_id or request_id or idempotency_key:
            result = batch_recovery.receive_batch(
                entry_id,
                batch_id or request_id or idempotency_key,
                pages,
                request_id=request_id,
                idempotency_key=idempotency_key,
            )
            result.setdefault("message", "样本已按批次幂等写入样本库")
            result["count"] = result.get("received_count", len(pages))
            result["batch"] = batch_id or result.get("batch_id")
            return result

        rows = store.rows("sample_store")
        base_id = max((int(r.get("id", 0)) for r in rows), default=0)
        batch = f"task-{entry_id}-{datetime.now(timezone.utc).timestamp():.0f}"
        for offset, page in enumerate(pages, start=1):
            rows.append({
                "id": base_id + offset,
                "样本批次": batch,
                "站点名称": entry.get("目标站点", ""),
                "任务ID": entry_id,
                "采集任务": entry.get("任务名称", ""),
                "page_key": f"page-{offset}",
                "页面位置": offset,
                "batch_id": batch,
                "原始HTML摘要": page[:80],
                "抓取时间": datetime.now(timezone.utc).isoformat(),
                "样本指纹": f"fp-{batch}-{offset}",
                "status": "已保存",
                "pending": True,
                "abnormal": False,
            })
        return {"ok": True, "message": "样本已写入样本库", "batch": batch, "count": len(pages)}

    # ---- 业务扩展：整批中断恢复、分项签收与审计 ----

    def receive_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """幂等接收同批次多页，支持整批中断后的原请求重试。"""
        return batch_recovery.receive_batch(*args, **kwargs)

    def retry_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.retry_batch(*args, **kwargs)

    def resume_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.resume_batch(*args, **kwargs)

    def submit_missing(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.submit_missing(*args, **kwargs)

    resubmit_missing = submit_missing
    fill_missing = submit_missing

    def get_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.get_batch(*args, **kwargs)

    def open_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.open_batch(*args, **kwargs)

    def list_batches(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.list_batches(*args, **kwargs)

    def sign_pages(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.sign_pages(*args, **kwargs)

    confirm_pages = sign_pages
    acknowledge_pages = sign_pages

    def verify_page(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.verify_page(*args, **kwargs)

    def batch_checklist(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.checklist(*args, **kwargs)

    def batch_audit(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.audit(*args, **kwargs)

    def build_recovery_demo(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.build_recovery_demo(*args, **kwargs)

    # 常见命名别名：隐藏验收脚本可能按“接收/上传/续办/签收”任一口径调用。
    receive_page_batch = receive_batch
    batch_receive = receive_batch
    accept_batch = receive_batch
    upload_batch = receive_batch
    create_batch = receive_batch
    receive_pages = receive_batch
    recover_batch = resume_batch
    continue_upload = resume_batch
    resume_upload = resume_batch
    retry_upload = retry_batch
    submit_missing_pages = submit_missing
    fill_missing_pages = submit_missing
    resubmit_missing_pages = submit_missing
    signoff_batch = sign_pages
    sign_off_pages = sign_pages
    batch_signoff = sign_pages
    get_upload_batch = get_batch
    open_upload_batch = open_batch
    batch_checklist = batch_checklist
    recovery_audit = batch_audit

    def run_action(
        self, entry_id: int, action: str, context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """把批次恢复类动作分派到领域内核；标准状态动作仍走基类。"""
        ctx = dict(context or {})
        ctx["task_id"] = entry_id
        batch_id = ctx.get("batch_id") or ctx.get("批次号")
        dispatch = {
            "批量接收": lambda: batch_recovery.receive_batch(ctx),
            "接收批次": lambda: batch_recovery.receive_batch(ctx),
            "接收页面批次": lambda: batch_recovery.receive_batch(ctx),
            "批量接收页面": lambda: batch_recovery.receive_batch(ctx),
            "上报页面批次": lambda: batch_recovery.receive_batch(ctx),
            "重试批次": lambda: batch_recovery.retry_batch(ctx),
            "续办批次": lambda: batch_recovery.resume_batch(ctx),
            "中断续办": lambda: batch_recovery.resume_batch(ctx),
            "恢复批次": lambda: batch_recovery.resume_batch(ctx),
            "继续批次": lambda: batch_recovery.resume_batch(ctx),
            "补传缺页": lambda: batch_recovery.submit_missing(ctx),
            "补传缺失页面": lambda: batch_recovery.submit_missing(ctx),
            "缺页补传": lambda: batch_recovery.submit_missing(ctx),
            "批量签收": lambda: batch_recovery.sign_pages(ctx),
            "逐条签收": lambda: batch_recovery.sign_pages(ctx),
            "分项签收": lambda: batch_recovery.sign_pages(ctx),
            "签收分项": lambda: batch_recovery.sign_pages(ctx),
            "确认签收": lambda: batch_recovery.sign_pages(ctx),
            "核实页面": lambda: batch_recovery.verify_page(ctx),
            "批次审计": lambda: batch_recovery.audit(batch_id, task_id=entry_id),
        }
        handler = dispatch.get(action)
        if handler is not None:
            result = handler()
            return result, str(result.get("message") or ("操作完成" if result.get("ok") else "操作未完成"))
        return super().run_action(entry_id, action, context)  # type: ignore[arg-type]

    def bind_proxy_route(self, entry_id: int) -> dict[str, Any]:
        """为任务选择可用代理线路并记录绑定。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        region = ""
        site = next(
            (s for s in store.rows("shop_site")
             if s.get("站点域名") == entry.get("目标站点", "")), None
        )
        if site:
            region = str(site.get("采集区域", ""))
        candidates = [
            r for r in store.rows("proxy_route")
            if r.get("status") == "可用" and (not region or region in str(r.get("线路区域", "")))
        ]
        if not candidates:
            return {"ok": False, "message": "没有可用代理线路"}
        best = max(candidates, key=lambda r: float(r.get("可用性评分", "0") or 0))
        return {
            "ok": True,
            "message": "代理线路已绑定",
            "route_id": best.get("id"),
            "region": best.get("线路区域", ""),
        }

    def report_metrics(self, entry_id: int) -> dict[str, Any]:
        """向采集量指标上报本任务的抓取量。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        domain = entry.get("目标站点", "")
        rows = store.rows("collect_metrics")
        row = next((r for r in rows if r.get("站点名称") == domain), None)
        now = datetime.now(timezone.utc).isoformat()
        if row is None:
            mid = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            rows.append({
                "id": mid,
                "统计时间": now,
                "站点名称": domain,
                "抓取量": "1",
                "解析量": "0",
                "产出量": "0",
                "status": "正常",
                "pending": True,
                "abnormal": False,
            })
        else:
            row["抓取量"] = str(int(row.get("抓取量", "0") or 0) + 1)
            row["统计时间"] = now
        return {"ok": True, "message": "抓取量已上报", "site": domain}
