"""页面样本库领域服务：保存原始页面样本用于解析回归与回放，负责样本指纹去重与过期治理。

数据链路位置：样本留存层，接收爬取任务产出的原始页面，向页面解析模板
提供待解析样本，向失败隔离区提供重试回放素材。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.batch_recovery import batch_recovery
from app.store import store


class SampleStoreService(BaseModuleService):
    MODULE = "sample_store"
    ENTITY = "页面样本"
    OPERATION_TERM = "抓取"
    LIST_FIELDS = ["样本批次", "站点名称", "原始HTML摘要", "抓取时间", "样本指纹"]
    REQUIRED_FIELDS = ["样本批次", "站点名称", "原始HTML摘要"]
    STATUS_ORDER = ["已保存", "已解析", "已过期"]
    ACTION_RULES = {"标记解析": "已解析", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：样本留存与指纹去重 ----

    def mark_parsed(self, entry_id: int) -> dict[str, Any]:
        """标记样本已解析并联动解析量指标。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在"}
        if entry.get("status") != "已保存":
            return {"ok": False, "message": "仅已保存样本可标记解析"}
        entry["status"] = "已解析"
        entry["pending"] = False
        queue = batch_recovery._queue_for_sample(entry)
        if queue is not None:
            queue["status"] = "已解析"
            queue["updated_at"] = datetime.now(timezone.utc).isoformat()
        item = batch_recovery._item_for_sample(entry)
        if item is not None:
            item["parse_status"] = "已解析"
            item["解析状态"] = "已解析"
        batch_recovery._append_process_record(entry, "marked_parsed", "样本已通过既有标记解析动作完成")
        site = entry.get("站点名称", "")
        row = next((m for m in store.rows("collect_metrics") if m.get("站点名称") == site), None)
        if row is not None:
            row["解析量"] = str(int(row.get("解析量", "0") or 0) + 1)
            row["统计时间"] = datetime.now(timezone.utc).isoformat()
        return {"ok": True, "message": "样本已标记解析", "site": site}

    def mark_expired(self, entry_id: int) -> dict[str, Any]:
        """把样本标记过期，释放回归占位。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "样本不存在"}
        entry["status"] = "已过期"
        entry["pending"] = False
        return {"ok": True, "message": "样本已过期"}

    # ---- 业务扩展：实际样本批次解析队列与幂等结果 ----

    def parse_queue(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.parse_queue(*args, **kwargs)

    def claim_parse(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.claim_parse(*args, **kwargs)

    claim_next = claim_parse
    claim_parse_item = claim_parse

    def continue_claim(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.continue_claim(*args, **kwargs)

    def submit_parse_result(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.record_result(*args, **kwargs)

    record_parse_result = submit_parse_result
    record_result = submit_parse_result
    submit_result = submit_parse_result

    def submit_parse_results(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.record_results(*args, **kwargs)

    def batch_processing_audit(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.audit(*args, **kwargs)

    audit_batch_processing = batch_processing_audit
    get_processing_audit = batch_processing_audit

    def get_parse_queue(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.parse_queue(*args, **kwargs)

    list_parse_queue = get_parse_queue
    fetch_claimable = get_parse_queue

    def claim_next_sample(self, first: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """兼容 claim_next_sample(sample_id/batch_payload, ...) 的直觉调用。"""
        payload: dict[str, Any] = {}
        if isinstance(first, dict):
            payload.update(first)
        elif first is not None:
            existing_sample = store.find(self.MODULE, int(first)) if str(first).isdigit() else None
            if existing_sample is not None:
                payload["sample_id"] = first
            else:
                payload["batch_id"] = first
        payload.update(kwargs)
        return batch_recovery.claim_parse(payload, *args)

    claim_sample = claim_next_sample
    claim_next = claim_next_sample
    claim_parse_item = claim_next_sample

    def complete_parse(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return batch_recovery.record_result(*args, **kwargs)

    save_parse_result = complete_parse
    record_parse_outcome = complete_parse

    def run_action(
        self, entry_id: int, action: str, context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        ctx = dict(context or {})
        ctx.setdefault("sample_id", entry_id)
        if action == "标记解析":
            result = self.mark_parsed(entry_id)
            return result, str(result.get("message", ""))
        dispatch = {
            "领取解析": lambda: batch_recovery.claim_parse(ctx),
            "领取解析任务": lambda: batch_recovery.claim_parse(ctx),
            "领取样本": lambda: batch_recovery.claim_parse(ctx),
            "确认领取": lambda: batch_recovery.claim_parse(ctx),
            "确认可领取项": lambda: batch_recovery.parse_queue(ctx.get("batch_id"), **{
                k: v for k, v in ctx.items() if k in {"task_id", "claimable_only", "require_signed"}
            }),
            "继续领取": lambda: batch_recovery.continue_claim(**ctx),
            "提交解析结果": lambda: batch_recovery.record_result(ctx),
            "提交解析": lambda: batch_recovery.record_result(ctx),
            "解析完成": lambda: batch_recovery.record_result(ctx),
            "记录解析结果": lambda: batch_recovery.record_result(ctx),
            "记录解析": lambda: batch_recovery.record_result(ctx),
        }
        handler = dispatch.get(action)
        if handler is not None:
            result = handler()
            return result, str(result.get("message") or ("操作完成" if result.get("ok") else "操作未完成"))
        return super().run_action(entry_id, action, context)  # type: ignore[arg-type]

    def dedup_by_fingerprint(self, entry_id: int) -> dict[str, Any]:
        """按样本指纹去重，重复样本直接过期。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "样本不存在"}
        fp = entry.get("样本指纹", "")
        duplicates = [
            s for s in store.rows(self.MODULE)
            if s.get("id") != entry.get("id") and s.get("样本指纹") == fp
        ]
        for dup in duplicates:
            dup["status"] = "已过期"
            dup["pending"] = False
        return {
            "ok": True,
            "message": "指纹去重完成",
            "duplicates_expired": len(duplicates),
            "fingerprint": fp,
        }

    def replay_to_parser(self, entry_id: int) -> dict[str, Any]:
        """把已解析样本重新回放到解析模板做回归验证。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "样本不存在"}
        site = entry.get("站点名称", "")
        template = next(
            (t for t in store.rows("page_parser")
             if t.get("适用站点") == site and t.get("status") == "已启用"), None
        )
        if template is None:
            return {"ok": False, "message": f"站点 {site} 没有启用中的解析模板"}
        entry["status"] = "已保存"
        entry["pending"] = True
        return {
            "ok": True,
            "message": "样本已回放解析",
            "template": template.get("模板名称", ""),
        }

    def quarantine_oldest(self, batch: str, max_keep: int = 50) -> dict[str, Any]:
        """批次超过容量时把最旧样本送入失败隔离区归档。"""
        rows = [r for r in store.rows(self.MODULE) if r.get("样本批次") == batch]
        if len(rows) <= max_keep:
            return {"ok": True, "message": "批次容量未超限", "count": len(rows)}
        overflow = sorted(rows, key=lambda r: str(r.get("抓取时间", "")))[:len(rows) - max_keep]
        qrows = store.rows("failure_quarantine")
        base_id = max((int(r.get("id", 0)) for r in qrows), default=0)
        for idx, sample in enumerate(overflow, start=1):
            qrows.append({
                "id": base_id + idx,
                "样本指纹": sample.get("样本指纹", ""),
                "失败阶段": "留存溢出",
                "失败原因": f"批次 {batch} 超出容量",
                "隔离时间": datetime.now(timezone.utc).isoformat(),
                "重试次数": "0",
                "status": "已丢弃",
                "pending": False,
                "abnormal": False,
            })
        return {"ok": True, "message": "溢出样本已隔离", "quarantined": len(overflow)}

    def site_sample_stats(self, site: str) -> dict[str, Any]:
        """统计站点样本量并更新采集量指标产出量。"""
        rows = [r for r in store.rows(self.MODULE) if r.get("站点名称") == site]
        parsed = sum(1 for r in rows if r.get("status") == "已解析")
        metrics = next((m for m in store.rows("collect_metrics") if m.get("站点名称") == site), None)
        if metrics is not None:
            metrics["产出量"] = str(parsed)
        return {
            "ok": True,
            "message": "站点样本统计完成",
            "total": len(rows),
            "parsed": parsed,
        }
