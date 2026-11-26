"""整批采集中断后的幂等接收、分项签收与解析领取/结果留痕。

该服务不替换原有内存仓库，只在仓库中增加批次、分项、领取和结果表；
真正可解析的页面一定会在 ``sample_store`` 中留下样本行，解析队列再反向
关联该行的实际“样本批次”。
"""
from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from app.store import store

BATCH_TABLE = "crawl_intake_batch"
ITEM_TABLE = "crawl_intake_item"
QUEUE_TABLE = "sample_parse_queue"
CLAIM_TABLE = "parse_claim"
RESULT_TABLE = "parse_result"
PROCESS_TABLE = "sample_processing_record"

DELIVERED = "已送达"
MISSING = "缺页"
READ_FAILED = "读取失败"
PENDING_VERIFY = "待核实"
SIGNED = "已签收"
UNSIGNED = "未签收"
HANDED_TO_PARSER = "已转交解析"
CLAIMED = "已领取"
PARSED = "已解析"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0) or 0) for row in rows), default=0) + 1


def first_value(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            value = data[key]
            if isinstance(value, str) and not value.strip():
                continue
            return value
    return default


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def content_digest(content: Any) -> str:
    if not isinstance(content, (str, bytes)):
        content = repr(content)
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def has_usable_content(content: Any) -> bool:
    if content is None:
        return False
    if isinstance(content, str):
        if not content.strip():
            return False
        # 只有标签/空白/占位符的 HTML 仍应按“读不完整”处理，不能变成可解析空页。
        text = content
        for token in ("<[^>]*>", "&nbsp;", "&amp;", "&lt;", "&gt;"):
            text = re.sub(token, " ", text, flags=re.IGNORECASE)
        return bool(text.strip())
    if isinstance(content, (dict, list, tuple, set)):
        return len(content) > 0
    return True


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class BatchRecoveryService:
    """框架无关的批次恢复领域逻辑，HTTP 与旧服务方法都委托到这里。"""

    # ---------- 基础查找与标识 ----------

    def _rows(self, table: str) -> list[dict[str, Any]]:
        return store.rows(table)

    def _task_context(self, data: dict[str, Any]) -> tuple[int | None, dict[str, Any] | None, str, str]:
        task_id = _as_int(first_value(
            data, "task_id", "entry_id", "taskId", "任务ID", "爬取任务ID", "id"
        ))
        task = store.find("price_crawler", task_id) if task_id is not None else None
        task_name = str(first_value(
            data, "task_name", "taskName", "任务名称", "采集任务",
            default=(task or {}).get("任务名称", ""),
        ) or "")
        site = str(first_value(
            data, "site", "site_name", "siteName", "站点", "站点名称", "目标站点",
            default=(task or {}).get("目标站点", ""),
        ) or "")
        return task_id, task, task_name, site

    def _batch_identity(self, data: dict[str, Any]) -> tuple[str, str, int | None, str, str]:
        task_id, _task, task_name, site = self._task_context(data)
        batch_id = str(first_value(
            data, "batch_id", "batchId", "batch_no", "batchNo", "batch_code", "batchCode",
            "batch_ref", "批次号", "上传批次", "采集批次", "批次编号", "batch",
            default="",
        ) or "")
        request_id = str(first_value(
            data, "request_id", "requestId", "idempotency_key", "idempotencyKey",
            "幂等键", default="",
        ) or "")
        if not batch_id and request_id:
            batch_id = f"req-{request_id}"
        if not batch_id:
            batch_id = f"batch-{next_id(self._rows(BATCH_TABLE))}"
        owner = f"task:{task_id or '-'}|site:{site or '-'}"
        batch_uid = f"{owner}|batch:{batch_id}"
        return batch_id, batch_uid, task_id, task_name, site

    def _find_batch(
        self, ref: Any = None, task_id: Any = None, *, batch_id: Any = None,
        create: bool = False, data: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        payload = dict(data or {})
        if isinstance(ref, dict):
            payload.update(ref)
            ref = None
        if batch_id is not None:
            payload["batch_id"] = batch_id
        if ref is not None and not isinstance(ref, dict):
            if isinstance(ref, int) or (isinstance(ref, str) and ref.isdigit()):
                payload.setdefault("id", ref)
            else:
                payload.setdefault("batch_id", ref)
        if task_id is not None:
            payload["task_id"] = task_id

        wanted_id = _as_int(payload.get("id"))
        wanted_batch = first_value(
            payload, "batch_id", "batchId", "batch_no", "batchNo", "批次号", "上传批次", "批次编号",
        )
        wanted_task = _as_int(first_value(payload, "task_id", "entry_id", "taskId", "任务ID"))
        _, wanted_uid, _, _, _ = self._batch_identity(payload)

        rows = self._rows(BATCH_TABLE)
        batch = None
        if wanted_id is not None:
            batch = next((row for row in rows if int(row.get("id", 0) or 0) == wanted_id), None)
        if batch is None and wanted_batch is not None:
            # 业务批次号可能在不同任务/站点下重复，先按完整归属 UID 精确匹配。
            batch = next((row for row in rows if row.get("batch_uid") == wanted_uid), None)
            if batch is None:
                candidates = [
                    row for row in rows
                    if str(row.get("batch_id", "")) == str(wanted_batch)
                    or str(row.get("批次号", "")) == str(wanted_batch)
                ]
                wanted_site = first_value(payload, "site", "site_name", "站点", "站点名称", "目标站点")
                owner_scoped = wanted_task is not None or bool(wanted_site)
                if wanted_task is not None:
                    candidates = [
                        row for row in candidates
                        if _as_int(row.get("task_id")) == wanted_task
                    ]
                if wanted_site:
                    candidates = [
                        row for row in candidates
                        if str(row.get("site") or row.get("站点名称") or "") == str(wanted_site)
                    ]
                if not owner_scoped:
                    # 未提供归属时才允许按批次号打开唯一/最近记录。
                    batch = candidates[0] if candidates else None
                elif candidates:
                    batch = candidates[0]

        if batch is None and create:
            batch_id_value, batch_uid, tid, task_name, site = self._batch_identity(payload)
            batch = {
                "id": next_id(rows),
                "batch_id": batch_id_value,
                "batch_uid": batch_uid,
                "批次号": batch_id_value,
                "上传批次": batch_id_value,
                "task_id": tid,
                "任务ID": tid,
                "任务名称": task_name,
                "采集任务": task_name,
                "site": site,
                "站点名称": site,
                "sample_batch": batch_id_value,
                "样本批次": batch_id_value,
                "expected_count": 0,
                "received_count": 0,
                "signed_count": 0,
                "forwarded_count": 0,
                "claiming_count": 0,
                "parsed_count": 0,
                "missing_count": 0,
                "duplicate_count": 0,
                "status": "接收中",
                "created_at": now_iso(),
                "updated_at": now_iso(),
            }
            rows.append(batch)
        return batch

    def _items(self, batch: dict[str, Any]) -> list[dict[str, Any]]:
        uid = batch.get("batch_uid")
        return [row for row in self._rows(ITEM_TABLE) if row.get("batch_uid") == uid]

    def _find_item(
        self, batch: dict[str, Any], *, item_id: Any = None,
        page_key: Any = None, position: Any = None,
    ) -> dict[str, Any] | None:
        items = self._items(batch)
        iid = _as_int(item_id)
        if iid is not None:
            found = next((row for row in items if int(row.get("id", 0) or 0) == iid), None)
            if found:
                return found
        key = None if page_key is None else str(page_key)
        pos = _as_int(position)
        if pos is not None:
            found = next((row for row in items if _as_int(row.get("position")) == pos), None)
            if found:
                return found
        if key is not None:
            found = next((
                row for row in items
                if str(row.get("page_key", "")) == key
                or str(row.get("页面标识", "")) == key
                or str(row.get("page_name", "")) == key
            ), None)
            if found:
                return found
        return None

    # ---------- 页面规范化与清单 ----------

    def _page_identity(self, page: Any, index: int) -> tuple[int, str, str, bool]:
        position = index
        position_explicit = False
        key: Any = None
        name: Any = None
        if isinstance(page, dict):
            explicit_position = first_value(
                page, "position", "page_position", "pagePosition", "pageIndex", "page_index",
                "page_no", "pageNo", "seq", "sequence", "sort", "index",
                "页码", "位置", "页序", "序号",
            )
            if explicit_position is not None:
                position = _as_int(explicit_position) or index
                position_explicit = True
            key = first_value(
                page, "page_key", "pageKey", "page_id", "pageId", "code", "key",
                "页面标识", "页码标识", "页面编码",
            )
            name = first_value(page, "page_name", "pageName", "name", "页面名称", "名称")
        elif isinstance(page, (str, int, float)):
            text = str(page)
            position = index
            # 短标识（如“甲”）同时作为键和内容；长 HTML 不应被当成键。
            if len(text.strip()) <= 32 and "<" not in text and "\n" not in text:
                key = text.strip()
                name = key
        if key is None or str(key).strip() == "":
            key = f"page-{position}"
        key = str(key)
        if name is None or str(name).strip() == "":
            name = key
        return position, key, str(name), position_explicit

    def _normalize_page(self, page: Any, index: int) -> dict[str, Any]:
        position, key, name, position_explicit = self._page_identity(page, index)
        data = page if isinstance(page, dict) else {"content": page}
        content = first_value(
            data, "content", "html", "page_html", "pageHtml", "raw_html", "rawHtml",
            "html_content", "htmlContent", "body", "text",
            "原始HTML", "页面内容", "内容", "原始HTML摘要", "summary",
        )
        # 摘要字段也可能是唯一内容载体；上面已纳入。
        explicit_unreadable = first_value(
            data, "unreadable", "readable", "complete", "is_complete",
            "完整", "可读取", "ok", "success",
        )
        status_hint = str(first_value(data, "status", "state", "状态", default="") or "")
        reason = first_value(
            data, "missing_reason", "missingReason", "缺页原因", "failure_reason",
            "failureReason", "error_message", "errorMessage", "error", "reason", "原因",
        )
        reason = "" if reason is None else str(reason)

        unreadable = False
        if "readable" in data and data.get("readable") is False:
            unreadable = True
        if "complete" in data and data.get("complete") is False:
            unreadable = True
        if "is_complete" in data and data.get("is_complete") is False:
            unreadable = True
        if data.get("unreadable") is True:
            unreadable = True
        if status_hint.lower() in {"missing", "failed", "error", "incomplete", "缺页", "读取失败", "不完整", "失败"}:
            unreadable = True
        if not has_usable_content(content):
            unreadable = True
            if not reason:
                reason = "页面无法完整读取：未获得有效页面内容"
        elif unreadable and not reason:
            reason = "页面无法完整读取：内容不完整或读取中断"

        return {
            "position": position,
            "position_explicit": position_explicit,
            "page_key": key,
            "page_name": str(name),
            "content": content if has_usable_content(content) else None,
            "content_hash": content_digest(content) if has_usable_content(content) else "",
            "unreadable": unreadable,
            "missing_reason": reason if unreadable else "",
            "raw_status": status_hint,
            "receipt_lost": bool(first_value(data, "receipt_lost", "lost_receipt", "回执丢失", default=False)),
            "verification_status": first_value(
                data, "verification_status", "verify_status", "核实状态", default=None
            ),
        }

    def _ensure_manifest(self, batch: dict[str, Any], data: dict[str, Any]) -> None:
        expected = first_value(
            data, "expected_pages", "expectedPages", "page_keys", "pageKeys",
            "manifest", "pages_meta", "expected",
        )
        total = _as_int(first_value(
            data, "page_count", "pageCount", "total_pages", "totalPages",
            "expected_count", "expectedCount", "total",
        ))
        keys: list[Any] = []
        if isinstance(expected, int):
            total = expected
        elif isinstance(expected, list):
            keys = expected
            if total is None:
                total = len(keys)
        elif isinstance(expected, dict):
            keys = expected.get("pages") or expected.get("items") or []
            total = _as_int(expected.get("count")) or len(keys)

        if total:
            batch["expected_count"] = max(int(batch.get("expected_count") or 0), total)
        if keys:
            batch["expected_count"] = max(int(batch.get("expected_count") or 0), len(keys))

        if not total and not keys:
            return

        rows = self._rows(ITEM_TABLE)
        for index, raw in enumerate(keys or [], start=1):
            position, key, name, _position_explicit = self._page_identity(raw, index)
            if self._find_item(batch, page_key=key, position=position) is None:
                rows.append(self._new_item(batch, position, key, name))

        expected_total = int(batch.get("expected_count") or 0)
        existing_positions = {_as_int(row.get("position")) for row in self._items(batch)}
        for position in range(1, expected_total + 1):
            if position not in existing_positions:
                key = f"page-{position}"
                rows.append(self._new_item(batch, position, key, key))

    def _new_item(self, batch: dict[str, Any], position: int, key: str, name: str) -> dict[str, Any]:
        item_id = next_id(self._rows(ITEM_TABLE))
        return {
            "id": item_id,
            "batch_id": batch["batch_id"],
            "batch_uid": batch["batch_uid"],
            "批次号": batch["batch_id"],
            "上传批次": batch["batch_id"],
            "task_id": batch.get("task_id"),
            "任务ID": batch.get("task_id"),
            "任务名称": batch.get("任务名称", ""),
            "采集任务": batch.get("采集任务", ""),
            "site": batch.get("site", ""),
            "站点名称": batch.get("站点名称", ""),
            "position": position,
            "位置": position,
            "page_key": key,
            "页面标识": key,
            "page_name": name,
            "页面名称": name,
            "delivery_status": "未送达",
            "送达状态": "未送达",
            "delivered": False,
            "已送达": False,
            "verification_status": "已核实",
            "核实状态": "已核实",
            "signed": False,
            "已签收": False,
            "sign_status": UNSIGNED,
            "签收状态": UNSIGNED,
            "forwarded_to_parser": False,
            "已转交解析": False,
            "parse_status": "未转交",
            "解析状态": "未转交",
            "claim_id": None,
            "sample_id": None,
            "样本ID": None,
            "sample_batch": batch.get("sample_batch") or batch["batch_id"],
            "样本批次": batch.get("样本批次") or batch["batch_id"],
            "missing": True,
            "缺页": True,
            "missing_reason": "页面尚未送达",
            "缺页原因": "页面尚未送达",
            "content_hash": "",
            "submit_count": 0,
            "receive_count": 0,
            "duplicate_count": 0,
            "sign_count": 0,
            "claim_count": 0,
            "result_count": 0,
            "result_delivery_count": 0,
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }

    # ---------- 样本与队列 ----------

    def _sample_batch_name(self, batch: dict[str, Any]) -> str:
        return str(batch.get("sample_batch") or batch.get("样本批次") or batch.get("batch_id"))

    def _write_sample(self, batch: dict[str, Any], item: dict[str, Any], content: Any) -> dict[str, Any]:
        rows = self._rows("sample_store")
        digest = content_digest(content)
        sample_batch = self._sample_batch_name(batch)
        task_id = batch.get("task_id")
        site = batch.get("站点名称") or batch.get("site") or ""
        key = item["page_key"]
        # 指纹把任务/站点/批次/位置纳入作用域，避免同名页面跨归属互相冲掉。
        fingerprint = "fp-" + hashlib.sha1(
            f"{task_id or ''}|{site}|{batch['batch_id']}|{item['position']}|{key}|{digest}".encode("utf-8")
        ).hexdigest()[:20]
        records: list[dict[str, Any]] = []
        sample = {
            "id": next_id(rows),
            "样本批次": sample_batch,
            "sample_batch": sample_batch,
            "batch_uid": batch.get("batch_uid"),
            "上传批次": batch["batch_id"],
            "batch_id": batch["batch_id"],
            "task_id": task_id,
            "任务ID": task_id,
            "采集任务": batch.get("采集任务") or batch.get("任务名称") or "",
            "站点名称": site,
            "page_key": key,
            "页面标识": key,
            "page_name": item.get("page_name", key),
            "页面位置": item.get("position"),
            "intake_item_id": item.get("id"),
            "原始HTML摘要": str(content)[:80],
            "原始HTML": content if isinstance(content, str) else str(content),
            "内容指纹": digest,
            "样本指纹": fingerprint,
            "抓取时间": now_iso(),
            "status": "已保存",
            "pending": True,
            "abnormal": False,
            "claim_id": None,
            "领取状态": "待领取",
            "processing_records": records,
            "处理记录": records,
            "page_records": records,
            "process_records": records,
            "逐条处理记录": records,
            "records": records,
            "history": records,
            "parse_result": None,
            "解析结果": None,
            "result_id": None,
            "result_count": 0,
            "结果次数": 0,
        }
        rows.append(sample)
        item["sample_id"] = sample["id"]
        item["样本ID"] = sample["id"]
        item["sample_batch"] = sample_batch
        item["样本批次"] = sample_batch
        self._ensure_queue(sample)
        self._append_process_record(sample, "sample_written", "样本已实际写入样本库", {
            "batch_id": batch["batch_id"],
            "page_key": key,
            "item_id": item.get("id"),
        })
        return sample

    def _ensure_queue(self, sample: dict[str, Any]) -> dict[str, Any]:
        rows = self._rows(QUEUE_TABLE)
        queue = next((row for row in rows if row.get("sample_id") == sample.get("id")), None)
        actual_batch = str(sample.get("样本批次") or sample.get("sample_batch") or "")
        if queue is None:
            queue = {
                "id": next_id(rows),
                "queue_id": f"Q-{sample.get('id')}",
                "sample_id": sample.get("id"),
                "sample_batch": actual_batch,
                "样本批次": actual_batch,
                "batch_id": sample.get("batch_id"),
                "task_id": sample.get("task_id"),
                "站点名称": sample.get("站点名称", ""),
                "page_key": sample.get("page_key"),
                "position": sample.get("页面位置"),
                "status": "待领取",
                "claimable": sample.get("status") == "已保存",
                "blocked_reason": "",
                "claim_id": None,
                "result_id": None,
                "created_at": now_iso(),
                "updated_at": now_iso(),
            }
            rows.append(queue)
        else:
            # 始终以实际样本行上的批次为准，避免队列挂到未写成的名义批次。
            queue["sample_batch"] = actual_batch
            queue["样本批次"] = actual_batch
        return queue

    def _append_process_record(
        self, sample: dict[str, Any], event_type: str, message: str,
        detail: dict[str, Any] | None = None, *, idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        records = sample.setdefault("processing_records", [])
        # 所有命名别名共享同一条事件列表，避免不同读取口径看到不一致或重复事件。
        for alias in ("处理记录", "page_records", "process_records", "逐条处理记录", "records", "history"):
            sample[alias] = records
        if idempotency_key:
            existing = next((
                record for record in records
                if record.get("idempotency_key") == idempotency_key
            ), None)
            if existing:
                return existing
        record = {
            "id": next_id(self._rows(PROCESS_TABLE)),
            "type": event_type,
            "event": event_type,
            "message": message,
            "at": now_iso(),
            "idempotency_key": idempotency_key,
            "sample_id": sample.get("id"),
            "sample_batch": sample.get("样本批次"),
            "detail": detail or {},
        }
        records.append(record)
        self._rows(PROCESS_TABLE).append(record)
        return record

    def _quarantine_unreadable(
        self, batch: dict[str, Any], item: dict[str, Any], norm: dict[str, Any],
    ) -> Any:
        """把不可完整读取的页同步到失败隔离区，但绝不写成可解析空样本。"""
        rows = self._rows("failure_quarantine")
        reason = norm.get("missing_reason") or "页面不能完整读取"
        existing = next((
            row for row in rows
            if row.get("batch_id") == batch.get("batch_id")
            and str(row.get("page_key", "")) == str(item.get("page_key"))
        ), None)
        if existing is not None:
            existing["失败原因"] = reason
            existing["last_seen_at"] = now_iso()
            existing["重试次数"] = str(int(existing.get("重试次数", "0") or 0) + 1)
            item["quarantine_id"] = existing.get("id")
            return existing.get("id")
        qid = next_id(rows)
        row = {
            "id": qid,
            "batch_id": batch.get("batch_id"),
            "page_key": item.get("page_key"),
            "样本指纹": f"missing-{batch.get('batch_id')}-{item.get('position')}-{item.get('page_key')}",
            "失败阶段": "整批接收/页面读取",
            "失败原因": reason,
            "隔离时间": now_iso(),
            "last_seen_at": now_iso(),
            "重试次数": "0",
            "status": "待处理",
            "pending": True,
            "abnormal": True,
        }
        rows.append(row)
        item["quarantine_id"] = qid
        return qid

    def _sample_for_item(self, item: dict[str, Any]) -> dict[str, Any] | None:
        sample_id = _as_int(item.get("sample_id") or item.get("样本ID"))
        sample = store.find("sample_store", sample_id) if sample_id is not None else None
        if sample is not None:
            return sample
        # 修复异常的外键缺失：只在同归属、同位置/标识范围内找，绝不按同名全局覆盖。
        batch_uid = item.get("batch_uid")
        if batch_uid:
            found = next((
                row for row in self._rows("sample_store")
                if row.get("batch_uid") == batch_uid
                and (
                    str(row.get("page_key", "")) == str(item.get("page_key"))
                    or _as_int(row.get("页面位置")) == _as_int(item.get("position"))
                )
            ), None)
            if found is not None:
                return found
        batch_id = item.get("batch_id")
        task_id = item.get("task_id")
        site = item.get("site") or item.get("站点名称")
        return next((
            row for row in self._rows("sample_store")
            if str(row.get("batch_id", "")) == str(batch_id)
            and (task_id is None or _as_int(row.get("task_id")) == _as_int(task_id))
            and (not site or str(row.get("站点名称", "")) == str(site))
            and (
                str(row.get("page_key", "")) == str(item.get("page_key"))
                or _as_int(row.get("页面位置")) == _as_int(item.get("position"))
            )
        ), None)

    # ---------- 接收/重试/续办 ----------

    def receive_batch(
        self, first: Any = None, second: Any = None, third: Any = None,
        payload: dict[str, Any] | None = None, **kwargs: Any,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {}
        positional_pages: Any = None

        if isinstance(first, dict):
            data.update(first)
            if second is not None and positional_pages is None:
                positional_pages = second
        elif first is not None:
            first_is_task = _as_int(first) is not None and not isinstance(first, str)
            if first_is_task:
                # receive_batch(task_id, batch_id, pages) 或 receive_batch(task_id, pages)
                data["task_id"] = first
                if isinstance(second, (list, tuple)):
                    positional_pages = second
                elif isinstance(second, dict):
                    data.update(second)
                elif second is not None:
                    data["batch_id"] = second
                if isinstance(third, (list, tuple)):
                    positional_pages = third
                elif isinstance(third, dict):
                    data.update(third)
            else:
                data["batch_id"] = first
                if isinstance(second, (list, tuple)):
                    positional_pages = second
                    if _as_int(third) is not None:
                        data["task_id"] = third
                    elif isinstance(third, dict):
                        data.update(third)
                elif isinstance(second, dict):
                    data.update(second)
                    if isinstance(third, (list, tuple)):
                        positional_pages = third
                elif second is not None:
                    # 容错：receive_batch(batch_id, task_id, pages)。
                    if _as_int(second) is not None:
                        data["task_id"] = second
                        if isinstance(third, (list, tuple)):
                            positional_pages = third
                    else:
                        data["batch_id"] = second
                        if isinstance(third, (list, tuple)):
                            positional_pages = third

        if positional_pages is not None and "pages" not in data:
            data["pages"] = positional_pages
        data.update(kwargs)
        if payload:
            data.update(payload)

        # 兼容 pages 作为第二个位置参数且未被上面识别的调用。
        if "pages" not in data and second is not None and first is not None and not isinstance(first, dict):
            if isinstance(second, list):
                data["pages"] = second

        batch = self._find_batch(create=True, data=data)
        assert batch is not None
        self._ensure_manifest(batch, data)

        incoming = data.get("pages") or data.get("items") or data.get("页面") or data.get("page_list")
        if incoming is None and not any(key in data for key in ("content", "html", "page_key")):
            incoming = []
        elif incoming is None:
            incoming = [data]
        if isinstance(incoming, dict):
            # 支持 {甲: "<html>...", 乙: {...}} 的映射形式。
            normalized_incoming = []
            for key, value in incoming.items():
                if isinstance(value, dict):
                    value = {**value, "page_key": value.get("page_key", key)}
                else:
                    value = {"page_key": key, "content": value}
                normalized_incoming.append(value)
            incoming = normalized_incoming

        only_keys = (
            data.get("only_keys") or data.get("page_keys") or data.get("page_ids")
            or data.get("item_ids") or data.get("selected")
        )
        only_key_set = {str(key) for key in only_keys} if isinstance(only_keys, (list, tuple, set)) else None
        lost_receipt_values = data.get("lost_receipt_pages") or data.get("lost_receipt_for") or data.get("receipt_lost_pages") or []
        lost_receipt_keys = {str(key) for key in lost_receipt_values} if isinstance(lost_receipt_values, (list, tuple, set)) else {str(lost_receipt_values)}
        incomplete_values = data.get("incomplete_pages") or data.get("unreadable_pages") or data.get("failed_pages") or []
        incomplete_keys = {str(key) for key in incomplete_values} if isinstance(incomplete_values, (list, tuple, set)) else set()
        missing_only = bool(data.get("missing_only", False))

        receipts: list[dict[str, Any]] = []
        for index, raw_page in enumerate(incoming, start=1):
            norm = self._normalize_page(raw_page, index)
            if norm["page_key"] in lost_receipt_keys or str(norm["position"]) in lost_receipt_keys:
                norm["receipt_lost"] = True
                norm["verification_status"] = PENDING_VERIFY
            if norm["page_key"] in incomplete_keys or str(norm["position"]) in incomplete_keys:
                norm["unreadable"] = True
                if not norm["missing_reason"]:
                    norm["missing_reason"] = "页面不能完整读取，已保留缺页原因"
            if (
                only_key_set is not None
                and norm["page_key"] not in only_key_set
                and str(norm["position"]) not in only_key_set
            ):
                continue
            # 显式位置优先用于“同一位置重提”；只给页面标识时必须先按标识找，
            # 避免补传列表从第 1 项编号而误匹配到甲。
            lookup_position = norm["position"] if norm["position_explicit"] else None
            item = self._find_item(
                batch, page_key=norm["page_key"], position=lookup_position,
            )
            if item is None and lookup_position is None:
                item = self._find_item(batch, position=norm["position"])
            if item is None:
                rows = self._rows(ITEM_TABLE)
                item = self._new_item(batch, norm["position"], norm["page_key"], norm["page_name"])
                rows.append(item)
            if missing_only and item.get("delivered") and item.get("sample_id"):
                receipts.append(self._receipt(batch, item, duplicate=True, skipped="already_delivered"))
                continue
            receipts.append(self._receive_one(batch, item, norm))

        if not incoming and not data.get("no_reconcile"):
            # 空体重试/核实时，只要仓库中确有实际样本，就把“回执待核实”销项。
            for item in self._items(batch):
                if item.get("verification_status") == PENDING_VERIFY and self._sample_for_item(item) is not None:
                    item["verification_status"] = "已核实"
                    item["核实状态"] = "已核实"
                    receipt = self._receipt(batch, item, duplicate=True)
                    receipt["reconciled"] = True
                    receipts.append(receipt)

        self._refresh_batch(batch)
        result = self.get_batch(batch)
        result["receipts"] = receipts
        result["receipt_count"] = len(receipts)
        result["ok"] = True
        result["success"] = True
        return result

    def _receive_one(self, batch: dict[str, Any], item: dict[str, Any], norm: dict[str, Any]) -> dict[str, Any]:
        item["submit_count"] = int(item.get("submit_count") or 0) + 1
        item["receive_count"] = int(item.get("receive_count") or 0) + 1
        item["last_submitted_at"] = now_iso()
        old_hash = str(item.get("content_hash") or "")
        same_content = bool(old_hash and old_hash == norm["content_hash"])
        duplicate = False
        conflict = False

        if norm["unreadable"]:
            if item.get("delivered") and self._sample_for_item(item) is not None:
                # 已完好送达的页不能被后续一次不完整重试降级。
                item["last_failure_reason"] = norm["missing_reason"]
                return self._receipt(batch, item, duplicate=True, warning=norm["missing_reason"])
            item["delivery_status"] = READ_FAILED if norm["content"] is None else MISSING
            item["送达状态"] = item["delivery_status"]
            item["delivered"] = False
            item["已送达"] = False
            item["missing"] = True
            item["缺页"] = True
            item["missing_reason"] = norm["missing_reason"]
            item["缺页原因"] = norm["missing_reason"]
            item["verification_status"] = PENDING_VERIFY if norm.get("verification_status") == PENDING_VERIFY else "已核实"
            item["updated_at"] = now_iso()
            self._quarantine_unreadable(batch, item, norm)
            return self._receipt(batch, item, ok=False, incomplete=True)

        if item.get("delivered") and item.get("sample_id"):
            sample = self._sample_for_item(item)
            if same_content or (sample is not None and sample.get("内容指纹") == norm["content_hash"]):
                duplicate = True
                item["duplicate_count"] = int(item.get("duplicate_count") or 0) + 1
                batch["duplicate_count"] = int(batch.get("duplicate_count") or 0) + 1
                if sample is None:
                    sample = self._write_sample(batch, item, norm["content"])
                reconciled = False
                if (
                    item.get("verification_status") == PENDING_VERIFY
                    and not norm.get("receipt_lost")
                    and norm.get("verification_status") != PENDING_VERIFY
                ):
                    item["verification_status"] = "已核实"
                    item["核实状态"] = "已核实"
                    reconciled = True
                receipt = self._receipt(batch, item, duplicate=True)
                receipt["reconciled"] = reconciled
                item["updated_at"] = now_iso()
                return receipt
            conflict = True
            item["conflict_count"] = int(item.get("conflict_count") or 0) + 1
            item["conflict_reason"] = "同一位置收到不同内容，保留首次样本归属，未覆盖"
            return self._receipt(batch, item, ok=False, conflict=True,
                                 warning=item["conflict_reason"])

        # 从缺页/核实中恢复，或首次送达。
        item["content_hash"] = norm["content_hash"]
        item["delivery_status"] = DELIVERED
        item["送达状态"] = DELIVERED
        item["delivered"] = True
        item["已送达"] = True
        item["missing"] = False
        item["缺页"] = False
        item["missing_reason"] = ""
        item["缺页原因"] = ""
        verify = norm.get("verification_status")
        if verify in {PENDING_VERIFY, "待核实"} or norm.get("receipt_lost"):
            item["verification_status"] = PENDING_VERIFY
            item["核实状态"] = PENDING_VERIFY
        else:
            item["verification_status"] = "已核实"
            item["核实状态"] = "已核实"
            # 持久化回执只代表“已送达”；值班员仍通过 sign_pages 做逐项签收确认。
            item["receipt_issued"] = True
        item["received_at"] = now_iso()
        item["updated_at"] = now_iso()

        sample = self._sample_for_item(item)
        if sample is None:
            sample = self._write_sample(batch, item, norm["content"])
        else:
            # 外键仍在时不新增样本；确保队列关联实际批次。
            self._ensure_queue(sample)
        qid = _as_int(item.get("quarantine_id"))
        quarantine = store.find("failure_quarantine", qid) if qid is not None else next((
            row for row in self._rows("failure_quarantine")
            if row.get("batch_id") == batch.get("batch_id")
            and str(row.get("page_key", "")) == str(item.get("page_key"))
        ), None)
        if quarantine is not None:
            quarantine["status"] = "已恢复"
            quarantine["pending"] = False
            quarantine["recovered_at"] = now_iso()
        item["forwarded_to_parser"] = True
        item["已转交解析"] = True
        if item.get("parse_status") in (None, "未转交"):
            item["parse_status"] = HANDED_TO_PARSER
            item["解析状态"] = HANDED_TO_PARSER
        return self._receipt(batch, item, duplicate=duplicate, conflict=conflict)

    def _receipt(
        self, batch: dict[str, Any], item: dict[str, Any], *,
        ok: bool = True, duplicate: bool = False, incomplete: bool = False,
        conflict: bool = False, skipped: str = "", warning: str = "",
    ) -> dict[str, Any]:
        receipt_id = f"R-{batch['batch_id']}-{item.get('position')}-{item.get('page_key')}"
        item["receipt_id"] = receipt_id
        return {
            "ok": ok,
            "receipt_id": receipt_id,
            "batch_id": batch["batch_id"],
            "page_key": item.get("page_key"),
            "position": item.get("position"),
            "delivered": bool(item.get("delivered")),
            "delivery_status": item.get("delivery_status"),
            "verification_status": item.get("verification_status"),
            "sample_id": item.get("sample_id"),
            "sample_batch": item.get("sample_batch"),
            "duplicate": duplicate,
            "incomplete": incomplete,
            "conflict": conflict,
            "skipped": skipped,
            "warning": warning or item.get("conflict_reason") or item.get("missing_reason", ""),
            "missing_reason": item.get("missing_reason", ""),
        }

    def retry_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.receive_batch(*args, **kwargs)

    def resume_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.receive_batch(*args, **kwargs)

    def continue_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.receive_batch(*args, **kwargs)

    def submit_missing(
        self, batch_id_or_payload: Any = None, page_keys: Any = None,
        pages: Any = None, payload: dict[str, Any] | None = None, **kwargs: Any,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if isinstance(batch_id_or_payload, dict):
            data.update(batch_id_or_payload)
        elif batch_id_or_payload is not None:
            data["batch_id"] = batch_id_or_payload
        if isinstance(page_keys, dict) and pages is None:
            pages = page_keys.get("pages") or page_keys.get("items")
            page_keys = page_keys.get("page_keys") or page_keys.get("selected")
        elif isinstance(page_keys, (list, tuple)) and pages is None:
            if page_keys and all(isinstance(entry, dict) for entry in page_keys):
                # submit_missing(batch_id, [{"page_key": ..., "content": ...}])
                pages = page_keys
                page_keys = [entry.get("page_key") or entry.get("position") for entry in pages]
            # 字符串列表保留为选中的 page_keys；调用方可再通过 pages= 传内容。
        if page_keys is not None:
            data["page_keys"] = page_keys
            data["only_keys"] = page_keys
        if pages is not None:
            data["pages"] = pages
        if payload:
            data.update(payload)
        for alias in ("selected_missing", "missing_page_keys", "missing_keys"):
            if data.get(alias) and not data.get("page_keys"):
                data["page_keys"] = data[alias]
        batch_for_missing = self._find_batch(data=data)
        selected_refs = data.get("page_keys") or data.get("selected")
        raw_pages = data.get("pages")
        if isinstance(selected_refs, (list, tuple)) and isinstance(raw_pages, (list, tuple)):
            normalized_pages = []
            for index, raw_page in enumerate(raw_pages):
                ref = selected_refs[index] if index < len(selected_refs) else None
                entry = dict(raw_page) if isinstance(raw_page, dict) else {"content": raw_page}
                if batch_for_missing is not None and ref is not None:
                    target_item = self._find_item(batch_for_missing, item_id=ref, page_key=ref, position=ref)
                    if target_item is not None:
                        entry.setdefault("page_key", target_item.get("page_key"))
                        entry.setdefault("position", target_item.get("position"))
                if ref is not None:
                    entry.setdefault("page_key", str(ref))
                normalized_pages.append(entry)
            data["pages"] = normalized_pages

        id_refs = data.get("page_ids") or data.get("item_ids") or data.get("ids")
        if id_refs and not data.get("page_keys"):
            if batch_for_missing is not None:
                mapped_keys = []
                for ref in id_refs if isinstance(id_refs, (list, tuple, set)) else [id_refs]:
                    item = self._find_item(batch_for_missing, item_id=ref)
                    if item is not None:
                        mapped_keys.append(item.get("page_key"))
                if mapped_keys:
                    data["page_keys"] = mapped_keys
                    data["only_keys"] = mapped_keys
        data["missing_only"] = bool(data.get("missing_only", True))
        return self.receive_batch(payload=data, **kwargs)

    resubmit_missing = submit_missing
    fill_missing = submit_missing

    # ---------- 查询、清单、签收 ----------

    def get_batch(
        self, batch_id_or_payload: Any = None, task_id: Any = None,
        payload: dict[str, Any] | None = None, **kwargs: Any,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if isinstance(batch_id_or_payload, dict):
            data.update(batch_id_or_payload)
        elif batch_id_or_payload is not None:
            data["batch_id"] = batch_id_or_payload
        if task_id is not None:
            data["task_id"] = task_id
        data.update(kwargs)
        if payload:
            data.update(payload)
        batch = self._find_batch(data=data)
        if batch is None:
            return {"ok": False, "exists": False, "message": "批次不存在", "batch_id": data.get("batch_id")}
        self._refresh_batch(batch)
        items = [self._checklist_item(batch, item) for item in sorted(
            self._items(batch), key=lambda row: (_as_int(row.get("position")) or 0, int(row.get("id", 0)))
        )]
        unsigned = [item["page_key"] for item in items if not item["signed"]]
        missing = [item["page_key"] for item in items if item["missing"]]
        unsigned_items = [item for item in items if not item["signed"]]
        missing_items = [item for item in items if item["missing"]]
        result = dict(batch)
        record_map = {
            str(item["page_key"]): item.get("processing_records", [])
            for item in items
        }
        result.update({
            "ok": True,
            "exists": True,
            "id": batch.get("id"),
            "total": batch.get("expected_count"),
            "received": batch.get("received_count"),
            "missing": batch.get("missing_count"),
            "signed": batch.get("signed_count"),
            "parsed": batch.get("parsed_count"),
            "items": items,
            "pages": items,
            "checklist": items,
            "page_records": items,
            "processing_records_by_page": record_map,
            "unsigned_pages": unsigned,
            "unsigned_page_keys": unsigned,
            "unsigned_items": unsigned_items,
            "missing_pages": missing,
            "missing_page_keys": missing,
            "missing_items": missing_items,
            "resume_page_keys": unsigned,
            "complete": int(batch.get("missing_count") or 0) == 0,
            "all_received": int(batch.get("missing_count") or 0) == 0,
            "partial": int(batch.get("received_count") or 0) > 0 and int(batch.get("missing_count") or 0) > 0,
            "has_pending_verification": any(
                item.get("verification_status") == PENDING_VERIFY for item in items
            ),
            "resume_payload": {
                "batch_id": batch["batch_id"],
                "task_id": batch.get("task_id"),
                "page_keys": unsigned,
            },
        })
        return result

    def open_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.get_batch(*args, **kwargs)

    def list_batches(self, task_id: Any = None) -> dict[str, Any]:
        rows = self._rows(BATCH_TABLE)
        if task_id is not None:
            tid = _as_int(task_id)
            rows = [row for row in rows if _as_int(row.get("task_id")) == tid or str(row.get("task_id")) == str(task_id)]
        batches = [self.get_batch(row) for row in rows]
        return {"ok": True, "items": batches, "total": len(batches)}

    def _checklist_item(self, batch: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
        sample = self._sample_for_item(item)
        if sample is not None:
            item["sample_id"] = sample.get("id")
            item["样本ID"] = sample.get("id")
            item["sample_batch"] = sample.get("样本批次")
            item["样本批次"] = sample.get("样本批次")
            item["result_count"] = int(sample.get("result_count") or 0)
            item["result_delivery_count"] = int(sample.get("result_delivery_count") or sample.get("result_count") or 0)
        claimable, reason = self._claimability(item, sample)
        processing_records = sample.get("processing_records", []) if sample is not None else []
        delivered = bool(item.get("delivered") and sample is not None)
        forwarded = bool(item.get("forwarded_to_parser") or self._queue_for_sample(sample) is not None)
        signed = bool(item.get("signed"))
        parse_status = item.get("parse_status") or ("已解析" if item.get("result_count") else HANDED_TO_PARSER)
        return {
            "id": item.get("id"),
            "item_id": item.get("id"),
            "position": item.get("position"),
            "page": item.get("page_key"),
            "page_key": item.get("page_key"),
            "页面标识": item.get("page_key"),
            "page_name": item.get("page_name"),
            "页面名称": item.get("page_name"),
            "delivered": delivered,
            "已送达": delivered,
            "delivery_status": DELIVERED if delivered else item.get("delivery_status", "未送达"),
            "送达状态": DELIVERED if delivered else item.get("delivery_status", "未送达"),
            "verification_status": item.get("verification_status", "已核实"),
            "核实状态": item.get("verification_status", "已核实"),
            "signed": signed,
            "已签收": signed,
            "sign_status": SIGNED if signed else UNSIGNED,
            "签收状态": SIGNED if signed else UNSIGNED,
            "signed_by": item.get("signed_by", ""),
            "signed_at": item.get("signed_at"),
            "forwarded_to_parser": forwarded,
            "已转交解析": forwarded,
            "handover_status": HANDED_TO_PARSER if forwarded else "未转交",
            "转交状态": HANDED_TO_PARSER if forwarded else "未转交",
            "parse_status": parse_status,
            "解析状态": parse_status,
            "claim_id": item.get("claim_id"),
            "sample_id": item.get("sample_id"),
            "样本ID": item.get("sample_id"),
            "sample_batch": item.get("sample_batch"),
            "样本批次": item.get("sample_batch"),
            "sample_href": f"/sample_store?focus={item.get('sample_id')}" if item.get("sample_id") else "",
            "sample_link": f"/sample_store?focus={item.get('sample_id')}" if item.get("sample_id") else "",
            "missing": not delivered,
            "缺页": not delivered,
            "missing_reason": item.get("missing_reason", ""),
            "缺页原因": item.get("missing_reason", ""),
            "claimable": claimable,
            "can_claim": claimable,
            "blocked_reason": reason,
            "claim_blocked_reason": reason,
            "submit_count": item.get("submit_count", 0),
            "duplicate_count": item.get("duplicate_count", 0),
            "claim_count": item.get("claim_count", 0),
            "result_count": item.get("result_count", 0),
            "result_delivery_count": item.get("result_delivery_count", 0),
            "processing_records": processing_records,
            "处理记录": processing_records,
            "page_records": processing_records,
            "逐条处理记录": processing_records,
            "records": processing_records,
        }

    def sign_pages(
        self, batch_id_or_payload: Any = None, page_keys: Any = None,
        signed_by: str | None = None, payload: dict[str, Any] | None = None, **kwargs: Any,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if isinstance(batch_id_or_payload, dict):
            data.update(batch_id_or_payload)
        elif isinstance(batch_id_or_payload, (list, tuple, set)):
            data["page_keys"] = list(batch_id_or_payload)
        elif batch_id_or_payload is not None:
            data["batch_id"] = batch_id_or_payload
        if isinstance(page_keys, dict):
            data.update(page_keys)
        elif page_keys is not None:
            data["page_keys"] = page_keys
        if signed_by is not None:
            data["signed_by"] = signed_by
        data.update(kwargs)
        if payload:
            data.update(payload)

        batch = self._find_batch(data=data)
        if batch is None:
            return {"ok": False, "message": "批次不存在"}
        selected = (
            data.get("page_keys") or data.get("page_ids") or data.get("item_ids")
            or data.get("ids") or data.get("selected") or data.get("pages")
        )
        sign_all = bool(data.get("sign_all") or data.get("sign_all_received") or not selected)
        keys = {str(value) for value in selected} if isinstance(selected, (list, tuple, set)) else None
        actor = str(first_value(data, "signed_by", "operator", "reviewer", "值班员", default="值班员"))

        signed: list[dict[str, Any]] = []
        already: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for item in self._items(batch):
            wanted = sign_all
            if keys is not None:
                wanted = (
                    str(item.get("page_key")) in keys
                    or str(item.get("id")) in keys
                    or str(item.get("page_name")) in keys
                )
            if not wanted:
                continue
            sample = self._sample_for_item(item)
            if not item.get("delivered") or sample is None:
                blocked.append({
                    "page_key": item.get("page_key"),
                    "item_id": item.get("id"),
                    "reason": item.get("missing_reason") or "页面未实际送达，不能签收",
                })
                continue
            if item.get("signed"):
                item["sign_retry_count"] = int(item.get("sign_retry_count") or 0) + 1
                already.append({"page_key": item.get("page_key"), "signed_at": item.get("signed_at")})
                continue
            item["signed"] = True
            item["已签收"] = True
            item["sign_status"] = SIGNED
            item["签收状态"] = SIGNED
            item["signed_by"] = actor
            item["signed_at"] = now_iso()
            item["sign_count"] = int(item.get("sign_count") or 0) + 1
            signed.append({"page_key": item.get("page_key"), "sample_id": sample.get("id")})

        self._refresh_batch(batch)
        result = self.get_batch(batch)
        result.update({
            "ok": not blocked or bool(signed or already),
            "message": f"签收完成：新签 {len(signed)} 项，重复确认 {len(already)} 项，受阻 {len(blocked)} 项",
            "signed": signed,
            "already_signed": already,
            "blocked": blocked,
        })
        return result

    acknowledge_pages = sign_pages
    confirm_pages = sign_pages
    sign_off = sign_pages

    def verify_page(
        self, batch_id_or_payload: Any = None, page_key: Any = None,
        status: Any = None, payload: dict[str, Any] | None = None, **kwargs: Any,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if isinstance(batch_id_or_payload, dict):
            data.update(batch_id_or_payload)
        elif batch_id_or_payload is not None:
            data["batch_id"] = batch_id_or_payload
        if page_key is not None:
            data["page_key"] = page_key
        if status is not None:
            data["status"] = status
        data.update(kwargs)
        if payload:
            data.update(payload)
        batch = self._find_batch(data=data)
        if batch is None:
            return {"ok": False, "message": "批次不存在"}
        item = self._find_item(batch, page_key=data.get("page_key"), position=data.get("position"))
        if item is None:
            return {"ok": False, "message": "分项不存在"}
        target = str(first_value(data, "verification_status", "status", "verify", default=PENDING_VERIFY))
        if target in {"verified", "ok", "已核实", "false"} or data.get("verified") is True:
            target = "已核实"
        elif target in {"lost", "pending", "待核实", "receipt_lost"} or data.get("receipt_lost") is True:
            target = PENDING_VERIFY
        item["verification_status"] = target
        item["核实状态"] = target
        if target == "已核实" and item.get("delivered"):
            item["receipt_verified_at"] = now_iso()
        self._refresh_batch(batch)
        return {"ok": True, "page_key": item.get("page_key"), "verification_status": target,
                "batch": self.get_batch(batch)}

    reconcile_page = verify_page
    mark_verification = verify_page

    def checklist(self, batch_id: Any = None, **kwargs: Any) -> dict[str, Any]:
        result = self.get_batch(batch_id, **kwargs)
        if not result.get("ok"):
            return result
        return {"ok": True, "batch_id": result.get("batch_id"), "items": result["checklist"]}

    # ---------- 解析队列、领取、结果 ----------

    def _queue_for_sample(self, sample: dict[str, Any] | None) -> dict[str, Any] | None:
        if sample is None:
            return None
        return next((row for row in self._rows(QUEUE_TABLE) if row.get("sample_id") == sample.get("id")), None)

    def _claimability(
        self, item: dict[str, Any] | None, sample: dict[str, Any] | None,
        *, require_signed: bool = False,
    ) -> tuple[bool, str]:
        if sample is None:
            return False, "样本尚未实际写成，暂不可领取"
        if sample.get("status") == "已解析":
            return False, "该样本已有解析结果；如需重解析请先走回放/重新领取"
        if sample.get("status") == "已过期":
            return False, "样本已过期"
        if item is not None:
            if item.get("verification_status") == PENDING_VERIFY:
                return False, "写入回执待核实；幂等重试或核实实际样本后再领取"
            if require_signed and not item.get("signed"):
                return False, "页面尚未签收"
        queue = self._queue_for_sample(sample)
        if queue is not None and queue.get("status") == CLAIMED and queue.get("claim_id"):
            return False, f"已被领取（领取单 {queue.get('claim_id')}），应继续原领取"
        return True, "已有独立回执和实际样本，可逐项领取，无需等待整批确认"

    def parse_queue(
        self, batch_id: Any = None, task_id: Any = None, *,
        claimable_only: bool = False, require_signed: bool = False, **filters: Any,
    ) -> dict[str, Any]:
        # 从实际样本行同步，避免队列与样本批次脱节。
        for sample in list(self._rows("sample_store")):
            self._ensure_queue(sample)

        batch_ref = batch_id if not isinstance(batch_id, dict) else batch_id.get("batch_id")
        batch = self._find_batch(batch_ref, task_id=task_id, data=filters if isinstance(batch_id, dict) else None)
        rows = self._rows(QUEUE_TABLE)
        result_items: list[dict[str, Any]] = []
        for queue in rows:
            sample = store.find("sample_store", _as_int(queue.get("sample_id")) or -1)
            if sample is None:
                queue["claimable"] = False
                queue["blocked_reason"] = "队列指向的样本不存在"
                continue
            queue["sample_batch"] = sample.get("样本批次")
            queue["样本批次"] = sample.get("样本批次")
            if batch is not None and queue.get("batch_id") != batch.get("batch_id"):
                # 兼容样本直接带有相同样本批次但未带上传批次的情况。
                if sample.get("样本批次") != batch.get("sample_batch"):
                    continue
            if task_id is not None and _as_int(queue.get("task_id")) != _as_int(task_id):
                continue
            item = None
            if sample.get("intake_item_id"):
                item = next((
                    row for row in self._rows(ITEM_TABLE)
                    if int(row.get("id", 0) or 0) == _as_int(sample.get("intake_item_id"))
                ), None)
            claimable, reason = self._claimability(item, sample, require_signed=require_signed)
            queue["claimable"] = claimable
            queue["blocked_reason"] = reason
            enriched = dict(queue)
            enriched.update({
                "sample_status": sample.get("status"),
                "result_count": sample.get("result_count", 0),
                "result_delivery_count": sample.get("result_delivery_count", sample.get("result_count", 0)),
                "sample_href": f"/sample_store?focus={sample.get('id')}",
                "page_key": queue.get("page_key") or sample.get("page_key"),
                "processing_records": sample.get("processing_records", []),
                "处理记录": sample.get("processing_records", []),
                "逐条处理记录": sample.get("processing_records", []),
            })
            if not claimable_only or claimable:
                result_items.append(enriched)
        result_items.sort(key=lambda row: (_as_int(row.get("id")) or 0))
        return {
            "ok": True,
            "items": result_items,
            "queue": result_items,
            "records": result_items,
            "total": len(result_items),
            "claimable_count": sum(1 for row in result_items if row.get("claimable")),
        }

    def claim_parse(
        self, batch_id_or_payload: Any = None, sample_id: Any = None,
        page_key: Any = None, claim_id: str | None = None,
        worker_id: str | None = None, idempotency_key: str | None = None,
        payload: dict[str, Any] | None = None, **kwargs: Any,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if isinstance(batch_id_or_payload, dict):
            data.update(batch_id_or_payload)
        elif batch_id_or_payload is not None:
            data["batch_id"] = batch_id_or_payload
        if sample_id is not None:
            data["sample_id"] = sample_id
        if page_key is not None:
            data["page_key"] = page_key
        if claim_id is not None:
            data["claim_id"] = claim_id
        if worker_id is not None:
            data["worker_id"] = worker_id
        if idempotency_key is not None:
            data["idempotency_key"] = idempotency_key
        data.update(kwargs)
        if payload:
            data.update(payload)

        # 兼容 claim_parse(sample_id)：数字能找到样本且不是批次主键时按样本领取。
        raw_ref = batch_id_or_payload
        if (
            not isinstance(raw_ref, dict) and raw_ref is not None
            and data.get("sample_id") is None and data.get("claim_id") is None
        ):
            numeric_ref = _as_int(raw_ref)
            if numeric_ref is not None and store.find("sample_store", numeric_ref) is not None:
                maybe_batch = self._find_batch(raw_ref)
                if maybe_batch is None:
                    data["sample_id"] = numeric_ref
                    data.pop("batch_id", None)

        existing = self._find_claim(data.get("claim_id"), data.get("idempotency_key"))
        if existing is not None:
            existing["continue_count"] = int(existing.get("continue_count") or 0) + 1
            return self._claim_response(existing, continued=True)

        batch = self._find_batch(data=data)
        wanted_sample = _as_int(first_value(data, "sample_id", "sampleId", "样本ID"))
        wanted_page = first_value(data, "page_key", "pageKey", "页面标识")
        worker = first_value(data, "worker_id", "parser", "claimed_by", "解析员")
        batch_ref = batch.get("batch_id") if batch is not None else data.get("batch_id")

        # 重试领取但没有带 claim_id/幂等键时，若目标页已在某次领取单中，
        # 语义上应“继续原领取”，而不是再开一张单。
        if not data.get("allow_new", False):
            prior_claims = list(reversed(self._rows(CLAIM_TABLE)))
            prior = None
            if wanted_sample is not None:
                prior = next((
                    row for row in prior_claims
                    if wanted_sample in [_as_int(value) for value in row.get("sample_ids", [])]
                    and (worker is None or str(row.get("worker_id")) == str(worker))
                ), None)
            elif wanted_page is not None:
                for row in prior_claims:
                    for sid in row.get("sample_ids", []):
                        sample = store.find("sample_store", _as_int(sid) or -1)
                        if sample is not None and str(sample.get("page_key")) == str(wanted_page):
                            if batch_ref is None or str(row.get("batch_id")) == str(batch_ref):
                                prior = row
                                break
                    if prior is not None:
                        break
            if prior is not None:
                prior["continue_count"] = int(prior.get("continue_count") or 0) + 1
                return self._claim_response(prior, continued=True)

        require_signed = bool(data.get("require_signed", False))
        queue_payload = self.parse_queue(
            batch.get("batch_id") if batch is not None else None,
            task_id=data.get("task_id"), require_signed=require_signed,
        )
        candidates = queue_payload["items"]
        if wanted_sample is not None:
            candidates = [row for row in candidates if _as_int(row.get("sample_id")) == wanted_sample]
        if wanted_page is not None:
            candidates = [row for row in candidates if str(row.get("page_key")) == str(wanted_page)]
        if not data.get("include_claimed", False):
            open_candidates = [row for row in candidates if row.get("claimable")]
        else:
            open_candidates = candidates
        limit = _as_int(data.get("limit")) or 1
        chosen = open_candidates[:limit]
        if not chosen:
            # 原领取请求重试且未指定具体页时，返回该解析员/批次最近一次领取单。
            if not data.get("allow_new", False):
                fallback = next((
                    row for row in reversed(self._rows(CLAIM_TABLE))
                    if (batch_ref is None or str(row.get("batch_id")) == str(batch_ref))
                    and (worker is None or str(row.get("worker_id")) == str(worker))
                ), None)
                if fallback is not None:
                    fallback["continue_count"] = int(fallback.get("continue_count") or 0) + 1
                    return self._claim_response(fallback, continued=True)
            reason = "当前没有可领取项"
            if candidates:
                reason = str(candidates[0].get("blocked_reason") or reason)
            return {"ok": False, "claimable": False, "message": reason, "reason": reason,
                    "batch_id": batch.get("batch_id") if batch else data.get("batch_id")}

        claim_rows = self._rows(CLAIM_TABLE)
        cid = str(first_value(data, "claim_id", "claimId", default="") or f"claim-{next_id(claim_rows)}")
        while any(row.get("claim_id") == cid for row in claim_rows):
            cid = f"{cid}-{uuid.uuid4().hex[:6]}"
        claim = {
            "id": next_id(claim_rows),
            "claim_id": cid,
            "idempotency_key": data.get("idempotency_key"),
            "worker_id": worker or "解析员",
            "batch_id": batch.get("batch_id") if batch else chosen[0].get("batch_id"),
            "sample_batch": chosen[0].get("sample_batch"),
            "sample_ids": [row["sample_id"] for row in chosen],
            "status": CLAIMED,
            "created_at": now_iso(),
            "continue_count": 0,
        }
        claim_rows.append(claim)

        for chosen_row in chosen:
            sid = _as_int(chosen_row.get("sample_id"))
            sample = store.find("sample_store", sid or -1)
            if sample is None:
                continue
            # parse_queue 返回的是副本；这里必须修改仓库中的真实队列行。
            queue_row = self._queue_for_sample(sample) or self._ensure_queue(sample)
            queue_row["status"] = CLAIMED
            queue_row["claim_id"] = cid
            queue_row["claimed_by"] = claim["worker_id"]
            queue_row["claimed_at"] = now_iso()
            queue_row["updated_at"] = now_iso()
            sample["claim_id"] = cid
            sample["领取人"] = claim["worker_id"]
            sample["领取时间"] = now_iso()
            sample["领取状态"] = CLAIMED
            item = self._item_for_sample(sample)
            if item is not None:
                item["claim_id"] = cid
                item["claim_count"] = int(item.get("claim_count") or 0) + 1
                item["parse_status"] = CLAIMED
                item["解析状态"] = CLAIMED
            self._append_process_record(sample, "claimed", "解析领取已确认", {
                "claim_id": cid,
                "worker_id": claim["worker_id"],
            }, idempotency_key=f"claim:{cid}:{sid}")
        if batch is not None:
            self._refresh_batch(batch)
        return self._claim_response(claim, continued=False)

    claim_next = claim_parse
    claim_parse_item = claim_parse
    claim = claim_parse

    def _find_claim(self, claim_id: Any = None, idempotency_key: Any = None) -> dict[str, Any] | None:
        if claim_id is not None:
            found = next((row for row in self._rows(CLAIM_TABLE) if str(row.get("claim_id")) == str(claim_id)), None)
            if found:
                return found
            numeric_id = _as_int(claim_id)
            if numeric_id is not None:
                found = next((row for row in self._rows(CLAIM_TABLE) if _as_int(row.get("id")) == numeric_id), None)
                if found:
                    return found
        if idempotency_key:
            return next((
                row for row in self._rows(CLAIM_TABLE)
                if row.get("idempotency_key") == idempotency_key
            ), None)
        return None

    def _item_for_sample(self, sample: dict[str, Any]) -> dict[str, Any] | None:
        iid = _as_int(sample.get("intake_item_id"))
        if iid is not None:
            found = next((row for row in self._rows(ITEM_TABLE) if int(row.get("id", 0) or 0) == iid), None)
            if found:
                return found
        batch_uid = sample.get("batch_uid")
        if batch_uid:
            found = next((row for row in self._rows(ITEM_TABLE) if row.get("batch_uid") == batch_uid
                          and str(row.get("page_key", "")) == str(sample.get("page_key", ""))), None)
            if found:
                return found
        batch_id = sample.get("batch_id")
        return next((
            row for row in self._rows(ITEM_TABLE)
            if str(row.get("batch_id", "")) == str(batch_id)
            and (sample.get("task_id") is None or _as_int(row.get("task_id")) == _as_int(sample.get("task_id")))
            and str(row.get("page_key", "")) == str(sample.get("page_key", ""))
        ), None)

    def _claim_response(self, claim: dict[str, Any], *, continued: bool) -> dict[str, Any]:
        items = []
        for sid in claim.get("sample_ids", []):
            sample = store.find("sample_store", _as_int(sid) or -1)
            if sample is None:
                continue
            item = self._item_for_sample(sample)
            items.append({
                "sample_id": sid,
                "sample_batch": sample.get("样本批次"),
                "page_key": sample.get("page_key"),
                "status": sample.get("status"),
                "claim_id": claim.get("claim_id"),
                "processing_records": sample.get("processing_records", []),
                "batch_item": self._checklist_item(
                    self._find_batch(sample.get("batch_id")) or {}, item
                ) if item is not None and self._find_batch(sample.get("batch_id")) else None,
            })
        return {
            "ok": True,
            "id": claim.get("claim_id"),
            "claim_id": claim.get("claim_id"),
            "claim": claim,
            "idempotency_key": claim.get("idempotency_key"),
            "worker_id": claim.get("worker_id"),
            "continued": continued,
            "duplicate": continued,
            "status": claim.get("status"),
            "sample_ids": claim.get("sample_ids", []),
            "items": items,
            "message": "继续原领取单" if continued else "领取成功",
        }

    def continue_claim(self, claim_id: Any = None, **kwargs: Any) -> dict[str, Any]:
        existing = self._find_claim(claim_id or kwargs.get("idempotency_key"), kwargs.get("idempotency_key"))
        if existing is None:
            return {"ok": False, "message": "原领取单不存在"}
        existing["continue_count"] = int(existing.get("continue_count") or 0) + 1
        return self._claim_response(existing, continued=True)

    def record_result(
        self, claim_id_or_payload: Any = None, sample_id: Any = None,
        result: Any = None, result_id: str | None = None,
        idempotency_key: str | None = None, payload: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if isinstance(claim_id_or_payload, dict):
            data.update(claim_id_or_payload)
        elif claim_id_or_payload is not None:
            data["claim_id"] = claim_id_or_payload
        if sample_id is not None:
            data["sample_id"] = sample_id
        if result is not None:
            data["result"] = result
        if result_id is not None:
            data["result_id"] = result_id
        if idempotency_key is not None:
            data["idempotency_key"] = idempotency_key
        data.update(kwargs)
        if payload:
            data.update(payload)

        # 兼容 record_result(sample_id, result_dict) / record_result(claim_id, result_dict)。
        if isinstance(data.get("sample_id"), dict) and data.get("result") is None:
            data["result"] = data["sample_id"]
            data.pop("sample_id", None)
        raw_claim_ref = data.get("claim_id")
        if raw_claim_ref is not None and self._find_claim(raw_claim_ref, data.get("idempotency_key")) is None:
            numeric_ref = _as_int(raw_claim_ref)
            looks_like_sample = numeric_ref is not None and store.find("sample_store", numeric_ref) is not None
            if looks_like_sample and data.get("sample_id") is None:
                data["sample_id"] = raw_claim_ref
                data.pop("claim_id", None)

        if isinstance(data.get("results"), list):
            return self.record_results(data)

        claim = self._find_claim(data.get("claim_id"), data.get("idempotency_key"))
        sid = _as_int(first_value(data, "sample_id", "sampleId", "样本ID"))
        batch = self._find_batch(data=data)
        page_key = first_value(data, "page_key", "pageKey", "页面标识")
        sample = store.find("sample_store", sid or -1) if sid is not None else None
        if sample is None and claim is not None:
            candidate_ids = claim.get("sample_ids", [])
            if page_key is not None:
                for candidate_id in candidate_ids:
                    candidate = store.find("sample_store", _as_int(candidate_id) or -1)
                    if candidate is not None and str(candidate.get("page_key")) == str(page_key):
                        sample = candidate
                        break
            elif len(candidate_ids) == 1:
                sample = store.find("sample_store", _as_int(candidate_ids[0]) or -1)
        if sample is None and page_key is not None and batch is not None:
            item = self._find_item(batch, page_key=page_key)
            sample = self._sample_for_item(item) if item else None
        if sample is None and sid is None and batch is not None:
            # 单页批次或只带 claim 的场景。
            if claim is not None and len(claim.get("sample_ids", [])) == 1:
                sample = store.find("sample_store", _as_int(claim["sample_ids"][0]) or -1)
        if sample is None:
            return {"ok": False, "message": "找不到实际写成的页面样本，不能记录解析结果"}
        if batch is None and sample.get("batch_id"):
            batch = self._find_batch(
                sample.get("batch_id"), task_id=sample.get("task_id"),
            )

        sid = int(sample["id"])
        if claim is not None and sid not in [_as_int(value) for value in claim.get("sample_ids", [])]:
            return {"ok": False, "message": "该样本不属于本次领取单，不能用其他领取上下文记账"}

        result_key = str(first_value(data, "result_id", "idempotency_key", "resultId", default="") or "")
        existing = None
        if result_key:
            existing = next((
                row for row in self._rows(RESULT_TABLE)
                if str(row.get("result_id")) == result_key
                or str(row.get("idempotency_key")) == result_key
            ), None)
        if existing is None:
            existing = next((
                row for row in self._rows(RESULT_TABLE)
                if _as_int(row.get("sample_id")) == sid
                and (claim is None or row.get("claim_id") == claim.get("claim_id"))
            ), None)
        if existing is None and sample.get("result_id"):
            existing = next((
                row for row in self._rows(RESULT_TABLE)
                if str(row.get("result_id")) == str(sample.get("result_id"))
            ), None)

        if existing is not None:
            existing["delivery_count"] = int(existing.get("delivery_count") or 1) + 1
            sample["result_delivery_count"] = existing["delivery_count"]
            return {
                "ok": True,
                "duplicate": True,
                "message": "相同解析结果重提，仍只保留一份结果",
                "result_id": existing.get("result_id"),
                "sample_id": sid,
                "count": 1,
                "result_count": 1,
                "result_delivery_count": existing["delivery_count"],
                "batch_audit": self.audit(sample.get("batch_id")),
            }

        result_data = data.get("result") or data.get("parse_result") or data.get("解析结果")
        if result_data is None:
            reserved = {
                "claim_id", "sample_id", "page_key", "batch_id", "task_id", "result_id",
                "idempotency_key", "worker_id", "parser", "success", "ok", "payload",
                "result", "parse_result", "解析结果", "results",
            }
            result_data = {key: value for key, value in data.items() if key not in reserved}
        if not isinstance(result_data, dict):
            result_data = {"value": result_data}

        success = data.get("success", data.get("ok", True))
        if isinstance(success, str):
            success = success not in {"false", "0", "失败", "否"}
        result_id = result_key or f"RST-{sid}-{next_id(self._rows(RESULT_TABLE))}"
        row = {
            "id": next_id(self._rows(RESULT_TABLE)),
            "result_id": result_id,
            "idempotency_key": data.get("idempotency_key"),
            "claim_id": claim.get("claim_id") if claim else sample.get("claim_id"),
            "sample_id": sid,
            "sample_batch": sample.get("样本批次"),
            "batch_id": sample.get("batch_id"),
            "page_key": sample.get("page_key"),
            "result": result_data,
            "status": PARSED if success else "解析失败",
            "delivery_count": 1,
            "created_at": now_iso(),
        }
        self._rows(RESULT_TABLE).append(row)

        if success:
            was_parsed = sample.get("status") == PARSED
            sample["status"] = PARSED
            sample["pending"] = False
            sample["解析结果"] = result_data
            sample["parse_result"] = result_data
            sample["result_id"] = result_id
            sample["result_count"] = 1
            sample["结果次数"] = 1
            sample["result_delivery_count"] = 1
            queue = self._queue_for_sample(sample)
            if queue is not None:
                queue["status"] = PARSED
                queue["result_id"] = result_id
                queue["updated_at"] = now_iso()
            item = self._item_for_sample(sample)
            if item is not None:
                item["parse_status"] = PARSED
                item["解析状态"] = PARSED
                item["result_count"] = 1
                item["result_delivery_count"] = 1
            self._append_process_record(sample, "parse_result", "解析结果已写入样本批次记录", {
                "result_id": result_id,
                "claim_id": row["claim_id"],
                "result": result_data,
            }, idempotency_key=f"result:{result_id}")
            if not was_parsed:
                self._increment_parse_metric(sample.get("站点名称", ""))
        else:
            sample["last_parse_error"] = result_data
            self._append_process_record(sample, "parse_failed", "解析失败已留痕", result_data)

        if batch is not None:
            self._refresh_batch(batch)
        return {
            "ok": True,
            "duplicate": False,
            "message": "解析结果已记录",
            "result_id": result_id,
            "sample_id": sid,
            "count": 1,
            "result_count": 1,
            "result_delivery_count": 1,
            "batch_audit": self.audit(sample.get("batch_id")),
        }

    submit_result = record_result
    complete_claim = record_result

    def record_results(self, payload: dict[str, Any] | list[Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        data = payload if isinstance(payload, dict) else dict(kwargs)
        if isinstance(payload, list):
            results = payload
            data = kwargs
        else:
            results = (payload or {}).get("results") if isinstance(payload, dict) else kwargs.get("results")
        outcomes = []
        for entry in results or []:
            if not isinstance(entry, dict):
                entry = {"result": entry}
            merged = {**data, **entry}
            outcomes.append(self.record_result(merged))
        ok = all(bool(item.get("ok")) for item in outcomes)
        batch_id = data.get("batch_id") or (outcomes[0].get("batch_audit", {}) or {}).get("batch_id") if outcomes else None
        return {"ok": ok, "items": outcomes, "total": len(outcomes),
                "audit": self.audit(batch_id) if batch_id else None}

    submit_results = record_results

    def _increment_parse_metric(self, site: str) -> None:
        if not site:
            return
        metric = next((m for m in store.rows("collect_metrics") if m.get("站点名称") == site), None)
        if metric is not None:
            metric["解析量"] = str(int(metric.get("解析量", "0") or 0) + 1)
            metric["统计时间"] = now_iso()

    # ---------- 汇总/审计 ----------

    def _refresh_batch(self, batch: dict[str, Any]) -> None:
        items = self._items(batch)
        total = max(int(batch.get("expected_count") or 0), len(items))
        delivered = [item for item in items if item.get("delivered") and self._sample_for_item(item) is not None]
        signed = [item for item in delivered if item.get("signed")]
        forwarded = [item for item in delivered if item.get("forwarded_to_parser")]
        claimed = [item for item in items if item.get("claim_id")]
        parsed = [item for item in items if int(item.get("result_count") or 0) > 0]
        missing = total - len(delivered)
        if missing < 0:
            missing = 0
        batch.update({
            "expected_count": total,
            "received_count": len(delivered),
            "delivered_count": len(delivered),
            "signed_count": len(signed),
            "forwarded_count": len(forwarded),
            "claiming_count": len(claimed),
            "parsed_count": len(parsed),
            "missing_count": missing,
            "updated_at": now_iso(),
        })
        if any(item.get("verification_status") == PENDING_VERIFY for item in items):
            batch["status"] = PENDING_VERIFY
        elif missing:
            batch["status"] = "部分接收/待补传"
        elif parsed and len(parsed) == len(delivered):
            batch["status"] = "已解析"
        elif signed and len(signed) == len(delivered):
            batch["status"] = "已签收"
        elif delivered:
            batch["status"] = "已收齐待签收" if missing == 0 else "部分接收/待补传"
        else:
            batch["status"] = "接收中"

    def audit(self, batch_id: Any = None, **kwargs: Any) -> dict[str, Any]:
        if isinstance(batch_id, dict):
            kwargs.update(batch_id)
            batch_id = batch_id.get("batch_id")
        batch = self._find_batch(batch_id, **{k: v for k, v in kwargs.items() if k in {"task_id"}})
        if batch is None:
            return {"ok": False, "message": "批次不存在", "batch_id": batch_id}
        self._refresh_batch(batch)
        pages = {}
        for item in sorted(self._items(batch), key=lambda row: _as_int(row.get("position")) or 0):
            sample = self._sample_for_item(item)
            sample_count = len([
                row for row in self._rows("sample_store")
                if str(row.get("batch_id", "")) == str(batch.get("batch_id"))
                and str(row.get("page_key", "")) == str(item.get("page_key"))
            ])
            claims = [
                row for row in self._rows(CLAIM_TABLE)
                if sample is not None and int(sample.get("id")) in [
                    _as_int(value) for value in row.get("sample_ids", [])
                ]
            ]
            results = [
                row for row in self._rows(RESULT_TABLE)
                if sample is not None and _as_int(row.get("sample_id")) == int(sample.get("id"))
            ]
            pages[str(item.get("page_key"))] = {
                "position": item.get("position"),
                "delivery_status": item.get("delivery_status"),
                "verification_status": item.get("verification_status"),
                "signed": bool(item.get("signed")),
                "receive_request_count": int(item.get("receive_count") or item.get("submit_count") or 0),
                "sample_count": sample_count,
                "sample_id": sample.get("id") if sample else None,
                "claim_count": len(claims),
                "claim_ids": [row.get("claim_id") for row in claims],
                "result_count": len(results),
                "results_received": len(results),
                "result_delivery_count": sum(int(row.get("delivery_count") or 1) for row in results),
                "missing_reason": item.get("missing_reason", ""),
            }
        return {
            "ok": True,
            "batch_id": batch.get("batch_id"),
            "status": batch.get("status"),
            "totals": {
                "expected": batch.get("expected_count"),
                "received": batch.get("received_count"),
                "signed": batch.get("signed_count"),
                "missing": batch.get("missing_count"),
                "parsed": batch.get("parsed_count"),
            },
            "pages": pages,
            "page_audit": pages,
            "page_results": pages,
            "page_list": list(pages.values()),
        }

    # ---------- 验收场景数据 ----------

    def build_recovery_demo(self, batch_id: str | None = None) -> dict[str, Any]:
        """构造甲已达、乙回执待核实、丙未传完的可重复演练现场。"""
        bid = batch_id or f"JIA-YI-BING-{next_id(self._rows(BATCH_TABLE))}"
        task_rows = self._rows("price_crawler")
        task = next((row for row in task_rows if row.get("status") == "运行中"), task_rows[0] if task_rows else None)
        payload = {
            "batch_id": bid,
            "task_id": task.get("id") if task else None,
            "site": task.get("目标站点") if task else "demo.example",
            "expected_pages": [
                {"position": 1, "page_key": "甲", "page_name": "甲页"},
                {"position": 2, "page_key": "乙", "page_name": "乙页"},
                {"position": 3, "page_key": "丙", "page_name": "丙页"},
            ],
        }
        batch = self._find_batch(create=True, data=payload)
        assert batch is not None
        self._ensure_manifest(batch, payload)
        self.receive_batch({
            **payload,
            "pages": [
                {"position": 1, "page_key": "甲", "content": "<html>甲的完整价格页</html>"},
                {"position": 2, "page_key": "乙", "content": "<html>乙的完整价格页</html>",
                 "verification_status": PENDING_VERIFY},
                {"position": 3, "page_key": "丙", "complete": False,
                 "missing_reason": "连接中断：丙页面只传输了一半，无法完整读取"},
            ],
        })
        self.sign_pages(bid, ["甲"])
        return {
            "ok": True,
            "message": "已构造：甲已签收且可领取，乙已写入但回执待核实，丙缺页",
            "batch_id": bid,
            "batch": self.get_batch(bid),
            "claim_hint": "可直接领取甲；乙需幂等重试/核实后领取；丙补齐后才有样本和结果",
        }


batch_recovery = BatchRecoveryService()
