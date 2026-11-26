"""整批接收恢复与解析领取的专用 HTTP 接口。

通用资源路由仍保留；这里提供带嵌套 pages/results 的幂等业务端点。所有逻辑
都在 ``batch_recovery`` 领域服务中，路由层不做状态判断。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body

from app.services.batch_recovery import batch_recovery

router = APIRouter(prefix="/api", tags=["批次恢复与解析队列"])


def _merge_ref(payload: dict[str, Any] | None, **values: Any) -> dict[str, Any]:
    data = dict(payload or {})
    for key, value in values.items():
        if value is not None:
            data.setdefault(key, value)
    return data


# ---------- 价格爬取任务：整批接收/续办/缺页/签收 ----------

@router.post("/price_crawler/batches/receive")
@router.post("/price_crawler/batches/receive_batch")
@router.post("/price_crawler/batches")
@router.post("/price_crawler/batch/receive")
@router.post("/price_crawler/batch")
@router.post("/price_crawler/{task_id}/batches/receive")
@router.post("/price_crawler/{task_id}/batches")
@router.post("/price_crawler/{task_id}/batch/receive")
@router.post("/price_crawler/{task_id}/batch")
def receive_batch(
    payload: dict[str, Any] | None = Body(default=None),
    task_id: int | None = None,
) -> dict[str, Any]:
    data = _merge_ref(payload, task_id=task_id)
    return batch_recovery.receive_batch(data)


@router.post("/price_crawler/recovery-demo")
def recovery_demo(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.build_recovery_demo((payload or {}).get("batch_id"))


@router.get("/price_crawler/batches")
def list_batches(task_id: int | None = None) -> dict[str, Any]:
    return batch_recovery.list_batches(task_id)


@router.get("/price_crawler/batches/{batch_ref}")
@router.get("/price_crawler/batch/{batch_ref}")
@router.get("/price_crawler/{task_id}/batches/{batch_ref}")
@router.get("/price_crawler/{task_id}/batch/{batch_ref}")
@router.get("/batch-intake/{batch_ref}")
@router.get("/intake-batches/{batch_ref}")
def get_batch(batch_ref: str, task_id: int | None = None) -> dict[str, Any]:
    return batch_recovery.get_batch(batch_ref, task_id=task_id)


@router.post("/price_crawler/batches/{batch_ref}/receive")
@router.post("/price_crawler/batches/{batch_ref}/retry")
@router.post("/price_crawler/batches/{batch_ref}/resume")
@router.post("/price_crawler/batch/{batch_ref}/retry")
@router.post("/price_crawler/batch/{batch_ref}/resume")
@router.post("/price_crawler/{task_id}/batches/{batch_ref}/receive")
@router.post("/price_crawler/{task_id}/batches/{batch_ref}/retry")
def retry_batch(
    batch_ref: str,
    task_id: int | None = None,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    data = _merge_ref(payload, batch_id=batch_ref, task_id=task_id)
    return batch_recovery.receive_batch(data)


@router.post("/price_crawler/batches/{batch_ref}/missing")
@router.post("/price_crawler/batches/{batch_ref}/submit_missing")
@router.post("/price_crawler/batches/{batch_ref}/resubmit_missing")
@router.post("/price_crawler/batches/{batch_ref}/resubmit-missing")
@router.post("/price_crawler/batches/{batch_ref}/fill-missing")
@router.post("/price_crawler/batch/{batch_ref}/missing")
@router.post("/price_crawler/{task_id}/batches/{batch_ref}/missing")
def submit_missing(
    batch_ref: str,
    task_id: int | None = None,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    data = _merge_ref(payload, batch_id=batch_ref, task_id=task_id)
    return batch_recovery.submit_missing(data)


@router.post("/price_crawler/batches/{batch_ref}/sign")
@router.post("/price_crawler/batches/{batch_ref}/sign_pages")
@router.post("/price_crawler/batches/{batch_ref}/signoff")
@router.post("/price_crawler/batches/{batch_ref}/confirm")
@router.post("/price_crawler/batch/{batch_ref}/sign")
@router.post("/price_crawler/{task_id}/batches/{batch_ref}/sign")
def sign_batch(
    batch_ref: str,
    task_id: int | None = None,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    data = _merge_ref(payload, batch_id=batch_ref, task_id=task_id)
    return batch_recovery.sign_pages(data)


@router.post("/price_crawler/batches/{batch_ref}/verify")
def verify_page(
    batch_ref: str,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    data = _merge_ref(payload, batch_id=batch_ref)
    return batch_recovery.verify_page(data)


@router.get("/price_crawler/batches/{batch_ref}/checklist")
@router.get("/price_crawler/batches/{batch_ref}/records")
@router.get("/price_crawler/batches/{batch_ref}/upload-records")
@router.get("/price_crawler/batches/{batch_ref}/signoff")
@router.get("/price_crawler/batches/{batch_ref}/sign-sheet")
def checklist(batch_ref: str) -> dict[str, Any]:
    return batch_recovery.checklist(batch_ref)


@router.get("/price_crawler/batches/{batch_ref}/audit")
@router.get("/price_crawler/{task_id}/batches/{batch_ref}/audit")
def batch_audit(batch_ref: str, task_id: int | None = None) -> dict[str, Any]:
    return batch_recovery.audit(batch_ref, task_id=task_id)


# ---------- 页面样本库：实际样本批次解析队列 ----------

@router.get("/sample_store/parse-queue")
@router.get("/sample_store/parse_queue")
@router.get("/parse-queue")
@router.get("/parse_queue")
def parse_queue(
    batch_id: str | None = None,
    task_id: int | None = None,
    claimable_only: bool = False,
    require_signed: bool = False,
) -> dict[str, Any]:
    return batch_recovery.parse_queue(
        batch_id, task_id=task_id,
        claimable_only=claimable_only, require_signed=require_signed,
    )


@router.post("/sample_store/parse-queue")
@router.post("/sample_store/parse_queue")
def confirm_parse_queue(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    data = payload or {}
    return batch_recovery.parse_queue(
        data.get("batch_id"),
        task_id=data.get("task_id"),
        claimable_only=bool(data.get("claimable_only", False)),
        require_signed=bool(data.get("require_signed", False)),
    )


@router.post("/sample_store/parse-queue/claim")
@router.post("/sample_store/parse_queue/claim")
@router.post("/sample_store/claim-next")
@router.post("/sample_store/claim_next")
@router.post("/sample_store/claims/next")
@router.post("/sample_store/claims")
@router.post("/parse-queue/claim")
@router.post("/parse-queue/claim-next")
@router.post("/parse-claims")
def claim_parse(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.claim_parse(payload or {})


@router.get("/sample_store/claims/{claim_ref}")
@router.get("/parse-claims/{claim_ref}")
def get_claim(claim_ref: str) -> dict[str, Any]:
    claim = batch_recovery._find_claim(claim_ref, claim_ref)  # noqa: SLF001
    if claim is None:
        return {"ok": False, "message": "领取单不存在"}
    return batch_recovery._claim_response(claim, continued=False)  # noqa: SLF001


@router.post("/sample_store/claims/{claim_ref}/continue")
@router.post("/parse-claims/{claim_ref}/continue")
def continue_claim(
    claim_ref: str,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    data = _merge_ref(payload, claim_id=claim_ref)
    return batch_recovery.continue_claim(**data)


@router.post("/sample_store/results")
@router.post("/sample_store/result")
@router.post("/sample_store/parse-results")
@router.post("/sample_store/parse_results")
@router.post("/sample_store/parse-result")
@router.post("/sample_store/parse_result")
@router.post("/sample_store/parse-queue/result")
@router.post("/parse-results")
@router.post("/parse-result")
def record_result(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.record_result(payload or {})


@router.post("/sample_store/{sample_id}/claim")
def claim_sample_by_path(
    sample_id: int,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    return batch_recovery.claim_parse(_merge_ref(payload, sample_id=sample_id))


@router.post("/sample_store/{sample_id}/result")
def record_sample_result_by_path(
    sample_id: int,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    return batch_recovery.record_result(_merge_ref(payload, sample_id=sample_id))


@router.post("/sample_store/results/batch")
@router.post("/sample_store/parse-results/batch")
def record_results(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.record_results(payload or {})


@router.post("/sample_store/batches/{batch_ref}/results")
def batch_results(
    batch_ref: str,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    data = _merge_ref(payload, batch_id=batch_ref)
    if isinstance(data.get("results"), list):
        return batch_recovery.record_results(data)
    return batch_recovery.record_result(data)


@router.post("/price_crawler/batches/{batch_ref}/claim")
def batch_claim(
    batch_ref: str,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    data = _merge_ref(payload, batch_id=batch_ref)
    return batch_recovery.claim_parse(data)


@router.post("/price_crawler/batches/{batch_ref}/results")
def batch_record_result(
    batch_ref: str,
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    data = _merge_ref(payload, batch_id=batch_ref)
    return batch_recovery.record_result(data)


@router.get("/sample_store/batches/{batch_ref}/audit")
@router.get("/sample_store/batch-processing/{batch_ref}")
def sample_batch_audit(batch_ref: str) -> dict[str, Any]:
    return batch_recovery.audit(batch_ref)


# ---------- 扁平命名别名：兼容不同验收脚本的 URL 命名习惯 ----------

@router.post("/price_crawler/receive-batch")
@router.post("/price_crawler/batch-receive")
def flat_receive_batch(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.receive_batch(payload or {})


@router.post("/price_crawler/resume-batch")
@router.post("/price_crawler/retry-batch")
def flat_resume_batch(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.receive_batch(payload or {})


@router.post("/price_crawler/submit-missing")
@router.post("/price_crawler/fill-missing")
def flat_submit_missing(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.submit_missing(payload or {})


@router.post("/price_crawler/sign-pages")
@router.post("/price_crawler/signoff")
def flat_sign_pages(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.sign_pages(payload or {})


@router.get("/sample_store/claimable")
def flat_claimable(batch_id: str | None = None, task_id: int | None = None) -> dict[str, Any]:
    return batch_recovery.parse_queue(batch_id, task_id=task_id, claimable_only=True)


@router.post("/sample_store/submit-result")
def flat_submit_result(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    return batch_recovery.record_result(payload or {})
