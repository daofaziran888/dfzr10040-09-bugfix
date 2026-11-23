"""协调模块接口：来源消费进度、处理单领用与增量归并确认。

路由层不做业务判断，归属校验与进度写权限全部在 CoordinationEngine 内完成；
这里只负责把参数交给引擎并把 ValueError 转成 400。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.coordination import coordination_engine

router = APIRouter(prefix="/api/coordination", tags=["来源协调"])


class JobRegisterPayload(BaseModel):
    job_code: str
    participant_sources: list[str]


class SourceWatermarkPayload(BaseModel):
    source_code: str
    start_through: str


class SourceEventsPayload(BaseModel):
    source_code: str
    events: list[dict[str, Any]]
    time_field: str = "event_time"


class JointTicketPayload(BaseModel):
    job_codes: list[str]
    source_codes: list[str]


class ClaimPayload(BaseModel):
    job_code: str
    source_code: str
    through: str | None = None
    ticket_id: int | None = None


class MergePayload(BaseModel):
    job_code: str
    source_code: str
    grant_id: int
    delta_records: list[dict[str, Any]] = Field(default_factory=list)
    join_key: str = "id"


def _guard(call: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        result = call(*args, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(result, dict):
        return result
    return {"ok": True, "data": result}


@router.post("/jobs")
def register_job(payload: JobRegisterPayload) -> dict[str, Any]:
    try:
        coordination_engine.register_job(
            payload.job_code, payload.participant_sources)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


@router.post("/sources/watermark")
def init_source_watermark(payload: SourceWatermarkPayload) -> dict[str, Any]:
    coordination_engine.init_source_watermark(
        payload.source_code, payload.start_through)
    return {"ok": True}


@router.post("/sources/events")
def append_source_events(payload: SourceEventsPayload) -> dict[str, Any]:
    ids = coordination_engine.append_source_events(
        payload.source_code, payload.events, payload.time_field)
    return {"ok": True, "event_ids": ids}


@router.post("/tickets")
def create_joint_ticket(payload: JointTicketPayload) -> dict[str, Any]:
    try:
        ticket = coordination_engine.create_joint_ticket(
            payload.job_codes, payload.source_codes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "ok": True,
        "ticket_id": ticket.ticket_id,
        "ticket_code": ticket.ticket_code,
        "ticket_type": ticket.ticket_type,
        "status": ticket.status,
    }


@router.get("/tickets/{ticket_id}")
def get_ticket(ticket_id: int) -> dict[str, Any]:
    return _guard(coordination_engine.ticket_intervals, ticket_id)


@router.post("/claims")
def claim_input(payload: ClaimPayload) -> dict[str, Any]:
    return _guard(
        coordination_engine.claim_input,
        payload.job_code, payload.source_code,
        payload.through, payload.ticket_id,
    )


@router.post("/claims/{grant_id}/release")
def release_grant(grant_id: int) -> dict[str, Any]:
    return coordination_engine.release_grant(grant_id)


@router.get("/ranges/{job_code}")
def executable_ranges(job_code: str) -> dict[str, Any]:
    return coordination_engine.executable_ranges(job_code)


@router.get("/progress/{job_code}/{source_code}")
def get_progress(job_code: str, source_code: str) -> dict[str, Any]:
    return coordination_engine.get_progress(job_code, source_code)


@router.post("/merges/confirm")
def confirm_merge(payload: MergePayload) -> dict[str, Any]:
    return coordination_engine.confirm_merge(
        payload.job_code, payload.source_code,
        payload.delta_records, payload.grant_id, payload.join_key,
    )


@router.get("/receipts")
def list_receipts() -> dict[str, Any]:
    receipts = coordination_engine.all_receipts()
    return {
        "ok": True,
        "items": [
            {
                "receipt_code": r.receipt_code,
                "job_code": r.job_code,
                "source_code": r.source_code,
                "ticket_code": r.ticket_code,
                "grant_id": r.grant_id,
                "range_start": r.range_start,
                "range_end": r.range_end,
                "confirmed_through": r.confirmed_through,
                "inserted": r.inserted,
                "updated": r.updated,
                "deleted": r.deleted,
                "rejected": r.rejected,
                "written_event_ids": r.written_event_ids,
                "skipped_event_ids": r.skipped_event_ids,
                "status": r.status,
            }
            for r in receipts
        ],
    }
