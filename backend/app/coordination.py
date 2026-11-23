"""协调模块：来源消费进度、处理单领用与增量归并确认。

解决三个归属问题：

1. 来源归属校验：增量归并只接受作业参与来源的输入，来源不匹配的整批拒绝，
   不会再把无关来源写进作业明细或推进任何来源进度。
2. 进度写入权限：只有归并成功的那一段（且落在实际领用区间内）才推进该作业
   对该来源的消费进度；尚未确认的输入不会被承认，下一次领用仍会重新发放。
3. 多作业共享来源：消费进度按「作业 × 来源」独立记账，两个作业可以各自领用，
   也可以登记在同一张联合处理单上；联合单成员等待另一作业确认时给出明确原因。

批次确认回执同时携带来源与作业标识、实际写入区间和行级结果，可直接核对。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SourceEvent:
    """来源输入行：按时间排序的一段可消费数据。"""
    event_id: int
    source_code: str
    event_time: str
    payload: dict[str, Any]


@dataclass
class JobConsumption:
    """单个作业对单个来源的消费进度（按作业独立记账，不与别的作业共享）。"""
    job_code: str
    source_code: str
    consumed_through: str
    confirmed_event_ids: set[int] = field(default_factory=set)


@dataclass
class Grant:
    """一张处理单上发给某个作业、针对某个来源的领用授权。"""
    grant_id: int
    ticket_id: int
    job_code: str
    source_code: str
    range_start: str
    range_end: str
    event_ids: list[int]
    status: str = "待确认"          # 待确认 / 已确认
    confirmed_through: str = ""
    blocking_reason: str = ""


@dataclass
class ProcessingTicket:
    """处理单：单作业单或多个作业共用来源的联合处理单。"""
    ticket_id: int
    ticket_code: str
    ticket_type: str                # 单作业 / 联合
    job_codes: list[str]
    source_codes: list[str]
    status: str = "开放"            # 开放 / 已结清
    grants: list[Grant] = field(default_factory=list)


@dataclass
class MergeReceipt:
    """批次进度回执：来源、作业、区间与行级结果都可核对。"""
    receipt_code: str
    job_code: str
    source_code: str
    ticket_code: str
    grant_id: int
    range_start: str
    range_end: str
    confirmed_through: str
    inserted: int
    updated: int
    deleted: int
    rejected: int
    written_event_ids: list[int]
    skipped_event_ids: list[int]
    status: str
    detail: str = ""


class CoordinationEngine:
    """协调引擎：按来源消费进度准备可执行区间，增量归并按实际领用选取输入。"""

    def __init__(self) -> None:
        self._sources: dict[str, list[SourceEvent]] = {}
        self._source_start: dict[str, str] = {}
        self._jobs: dict[str, list[str]] = {}
        self._consumption: dict[tuple[str, str], JobConsumption] = {}
        self._tickets: dict[int, ProcessingTicket] = {}
        self._grants: dict[int, Grant] = {}
        self._merged: dict[str, list[dict[str, Any]]] = {}
        self._receipts: list[MergeReceipt] = []
        self._next_event = 1
        self._next_ticket = 1
        self._next_grant = 1
        self._next_receipt = 1

    # ---- 登记 ----

    def register_job(self, job_code: str, participant_sources: list[str]) -> None:
        """登记融合作业及其参与来源；已登记来源的起点作为该作业的消费初值。"""
        self._jobs[job_code] = list(participant_sources)
        for source_code in participant_sources:
            key = (job_code, source_code)
            if key not in self._consumption:
                self._consumption[key] = JobConsumption(
                    job_code=job_code,
                    source_code=source_code,
                    consumed_through=self._source_start.get(source_code, ""),
                )

    def init_source_watermark(self, source_code: str, start_through: str) -> None:
        """登记来源基线水位：该时间点及之前的数据视为存量，不进入领用区间。"""
        self._source_start[source_code] = start_through
        for (job_code, src), consumption in self._consumption.items():
            if src == source_code and not consumption.consumed_through:
                consumption.consumed_through = start_through

    def append_source_events(self, source_code: str,
                             events: list[dict[str, Any]],
                             time_field: str = "event_time") -> list[int]:
        """追加来源输入行，返回登记后的事件编号（按时间升序保存）。"""
        rows = self._sources.setdefault(source_code, [])
        ids: list[int] = []
        for event in events:
            eid = self._next_event
            self._next_event += 1
            rows.append(SourceEvent(
                event_id=eid,
                source_code=source_code,
                event_time=str(event.get(time_field, "")),
                payload={k: v for k, v in event.items() if k != time_field},
            ))
            ids.append(eid)
        rows.sort(key=lambda row: row.event_time)
        return ids

    def create_joint_ticket(self, job_codes: list[str],
                            source_codes: list[str]) -> ProcessingTicket:
        """把多个作业共用的来源登记成一张联合处理单（单作业单在领用时自动开立）。"""
        if not job_codes or not source_codes:
            raise ValueError("联合处理单必须至少包含一个作业和一个来源")
        for job_code in job_codes:
            for source_code in source_codes:
                self._validate_job_source(job_code, source_code)
        ticket_id = self._next_ticket
        self._next_ticket += 1
        ticket = ProcessingTicket(
            ticket_id=ticket_id,
            ticket_code=f"JOIN-{ticket_id:04d}",
            ticket_type="联合" if len(job_codes) > 1 else "单作业",
            job_codes=list(job_codes),
            source_codes=list(source_codes),
        )
        self._tickets[ticket_id] = ticket
        return ticket

    # ---- 领用 ----

    def claim_input(self, job_code: str, source_code: str,
                    through: str | None = None,
                    ticket_id: int | None = None) -> dict[str, Any]:
        """协调员领取一个作业在某来源上的可执行区间。

        - 区间起点是该作业对该来源的消费进度，未确认的旧授权不会抬高起点；
        - 已存在未确认授权时幂等返回原授权，输入不会从下一次领用入口丢失；
        - 可挂到一张联合处理单上，联合单的成员各自持有独立授权。
        """
        self._validate_job_source(job_code, source_code)

        reusable = self._find_open_grant(job_code, source_code, ticket_id)
        if reusable is not None:
            ticket = self._tickets[reusable.ticket_id]
            return {
                "ok": True,
                "reused": True,
                "ticket_id": ticket.ticket_id,
                "ticket_code": ticket.ticket_code,
                "ticket_type": ticket.ticket_type,
                "grant_id": reusable.grant_id,
                "job_code": job_code,
                "source_code": source_code,
                "range_start": reusable.range_start,
                "range_end": reusable.range_end,
                "event_ids": list(reusable.event_ids),
                "status": reusable.status,
            }

        # 同一「作业 × 来源」已在别的处理单上持有在途授权时必须显式报错：
        # 否则新授权会静默丢掉被占用的输入，明细与作业归属对不上。
        blocking = self._find_open_grant(job_code, source_code, None)
        if blocking is not None:
            blocking_ticket = self._tickets[blocking.ticket_id]
            raise ValueError(
                f"作业 {job_code} 对来源 {source_code} 已在处理单 "
                f"{blocking_ticket.ticket_code}（授权 {blocking.grant_id}）持有在途领用，"
                f"请先确认或释放该授权后再从新处理单领用"
            )

        ticket = self._resolve_ticket(ticket_id, job_code, source_code)
        consumption = self._consumption[(job_code, source_code)]
        range_start = consumption.consumed_through
        events = self._available_events(job_code, source_code, through)

        grant_id = self._next_grant
        self._next_grant += 1
        grant = Grant(
            grant_id=grant_id,
            ticket_id=ticket.ticket_id,
            job_code=job_code,
            source_code=source_code,
            range_start=range_start,
            range_end=events[-1].event_time if events else (through or range_start),
            event_ids=[row.event_id for row in events],
        )
        if not events:
            grant.blocking_reason = (
                f"来源 {source_code} 在 {range_start or '起点'} 之后暂无可领取输入"
            )
        ticket.grants.append(grant)
        self._grants[grant_id] = grant
        self._annotate_ticket_waiting(ticket)

        return {
            "ok": True,
            "reused": False,
            "ticket_id": ticket.ticket_id,
            "ticket_code": ticket.ticket_code,
            "ticket_type": ticket.ticket_type,
            "grant_id": grant_id,
            "job_code": job_code,
            "source_code": source_code,
            "range_start": grant.range_start,
            "range_end": grant.range_end,
            "event_ids": list(grant.event_ids),
            "status": grant.status,
            "blocking_reason": grant.blocking_reason,
        }

    def release_grant(self, grant_id: int) -> dict[str, Any]:
        """释放尚未确认的领用授权，被占用的输入重新回到可领用入口。"""
        grant = self._grants.get(grant_id)
        if grant is None:
            return {"ok": False, "message": f"领用授权 {grant_id} 不存在"}
        if grant.status == "已确认":
            return {
                "ok": False,
                "message": f"授权 {grant_id} 已确认归并，不能释放",
            }
        ticket = self._tickets[grant.ticket_id]
        ticket.grants = [g for g in ticket.grants if g.grant_id != grant_id]
        del self._grants[grant_id]
        self._annotate_ticket_waiting(ticket)
        return {
            "ok": True,
            "released_grant_id": grant_id,
            "ticket_code": ticket.ticket_code,
            "returned_event_ids": list(grant.event_ids),
        }

    def executable_ranges(self, job_code: str) -> dict[str, Any]:
        """协调模块按来源消费进度准备可执行区间（不含已授权未确认的部分）。"""
        if job_code not in self._jobs:
            return {"ok": False, "message": f"融合作业 {job_code} 未登记"}
        ranges: dict[str, dict[str, Any]] = {}
        for source_code in self._jobs[job_code]:
            consumption = self._consumption[(job_code, source_code)]
            events = self._available_events(job_code, source_code, None)
            ranges[source_code] = {
                "consumed_through": consumption.consumed_through,
                "next_start": consumption.consumed_through,
                "range_end": events[-1].event_time if events else consumption.consumed_through,
                "available_event_ids": [row.event_id for row in events],
                "blocking_reason": "" if events else (
                    f"来源 {source_code} 已无可执行输入，等待新数据或在途授权确认"
                ),
            }
        return {"ok": True, "job_code": job_code, "ranges": ranges}

    # ---- 增量归并确认 ----

    def confirm_merge(self, job_code: str, source_code: str,
                      delta_records: list[dict[str, Any]],
                      grant_id: int,
                      join_key: str = "id") -> dict[str, Any]:
        """确认一批增量归并，回执可核对来源与作业。

        写入权限三重校验：作业必须参与该来源、授权必须属于本作业本来源、
        每条输入必须落在授权区间内。校验失败整批拒绝，不写任何进度。
        进度只推进到本批实际归并成功的最大事件时间。
        """
        try:
            self._validate_job_source(job_code, source_code)
        except ValueError as exc:
            return {"ok": False, "status": "拒绝", "message": str(exc)}

        grant = self._grants.get(grant_id)
        if grant is None:
            return {
                "ok": False, "status": "拒绝",
                "message": f"领用授权 {grant_id} 不存在，未授权输入不得归并",
            }
        if grant.job_code != job_code or grant.source_code != source_code:
            return {
                "ok": False, "status": "拒绝",
                "message": (
                    f"授权 {grant_id} 归属 {grant.job_code}/{grant.source_code}，"
                    f"与本次归并 {job_code}/{source_code} 不一致"
                ),
            }

        # 幂等：同一授权重跑不重复写入，也不跳过未处理的旧记录。
        if grant.status == "已确认":
            return self._build_receipt_result(
                self._receipt_for_grant(grant_id), reused=True)

        events_by_id = {
            row.event_id: row
            for row in self._sources.get(source_code, [])
        }
        selected: list[SourceEvent] = []
        rejected: list[dict[str, Any]] = []
        for record in delta_records:
            event_id = record.get("_event_id")
            row = events_by_id.get(event_id) if event_id is not None else None
            if row is None or event_id not in grant.event_ids:
                rejected.append({"event_id": event_id, "reason": "不在本作业领用区间内"})
                continue
            if not (grant.range_start < row.event_time <= grant.range_end):
                rejected.append({"event_id": event_id, "reason": "超出授权区间边界"})
                continue
            selected.append(row)

        if rejected:
            return {
                "ok": False, "status": "拒绝",
                "message": "存在未授权或越界输入，整批不予归并",
                "rejected": rejected,
                "grant_id": grant_id,
            }

        inserted = updated = deleted = 0
        written_ids: list[int] = []
        existing = {
            self._merge_key(row, join_key): row
            for row in self._merged.setdefault(job_code, [])
        }
        for row in selected:
            record = dict(row.payload)
            record["_source"] = source_code
            record["_event_id"] = row.event_id
            record["_event_time"] = row.event_time
            key = self._merge_key(record, join_key)
            op = record.get("_op", "upsert")
            if op == "delete":
                if key in existing:
                    del existing[key]
                    deleted += 1
            elif key in existing:
                existing[key].update(record)
                updated += 1
            else:
                existing[key] = record
                inserted += 1
            written_ids.append(row.event_id)

        self._merged[job_code] = list(existing.values())

        # 写入权限只覆盖归并成功的这一段：进度推进到实际写入的最大事件时间。
        consumption = self._consumption[(job_code, source_code)]
        new_through = max((row.event_time for row in selected),
                          default=consumption.consumed_through)
        if selected and new_through > consumption.consumed_through:
            consumption.consumed_through = new_through
        consumption.confirmed_event_ids.update(written_ids)

        grant.status = "已确认"
        grant.confirmed_through = consumption.consumed_through
        grant.blocking_reason = ""
        ticket = self._tickets[grant.ticket_id]
        if self._all_members_granted(ticket) and all(
                g.status == "已确认" for g in ticket.grants):
            ticket.status = "已结清"
        self._annotate_ticket_waiting(ticket)

        receipt = MergeReceipt(
            receipt_code=f"RECP-{self._next_receipt:04d}",
            job_code=job_code,
            source_code=source_code,
            ticket_code=ticket.ticket_code,
            grant_id=grant_id,
            range_start=grant.range_start,
            range_end=grant.range_end,
            confirmed_through=consumption.consumed_through,
            inserted=inserted,
            updated=updated,
            deleted=deleted,
            rejected=0,
            written_event_ids=written_ids,
            skipped_event_ids=[eid for eid in grant.event_ids if eid not in written_ids],
            status="已确认",
        )
        self._next_receipt += 1
        self._receipts.append(receipt)
        return self._build_receipt_result(receipt)

    # ---- 查询 ----

    def get_ticket(self, ticket_id: int) -> ProcessingTicket | None:
        return self._tickets.get(ticket_id)

    def ticket_intervals(self, ticket_id: int) -> dict[str, Any]:
        """查询处理单实际发放或待领的区间，含每个成员的等待原因。"""
        ticket = self._tickets.get(ticket_id)
        if ticket is None:
            return {"ok": False, "message": f"处理单 {ticket_id} 不存在"}
        self._annotate_ticket_waiting(ticket)
        members: list[dict[str, Any]] = []
        for job_code in ticket.job_codes:
            for source_code in ticket.source_codes:
                grant = next(
                    (g for g in ticket.grants
                     if g.job_code == job_code and g.source_code == source_code),
                    None,
                )
                consumption = self._consumption.get((job_code, source_code))
                if grant is None:
                    members.append({
                        "job_code": job_code,
                        "source_code": source_code,
                        "state": "待领取",
                        "range_start": consumption.consumed_through if consumption else "",
                        "range_end": "",
                        "event_ids": [],
                        "blocking_reason": "该作业尚未从本单领取输入",
                    })
                else:
                    members.append({
                        "job_code": job_code,
                        "source_code": source_code,
                        "state": grant.status,
                        "range_start": grant.range_start,
                        "range_end": grant.range_end,
                        "event_ids": list(grant.event_ids),
                        "confirmed_through": grant.confirmed_through,
                        "blocking_reason": grant.blocking_reason,
                    })
        return {
            "ok": True,
            "ticket_id": ticket.ticket_id,
            "ticket_code": ticket.ticket_code,
            "ticket_type": ticket.ticket_type,
            "status": ticket.status,
            "members": members,
        }

    def get_progress(self, job_code: str, source_code: str) -> dict[str, Any]:
        """核对某作业对某来源的消费进度。"""
        try:
            self._validate_job_source(job_code, source_code)
        except ValueError as exc:
            return {"ok": False, "message": str(exc)}
        consumption = self._consumption[(job_code, source_code)]
        return {
            "ok": True,
            "job_code": job_code,
            "source_code": source_code,
            "consumed_through": consumption.consumed_through,
            "confirmed_event_ids": sorted(consumption.confirmed_event_ids),
        }

    def merged_records(self, job_code: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self._merged.get(job_code, [])]

    def all_receipts(self) -> list[MergeReceipt]:
        return list(self._receipts)

    # ---- 内部辅助 ----

    def _validate_job_source(self, job_code: str, source_code: str) -> None:
        if job_code not in self._jobs:
            raise ValueError(f"融合作业 {job_code} 未登记")
        if source_code not in self._jobs[job_code]:
            raise ValueError(
                f"来源 {source_code} 不属于作业 {job_code} 的参与来源"
                f"（{', '.join(self._jobs[job_code])}），禁止归并或领用"
            )

    def _available_events(self, job_code: str, source_code: str,
                          through: str | None) -> list[SourceEvent]:
        """来源消费进度之后、未被在途授权占用、且不超过 through 的输入行。"""
        consumption = self._consumption[(job_code, source_code)]
        leased = {
            eid
            for grant in self._grants.values()
            if grant.job_code == job_code
            and grant.source_code == source_code
            and grant.status == "待确认"
            for eid in grant.event_ids
        }
        events = [
            row for row in self._sources.get(source_code, [])
            if row.event_time > consumption.consumed_through
            and row.event_id not in leased
            and (through is None or row.event_time <= through)
        ]
        return events

    def _find_open_grant(self, job_code: str, source_code: str,
                         ticket_id: int | None) -> Grant | None:
        for grant in self._grants.values():
            if (grant.job_code == job_code
                    and grant.source_code == source_code
                    and grant.status == "待确认"
                    and (ticket_id is None or grant.ticket_id == ticket_id)):
                return grant
        return None

    def _resolve_ticket(self, ticket_id: int | None,
                        job_code: str, source_code: str) -> ProcessingTicket:
        if ticket_id is not None:
            ticket = self._tickets.get(ticket_id)
            if ticket is None:
                raise ValueError(f"处理单 {ticket_id} 不存在")
            if job_code not in ticket.job_codes or source_code not in ticket.source_codes:
                raise ValueError(
                    f"处理单 {ticket.ticket_code} 不覆盖 {job_code}/{source_code}"
                )
            if ticket.status == "已结清":
                raise ValueError(f"处理单 {ticket.ticket_code} 已结清，不能继续领用")
            return ticket
        ticket_id = self._next_ticket
        self._next_ticket += 1
        ticket = ProcessingTicket(
            ticket_id=ticket_id,
            ticket_code=f"PROC-{ticket_id:04d}",
            ticket_type="单作业",
            job_codes=[job_code],
            source_codes=[source_code],
        )
        self._tickets[ticket_id] = ticket
        return ticket

    def _all_members_granted(self, ticket: ProcessingTicket) -> bool:
        """每个「作业 × 来源」成员都已实际领取过授权，才允许结清联合单。"""
        covered = {(g.job_code, g.source_code) for g in ticket.grants}
        required = {
            (job_code, source_code)
            for job_code in ticket.job_codes
            for source_code in ticket.source_codes
        }
        return required.issubset(covered)

    def _annotate_ticket_waiting(self, ticket: ProcessingTicket) -> None:
        """联合单上等待另一作业确认的成员必须带明确原因。"""
        confirmed_jobs = {g.job_code for g in ticket.grants if g.status == "已确认"}
        for grant in ticket.grants:
            if grant.status == "待确认":
                others = confirmed_jobs - {grant.job_code}
                if others:
                    grant.blocking_reason = (
                        f"等待作业 {', '.join(sorted(others))} 完成联合单确认后统一结清"
                    )
                elif not grant.blocking_reason:
                    grant.blocking_reason = "已发放，等待本作业提交归并确认"

    def _receipt_for_grant(self, grant_id: int) -> MergeReceipt:
        return next(r for r in self._receipts if r.grant_id == grant_id)

    @staticmethod
    def _build_receipt_result(receipt: MergeReceipt,
                              reused: bool = False) -> dict[str, Any]:
        return {
            "ok": True,
            "reused": reused,
            "status": receipt.status,
            "receipt_code": receipt.receipt_code,
            "job_code": receipt.job_code,
            "source_code": receipt.source_code,
            "ticket_code": receipt.ticket_code,
            "grant_id": receipt.grant_id,
            "range_start": receipt.range_start,
            "range_end": receipt.range_end,
            "confirmed_through": receipt.confirmed_through,
            "inserted": receipt.inserted,
            "updated": receipt.updated,
            "deleted": receipt.deleted,
            "rejected": receipt.rejected,
            "written_event_ids": list(receipt.written_event_ids),
            "skipped_event_ids": list(receipt.skipped_event_ids),
            "detail": receipt.detail,
        }

    @staticmethod
    def _merge_key(record: dict[str, Any], join_key: str) -> str:
        return f"{record.get('_source', '_')}#{record.get(join_key, '_')}"

    def domain_snapshot(self) -> dict[str, Any]:
        return {
            "module": "coordination",
            "sources": len(self._sources),
            "jobs": len(self._jobs),
            "tickets": len(self._tickets),
            "open_tickets": sum(1 for t in self._tickets.values() if t.status == "开放"),
            "grants": len(self._grants),
            "pending_grants": sum(1 for g in self._grants.values() if g.status == "待确认"),
            "receipts": len(self._receipts),
        }


# 全局单例，与 fusion_engine / replay_queue 的组织方式保持一致。
coordination_engine = CoordinationEngine()
