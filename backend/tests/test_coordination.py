"""协调模块测试：来源归属校验、处理单领用与增量归并进度写权限。

场景骨架（题面验收路径）：
- J1、J2 共享来源 A；另有来源 B 只属于 J1。
- 协调员既能领取单作业记录，也能把同一来源登记成一张联合处理单。
- 写入权限只覆盖归并成功的那一段，未确认输入不会从下一次领用入口丢失。
- 批次进度回执可核对来源与作业；联合单等待另一作业确认的成员带明确原因。
"""
from __future__ import annotations

import pytest

from app.coordination import CoordinationEngine
from app.fusion_engine import FusionEngine


def _build_engine() -> CoordinationEngine:
    engine = CoordinationEngine()
    # J1 只包含 A，另挂来源 B；J2 与 J1 共用 A。
    engine.register_job("J1", ["A", "B"])
    engine.register_job("J2", ["A"])
    engine.append_source_events("A", [
        {"id": "a1", "event_time": "2026-11-23T09:01:00", "name": "a-0901"},
        {"id": "a2", "event_time": "2026-11-23T09:02:00", "name": "a-0902"},
        {"id": "a3", "event_time": "2026-11-23T09:03:00", "name": "a-0903"},
        {"id": "a4", "event_time": "2026-11-23T09:04:00", "name": "a-0904"},
        {"id": "a5", "event_time": "2026-11-23T09:05:00", "name": "a-0905"},
    ])
    # 来源 B 已有 09:00 的存量基线，新增数据从 09:10 起。
    engine.init_source_watermark("B", "2026-11-23T09:00:00")
    engine.append_source_events("B", [
        {"id": "b1", "event_time": "2026-11-23T09:00:00", "name": "b-0900"},
        {"id": "b2", "event_time": "2026-11-23T09:10:00", "name": "b-0910"},
    ])
    return engine


@pytest.fixture
def engine() -> CoordinationEngine:
    return _build_engine()


class TestSourceOwnershipValidation:
    """来源归属校验：无关来源不能写作业明细，也不能推进任何来源进度。"""

    def test_foreign_source_merge_rejected_and_progress_untouched(self, engine: CoordinationEngine) -> None:
        """先用只包含 A 的 J1 指向来源 B 的越权输入，B 仍停在 09:00。"""
        # J1 参与 B，但本用例先制造一张只覆盖 A 的单作业单，再拿 B 的输入越权归并。
        claim = engine.claim_input("J1", "A", through="2026-11-23T09:05:00")
        grant_id = claim["grant_id"]

        b_events = engine._sources["B"]
        foreign = [
            {"_event_id": b_events[0].event_id, "id": "b1", "_op": "upsert"},
        ]
        result = engine.confirm_merge("J1", "B", foreign, grant_id=grant_id)
        assert result["ok"] is False
        assert result["status"] == "拒绝"

        # J1/B 进度仍停在来源起点 09:00，B 的输入没有被任何作业消费。
        progress_b = engine.get_progress("J1", "B")
        assert progress_b["consumed_through"] == "2026-11-23T09:00:00"
        assert engine.merged_records("J1") == []

    def test_source_not_participated_by_job_rejected_at_claim(self, engine: CoordinationEngine) -> None:
        """J2 不参与来源 B，连领用入口都不能开。"""
        with pytest.raises(ValueError, match="不属于作业 J2"):
            engine.claim_input("J2", "B")

    def test_grant_cross_job_source_mismatch_rejected(self, engine: CoordinationEngine) -> None:
        """授权归属与归并归属不一致时整批拒绝。"""
        grant_j1 = engine.claim_input("J1", "A", through="2026-11-23T09:05:00")
        a_events = engine._sources["A"]
        result = engine.confirm_merge(
            "J2", "A",
            [{"_event_id": a_events[0].event_id, "id": "a1"}],
            grant_id=grant_j1["grant_id"],
        )
        assert result["ok"] is False
        assert "授权" in result["message"]
        # J2 自己的消费进度没有被 J1 的授权带动。
        assert engine.get_progress("J2", "A")["consumed_through"] == ""


class TestJointTicketAndProgress:
    """联合处理单：两个作业共享来源，进度按作业独立记账。"""

    def test_full_joint_ticket_acceptance_path(self, engine: CoordinationEngine) -> None:
        # 1) 登记 J1/J2 共用来源 A 的联合处理单（单仍开放）。
        ticket = engine.create_joint_ticket(["J1", "J2"], ["A"])
        assert ticket.ticket_type == "联合"
        assert ticket.status == "开放"

        # 2) J1 从联合单领取 A 到 09:05 的区间并确认归并。
        claim_j1 = engine.claim_input(
            "J1", "A", through="2026-11-23T09:05:00", ticket_id=ticket.ticket_id)
        assert claim_j1["ticket_code"] == ticket.ticket_code
        assert len(claim_j1["event_ids"]) == 5

        a_events = engine._sources["A"]
        records_j1 = [
            {"_event_id": row.event_id, "id": row.payload["id"]}
            for row in a_events
        ]
        receipt_j1 = engine.confirm_merge("J1", "A", records_j1, grant_id=claim_j1["grant_id"])
        assert receipt_j1["ok"] is True
        assert receipt_j1["source_code"] == "A"
        assert receipt_j1["job_code"] == "J1"
        assert receipt_j1["confirmed_through"] == "2026-11-23T09:05:00"
        assert len(receipt_j1["written_event_ids"]) == 5

        # 3) 查询联合单实际发放/待领区间：J1 已确认，J2 待领取并带明确原因。
        intervals = engine.ticket_intervals(ticket.ticket_id)
        j1_member = next(m for m in intervals["members"] if m["job_code"] == "J1")
        j2_member = next(m for m in intervals["members"] if m["job_code"] == "J2")
        assert j1_member["state"] == "已确认"
        assert j2_member["state"] == "待领取"
        assert j2_member["blocking_reason"]

        # 联合单仍开放：两个确认进度发生冲突时由各作业独立进度隔离。
        assert intervals["status"] == "开放"
        assert engine.get_progress("J1", "A")["consumed_through"] == "2026-11-23T09:05:00"
        assert engine.get_progress("J2", "A")["consumed_through"] == ""

        # 4) 值班从这张联合单领取 J2 续办：J2 拿到自己的原区间，不被 J1 的进度抬高。
        claim_j2 = engine.claim_input("J2", "A", ticket_id=ticket.ticket_id)
        assert claim_j2["range_start"] == ""
        assert claim_j2["event_ids"] == [row.event_id for row in a_events]

        # 5) J2 提交归并，检查它实际写入 A 的旧记录（09:01~09:05 全部重放给 J2）。
        records_j2 = [
            {"_event_id": row.event_id, "id": row.payload["id"]}
            for row in a_events
        ]
        # 6) 同一联合单重跑 J1（联合单仍开放）：已确认输入不会重复写入。
        rerun_receipt = engine.confirm_merge(
            "J1", "A", records_j1, grant_id=claim_j1["grant_id"])
        assert rerun_receipt["ok"] is True
        assert rerun_receipt["reused"] is True
        assert rerun_receipt["written_event_ids"] == receipt_j1["written_event_ids"]
        # 协调模块按来源消费进度给出 J1 的可执行区间：09:05 之后没有新输入。
        ranges_j1 = engine.executable_ranges("J1")
        assert ranges_j1["ranges"]["A"]["available_event_ids"] == []

        receipt_j2 = engine.confirm_merge("J2", "A", records_j2, grant_id=claim_j2["grant_id"])
        assert receipt_j2["ok"] is True
        assert sorted(receipt_j2["written_event_ids"]) == [row.event_id for row in a_events]
        assert receipt_j2["confirmed_through"] == "2026-11-23T09:05:00"

        # 全部成员确认后联合单结清，结清单不能再领用新授权；J2 也没有跳过任何旧记录。
        assert engine.get_ticket(ticket.ticket_id).status == "已结清"
        with pytest.raises(ValueError, match="已结清"):
            engine.claim_input("J1", "A", ticket_id=ticket.ticket_id)
        # 无新数据时的单作业领用同样返回空区间，不会重放已确认的旧记录。
        fresh_claim = engine.claim_input("J1", "A")
        assert fresh_claim["event_ids"] == []
        assert len(engine.merged_records("J1")) == 5
        assert len(engine.merged_records("J2")) == 5

        # 全部成员确认后联合单结清。
        assert engine.get_ticket(ticket.ticket_id).status == "已结清"

    def test_unacknowledged_input_remains_claimable(self, engine: CoordinationEngine) -> None:
        """未确认的输入不会从下一次领用入口丢失。"""
        first = engine.claim_input("J1", "A", through="2026-11-23T09:03:00")
        assert len(first["event_ids"]) == 3
        # 不确认，再次领用：幂等返回同一张未确认授权，区间不变。
        again = engine.claim_input("J1", "A")
        assert again["reused"] is True
        assert again["grant_id"] == first["grant_id"]
        assert again["event_ids"] == first["event_ids"]
        assert engine.get_progress("J1", "A")["consumed_through"] == ""

    def test_pending_grant_blocks_claim_on_another_ticket(self, engine: CoordinationEngine) -> None:
        """在途授权未结清时，不能从新处理单静默领走被占用的输入。"""
        ticket = engine.create_joint_ticket(["J1", "J2"], ["A"])
        first = engine.claim_input(
            "J1", "A", through="2026-11-23T09:05:00", ticket_id=ticket.ticket_id)
        assert len(first["event_ids"]) == 5
        # 再开一张联合单领用同一作业/来源：必须显式报错而不是返回空区间。
        another = engine.create_joint_ticket(["J1", "J2"], ["A"])
        with pytest.raises(ValueError, match="在途领用"):
            engine.claim_input("J1", "A", ticket_id=another.ticket_id)
        # 释放后输入重新回到可领用入口，进度没有被推进。
        released = engine.release_grant(first["grant_id"])
        assert released["returned_event_ids"] == first["event_ids"]
        reclaimed = engine.claim_input("J1", "A", ticket_id=another.ticket_id)
        assert reclaimed["event_ids"] == first["event_ids"]
        assert engine.get_progress("J1", "A")["consumed_through"] == ""

    def test_confirmed_grant_cannot_be_released(self, engine: CoordinationEngine) -> None:
        """已确认授权不能释放，已归并的段不受影响。"""
        claim = engine.claim_input("J1", "A", through="2026-11-23T09:02:00")
        a_events = engine._sources["A"][:2]
        engine.confirm_merge(
            "J1", "A",
            [{"_event_id": row.event_id, "id": row.payload["id"]} for row in a_events],
            grant_id=claim["grant_id"],
        )
        result = engine.release_grant(claim["grant_id"])
        assert result["ok"] is False
        assert engine.get_progress("J1", "A")["consumed_through"] == "2026-11-23T09:02:00"

    def test_write_permission_bounded_to_merged_segment(self, engine: CoordinationEngine) -> None:
        """进度只承认归并成功的那一段，区间内未提交的行不被承认。"""
        claim = engine.claim_input("J1", "A", through="2026-11-23T09:05:00")
        a_events = engine._sources["A"]
        # 只确认前两行（到 09:02），授权区间虽然开到 09:05。
        partial = [
            {"_event_id": a_events[0].event_id, "id": "a1"},
            {"_event_id": a_events[1].event_id, "id": "a2"},
        ]
        # 授权允许部分提交：未提交行不应阻断，但越界行必须阻断。
        receipt = engine.confirm_merge("J1", "A", partial, grant_id=claim["grant_id"])
        assert receipt["ok"] is True
        assert receipt["confirmed_through"] == "2026-11-23T09:02:00"
        assert receipt["skipped_event_ids"] == [a_events[2].event_id,
                                                a_events[3].event_id,
                                                a_events[4].event_id]
        # 09:03 及之后的输入仍归 J1 自己消费，下一次领用继续发放。
        nxt = engine.claim_input("J1", "A")
        assert nxt["event_ids"] == [a_events[2].event_id,
                                    a_events[3].event_id,
                                    a_events[4].event_id]

    def test_out_of_range_input_rejected_without_any_write(self, engine: CoordinationEngine) -> None:
        """越界输入整批拒绝，作业明细和来源进度都不动。"""
        claim = engine.claim_input("J1", "A", through="2026-11-23T09:02:00")
        a_events = engine._sources["A"]
        result = engine.confirm_merge(
            "J1", "A",
            [{"_event_id": a_events[4].event_id, "id": "a5"}],  # 09:05 超出授权
            grant_id=claim["grant_id"],
        )
        assert result["ok"] is False
        assert result["rejected"]
        assert engine.merged_records("J1") == []
        assert engine.get_progress("J1", "A")["consumed_through"] == ""

    def test_joint_ticket_waiting_member_has_explicit_reason(self, engine: CoordinationEngine) -> None:
        """等待另一作业确认的成员要有明确原因。"""
        ticket = engine.create_joint_ticket(["J1", "J2"], ["A"])
        claim_j1 = engine.claim_input("J1", "A", ticket_id=ticket.ticket_id)
        a_events = engine._sources["A"]
        engine.confirm_merge(
            "J1", "A",
            [{"_event_id": row.event_id, "id": row.payload["id"]} for row in a_events],
            grant_id=claim_j1["grant_id"],
        )
        # J2 领取后挂起，联合单详情必须给出等待 J1 已确认、待 J2 续办的原因。
        claim_j2 = engine.claim_input("J2", "A", ticket_id=ticket.ticket_id)
        detail = engine.ticket_intervals(ticket.ticket_id)
        j2 = next(m for m in detail["members"] if m["job_code"] == "J2")
        assert j2["state"] == "待确认"
        assert "J1" in j2["blocking_reason"]
        assert claim_j2["grant_id"] != claim_j1["grant_id"]

    def test_receipt_reconciles_source_and_job(self, engine: CoordinationEngine) -> None:
        """批次进度回执能同时核对来源、作业、处理单和写入区间。"""
        ticket = engine.create_joint_ticket(["J1", "J2"], ["A"])
        claim = engine.claim_input(
            "J1", "A", through="2026-11-23T09:03:00", ticket_id=ticket.ticket_id)
        a_events = engine._sources["A"][:3]
        receipt = engine.confirm_merge(
            "J1", "A",
            [{"_event_id": row.event_id, "id": row.payload["id"]} for row in a_events],
            grant_id=claim["grant_id"],
        )
        assert receipt["job_code"] == "J1"
        assert receipt["source_code"] == "A"
        assert receipt["ticket_code"] == ticket.ticket_code
        assert receipt["range_start"] == ""
        assert receipt["range_end"] == "2026-11-23T09:03:00"
        assert receipt["confirmed_through"] == "2026-11-23T09:03:00"
        assert engine.all_receipts()[0].receipt_code.startswith("RECP-")


class TestFusionEngineOwnershipFix:
    """FusionEngine 增量归并入口的来源归属与水位写权限修复回归。"""

    def test_foreign_source_delta_rejected(self) -> None:
        engine = FusionEngine()
        engine.register_source("SRC-A", "src_a", "MySQL", "conn://a")
        job = engine.create_fusion_job("WT_X", ["SRC-A"], join_key="id")
        result = engine.execute_delta_merge(
            job.job_id,
            [{"_source": "SRC-B", "id": "x", "_timestamp": "2026-11-23T09:05:00"}],
        )
        assert result["ok"] is False
        assert "SRC-B" in result["message"]
        assert job.merged_records == []

    def test_watermark_of_other_source_not_advanced(self) -> None:
        engine = FusionEngine()
        engine.register_source("SRC-A", "src_a", "MySQL", "conn://a")
        engine.register_source("SRC-B", "src_b", "MySQL", "conn://b")
        job = engine.create_fusion_job("WT_X", ["SRC-A"], join_key="id")
        wm_b = engine.create_watermark("SRC-B", "2026-11-23T09:00:00")
        result = engine.execute_delta_merge(
            job.job_id,
            [{"_source": "SRC-A", "id": "a1", "_timestamp": "2026-11-23T09:05:00"}],
            watermark_record_id=wm_b.record_id,
        )
        assert result["ok"] is False
        assert wm_b.current_watermark == "2026-11-23T09:00:00"

    def test_watermark_advances_only_to_merged_segment(self) -> None:
        engine = FusionEngine()
        engine.register_source("SRC-A", "src_a", "MySQL", "conn://a")
        job = engine.create_fusion_job("WT_X", ["SRC-A"], join_key="id")
        wm = engine.create_watermark("SRC-A", "2026-11-23T09:00:00")
        result = engine.execute_delta_merge(
            job.job_id,
            [{"_source": "SRC-A", "id": "a1", "_timestamp": "2026-11-23T09:05:00"}],
            watermark_record_id=wm.record_id,
        )
        assert result["ok"] is True
        # 不再用当前时间覆盖，而是精确停在本批实际归并的最大事件时间。
        assert wm.current_watermark == "2026-11-23T09:05:00"
        assert result["watermark"]["data_source"] == "SRC-A"
        assert result["watermark"]["job_code"] == job.job_code
