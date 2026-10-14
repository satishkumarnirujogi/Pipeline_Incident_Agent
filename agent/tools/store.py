"""Where the tool envelope keeps its records: tool_calls, actions, approvals.

PostgresToolStore is the real one; InMemoryToolStore serves unit tests.
"""

from __future__ import annotations

import uuid
from typing import Any, Protocol

from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from agent.db import agent_engine
from agent.models import Action, Approval, Escalation, ToolCall


class ToolStore(Protocol):
    def record_call(self, row: dict[str, Any]) -> None: ...
    def find_action(self, key: str) -> dict[str, Any] | None: ...
    def start_action(self, row: dict[str, Any]) -> None: ...
    def finish_action(self, key: str, status: str, result: dict[str, Any]) -> None: ...
    def count_succeeded(self, incident_id: str, tier: int) -> int: ...
    def get_approval(self, approval_id: str) -> dict[str, Any] | None: ...


class PostgresToolStore:
    def __init__(self) -> None:
        self.engine = agent_engine()

    def record_call(self, row: dict[str, Any]) -> None:
        with self.engine.begin() as conn:
            conn.execute(insert(ToolCall).values(**row))

    def find_action(self, key: str) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            r = conn.execute(
                select(Action.status, Action.result).where(Action.idempotency_key == key)
            ).first()
        return {"status": r.status, "result": r.result} if r else None

    def start_action(self, row: dict[str, Any]) -> None:
        stmt = pg_insert(Action).values(status="running", **row)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Action.idempotency_key],
            set_={
                "status": "running",
                "approval_id": stmt.excluded.approval_id,
                "executed_at": func.now(),
            },
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def finish_action(self, key: str, status: str, result: dict[str, Any]) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(Action)
                .where(Action.idempotency_key == key)
                .values(status=status, result=result)
            )

    def count_succeeded(self, incident_id: str, tier: int) -> int:
        with self.engine.connect() as conn:
            return conn.execute(
                select(func.count())
                .select_from(Action)
                .where(
                    Action.incident_id == incident_id,
                    Action.tier == tier,
                    Action.status == "succeeded",
                )
            ).scalar_one()

    def get_approval(self, approval_id: str) -> dict[str, Any] | None:
        try:
            aid = uuid.UUID(str(approval_id))
        except ValueError:
            return None
        with self.engine.connect() as conn:
            r = conn.execute(
                select(
                    Approval.incident_id, Approval.tool, Approval.args_hash, Approval.decision
                ).where(Approval.id == aid)
            ).first()
        return dict(r._mapping) if r else None

    # used by escalate_to_human; not part of the envelope protocol
    def add_escalation(self, row: dict[str, Any]) -> int:
        with self.engine.begin() as conn:
            return conn.execute(
                insert(Escalation).values(**row).returning(Escalation.id)
            ).scalar_one()


class InMemoryToolStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.actions: dict[str, dict[str, Any]] = {}
        self.approvals: dict[str, dict[str, Any]] = {}
        self.escalations: list[dict[str, Any]] = []

    def record_call(self, row: dict[str, Any]) -> None:
        self.calls.append(row)

    def find_action(self, key: str) -> dict[str, Any] | None:
        a = self.actions.get(key)
        return {"status": a["status"], "result": a.get("result")} if a else None

    def start_action(self, row: dict[str, Any]) -> None:
        self.actions[row["idempotency_key"]] = {**row, "status": "running"}

    def finish_action(self, key: str, status: str, result: dict[str, Any]) -> None:
        self.actions[key].update(status=status, result=result)

    def count_succeeded(self, incident_id: str, tier: int) -> int:
        return sum(
            1
            for a in self.actions.values()
            if a["incident_id"] == incident_id and a["tier"] == tier and a["status"] == "succeeded"
        )

    def get_approval(self, approval_id: str) -> dict[str, Any] | None:
        return self.approvals.get(str(approval_id))

    def add_escalation(self, row: dict[str, Any]) -> int:
        self.escalations.append(row)
        return len(self.escalations)
