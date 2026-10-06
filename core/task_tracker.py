import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any

UTC = timezone.utc

STATUS_OPEN = "OPEN"
STATUS_PARTIAL = "PARTIAL_DONE"
STATUS_READY = "READY_REVIEW"
STATUS_CLOSED = "CLOSED"
VALID_STATUSES = {STATUS_OPEN, STATUS_PARTIAL, STATUS_READY, STATUS_CLOSED}


class TaskTrackerError(ValueError):
    pass


@dataclass
class TaskTrackerConfig:
    analyze_timeout_min: int = 15
    review_timeout_min: int = 20
    reminder_cooldown_min: int = 30


class TaskTracker:
    def __init__(self, db_path: str, config: TaskTrackerConfig | None = None):
        self.db_path = db_path
        self.config = config or TaskTrackerConfig()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        conn = self._conn()
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                task_type TEXT DEFAULT 'IC',
                status TEXT NOT NULL,
                required_actors_json TEXT,
                done_actors_json TEXT,
                created_by TEXT,
                decision_by TEXT,
                decision_text TEXT,
                due_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                closed_at TEXT,
                metadata_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                task_id TEXT NOT NULL,
                reminder_type TEXT NOT NULL,
                last_sent_at TEXT NOT NULL,
                PRIMARY KEY(task_id, reminder_type)
            )
            """
        )
        conn.commit()
        conn.close()

    def _now_iso(self) -> str:
        return datetime.now(UTC).isoformat()

    def _must_ic(self, task_id: str):
        if not task_id or not task_id.startswith("IC-"):
            raise TaskTrackerError("only IC-* task_id is supported in MVP")

    def _parse_json_list(self, raw: str | None) -> list[str]:
        if not raw:
            return []
        try:
            data = json.loads(raw)
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def _log_event(self, conn: sqlite3.Connection, task_id: str, event_type: str, actor: str | None = None, payload: dict[str, Any] | None = None):
        conn.execute(
            "INSERT INTO task_events (task_id, event_type, actor, payload_json, created_at) VALUES (?,?,?,?,?)",
            (task_id, event_type, actor, json.dumps(payload or {}, ensure_ascii=False), self._now_iso()),
        )

    def create_task(self, task_id: str, created_by: str | None = None, required_actors: list[str] | None = None, due_at: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        self._must_ic(task_id)
        required_actors = sorted(set(required_actors or []))
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row:
                return self._row_to_dict(row) | {"idempotent": True}

            now = self._now_iso()
            conn.execute(
                """
                INSERT INTO tasks (
                    task_id, task_type, status, required_actors_json, done_actors_json,
                    created_by, due_at, created_at, updated_at, metadata_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    task_id,
                    "IC",
                    STATUS_OPEN,
                    json.dumps(required_actors, ensure_ascii=False),
                    json.dumps([], ensure_ascii=False),
                    created_by,
                    due_at,
                    now,
                    now,
                    json.dumps(metadata or {}, ensure_ascii=False),
                ),
            )
            self._log_event(conn, task_id, "CREATE", created_by, {"required_actors": required_actors, "due_at": due_at})
            conn.commit()
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            return self._row_to_dict(row)
        finally:
            conn.close()

    def done_task(self, task_id: str, actor: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        self._must_ic(task_id)
        if not actor:
            raise TaskTrackerError("actor is required")
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if not row:
                raise TaskTrackerError("task not found")
            if row["status"] == STATUS_CLOSED:
                return self._row_to_dict(row) | {"idempotent": True, "note": "already closed"}

            done_actors = set(self._parse_json_list(row["done_actors_json"]))
            if actor in done_actors:
                return self._row_to_dict(row) | {"idempotent": True}

            done_actors.add(actor)
            required = set(self._parse_json_list(row["required_actors_json"]))
            new_status = STATUS_PARTIAL
            if required and required.issubset(done_actors):
                new_status = STATUS_READY

            now = self._now_iso()
            conn.execute(
                "UPDATE tasks SET done_actors_json=?, status=?, updated_at=? WHERE task_id=?",
                (json.dumps(sorted(done_actors), ensure_ascii=False), new_status, now, task_id),
            )
            self._log_event(conn, task_id, "DONE", actor, metadata or {})
            conn.commit()
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            return self._row_to_dict(row)
        finally:
            conn.close()

    def decide_task(self, task_id: str, actor: str, decision_text: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        self._must_ic(task_id)
        if not actor:
            raise TaskTrackerError("actor is required")
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if not row:
                raise TaskTrackerError("task not found")
            if row["status"] == STATUS_CLOSED:
                return self._row_to_dict(row) | {"idempotent": True}

            now = self._now_iso()
            conn.execute(
                "UPDATE tasks SET status=?, decision_by=?, decision_text=?, closed_at=?, updated_at=? WHERE task_id=?",
                (STATUS_CLOSED, actor, decision_text, now, now, task_id),
            )
            self._log_event(conn, task_id, "DECIDE", actor, {"decision_text": decision_text, **(metadata or {})})
            conn.commit()
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            return self._row_to_dict(row)
        finally:
            conn.close()

    def close_task(self, task_id: str, actor: str, reason: str | None = None) -> dict[str, Any]:
        self._must_ic(task_id)
        if not actor:
            raise TaskTrackerError("actor is required")
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if not row:
                raise TaskTrackerError("task not found")
            if row["status"] == STATUS_CLOSED:
                return self._row_to_dict(row) | {"idempotent": True}
            now = self._now_iso()
            conn.execute(
                "UPDATE tasks SET status=?, closed_at=?, updated_at=? WHERE task_id=?",
                (STATUS_CLOSED, now, now, task_id),
            )
            self._log_event(conn, task_id, "CLOSE", actor, {"reason": reason})
            conn.commit()
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            return self._row_to_dict(row)
        finally:
            conn.close()

    def list_active(self) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            rows = conn.execute("SELECT * FROM tasks WHERE status != ? ORDER BY created_at DESC", (STATUS_CLOSED,)).fetchall()
            return [self._row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def list_overdue(self, touch: bool = False) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        conn = self._conn()
        try:
            rows = conn.execute("SELECT * FROM tasks WHERE status != ? ORDER BY created_at ASC", (STATUS_CLOSED,)).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                status = r["status"]
                created_at = datetime.fromisoformat(r["created_at"])
                updated_at = datetime.fromisoformat(r["updated_at"])

                reminder_type = None
                trigger = None
                if status in {STATUS_OPEN, STATUS_PARTIAL} and now - created_at >= timedelta(minutes=self.config.analyze_timeout_min):
                    reminder_type = "analysis_overdue"
                    trigger = created_at
                elif status == STATUS_READY and now - updated_at >= timedelta(minutes=self.config.review_timeout_min):
                    reminder_type = "decision_overdue"
                    trigger = updated_at

                if not reminder_type:
                    continue

                rem = conn.execute(
                    "SELECT last_sent_at FROM reminders WHERE task_id=? AND reminder_type=?",
                    (r["task_id"], reminder_type),
                ).fetchone()
                if rem:
                    last = datetime.fromisoformat(rem["last_sent_at"])
                    if now - last < timedelta(minutes=self.config.reminder_cooldown_min):
                        continue

                item = self._row_to_dict(r)
                item["reminder_type"] = reminder_type
                item["overdue_minutes"] = int((now - trigger).total_seconds() // 60)
                out.append(item)

                if touch:
                    conn.execute(
                        "INSERT INTO reminders (task_id, reminder_type, last_sent_at) VALUES (?,?,?) ON CONFLICT(task_id, reminder_type) DO UPDATE SET last_sent_at=excluded.last_sent_at",
                        (r["task_id"], reminder_type, now_iso),
                    )
                    self._log_event(conn, r["task_id"], "REMIND", "system", {"reminder_type": reminder_type})

            if touch:
                conn.commit()
            return out
        finally:
            conn.close()

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        d["required_actors"] = self._parse_json_list(d.pop("required_actors_json", None))
        d["done_actors"] = self._parse_json_list(d.pop("done_actors_json", None))
        try:
            d["metadata"] = json.loads(d.pop("metadata_json", "{}") or "{}")
        except Exception:
            d["metadata"] = {}
        return d
