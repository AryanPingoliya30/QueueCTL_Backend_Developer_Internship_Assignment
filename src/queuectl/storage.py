from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from .db import Database, DEFAULT_CONFIG, get_database
from .utils import dump_json, to_iso, utcnow


@dataclass
class Job:
    id: str
    command: str
    state: str
    attempts: int
    max_retries: int
    created_at: str
    updated_at: str
    available_at: str
    priority: int = 0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    last_error: Optional[str] = None
    last_exit_code: Optional[int] = None
    output: Optional[str] = None
    metadata: Optional[str] = None


class Storage:
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_database()

    # Job operations -----------------------------------------------------
    def enqueue(self, payload: Dict) -> Job:
        now = utcnow()
        job_id = payload.get("id") or uuid.uuid4().hex
        command = payload.get("command")
        if not command:
            raise ValueError("Job payload must include a 'command'")

        max_retries = int(payload.get("max_retries") or self.get_config("max_retries"))
        priority = int(payload.get("priority", 0))
        available_at_iso = payload.get("available_at") or to_iso(now)
        metadata = payload.get("metadata")

        def _insert(conn):
            conn.execute(
                """
                INSERT INTO jobs (
                    id, command, state, attempts, max_retries, priority,
                    created_at, updated_at, available_at, metadata
                ) VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    command,
                    max_retries,
                    priority,
                    to_iso(now),
                    to_iso(now),
                    available_at_iso,
                    metadata if metadata is None or isinstance(metadata, str) else json.dumps(metadata),
                ),
            )

        try:
            self.db.transaction(_insert)
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"Job with id {job_id} already exists") from exc
        job = self.get_job(job_id)
        assert job is not None
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        def _fetch(conn):
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return Job(**row) if row else None

        return self.db.transaction(_fetch)

    def list_jobs(self, state: Optional[str] = None) -> List[Job]:
        def _list(conn):
            if state:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE state = ? ORDER BY updated_at DESC",
                    (state,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY updated_at DESC",
                ).fetchall()
            return [Job(**row) for row in rows]

        return self.db.transaction(_list)

    def list_dead_jobs(self) -> List[Job]:
        return self.list_jobs(state="dead")

    def job_summary(self) -> Dict[str, int]:
        def _summary(conn):
            rows = conn.execute(
                "SELECT state, COUNT(*) as count FROM jobs GROUP BY state"
            ).fetchall()
            summary = {row["state"]: row["count"] for row in rows}
            for state in ["pending", "processing", "completed", "failed", "dead"]:
                summary.setdefault(state, 0)
            return summary

        return self.db.transaction(_summary)

    def acquire_job(self) -> Optional[Job]:
        now_iso = to_iso(utcnow())

        def _acquire(conn):
            row = conn.execute(
                """
                SELECT id FROM jobs
                WHERE state IN ('pending', 'failed')
                  AND datetime(available_at) <= datetime(?)
                ORDER BY priority DESC, available_at ASC, created_at ASC
                LIMIT 1
                """,
                (now_iso,),
            ).fetchone()
            if row is None:
                return None
            job_id = row["id"]
            updated = conn.execute(
                """
                UPDATE jobs
                SET state = 'processing', attempts = attempts + 1,
                    started_at = ?, updated_at = ?
                WHERE id = ? AND state IN ('pending', 'failed')
                """,
                (now_iso, now_iso, job_id),
            )
            if updated.rowcount == 0:
                return None
            job_row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return Job(**job_row)

        return self.db.transaction(_acquire)

    def mark_completed(self, job_id: str, output: str) -> None:
        completed_at = to_iso(utcnow())

        def _complete(conn):
            conn.execute(
                """
                UPDATE jobs
                SET state = 'completed', updated_at = ?, completed_at = ?,
                    last_error = NULL, last_exit_code = 0, output = ?
                WHERE id = ?
                """,
                (completed_at, completed_at, output, job_id),
            )

        self.db.transaction(_complete)

    def mark_failed(
        self,
        job: Job,
        *,
        exit_code: int,
        error: str,
        backoff_base: int,
    ) -> None:
        now = utcnow()
        attempts = job.attempts
        next_delay = backoff_base ** attempts
        next_available = to_iso(now + timedelta(seconds=next_delay))
        updated = to_iso(now)

        def _update(conn):
            if attempts >= job.max_retries:
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = 'dead', updated_at = ?, last_error = ?,
                        last_exit_code = ?, available_at = ?, output = NULL
                    WHERE id = ?
                    """,
                    (updated, error, exit_code, next_available, job.id),
                )
            else:
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = 'failed', updated_at = ?, available_at = ?,
                        last_error = ?, last_exit_code = ?, output = NULL
                    WHERE id = ?
                    """,
                    (updated, next_available, error, exit_code, job.id),
                )

        self.db.transaction(_update)

    # Config -------------------------------------------------------------
    def get_config(self, key: str) -> str:
        def _get(conn):
            row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
            if row:
                return row["value"]
            return DEFAULT_CONFIG[key]

        return self.db.transaction(_get)

    def set_config(self, key: str, value: str) -> None:
        def _set(conn):
            conn.execute(
                "INSERT INTO config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

        self.db.transaction(_set)

    def list_config(self) -> Dict[str, str]:
        def _list(conn):
            rows = conn.execute("SELECT key, value FROM config").fetchall()
            cfg = {row["key"]: row["value"] for row in rows}
            for key, default in DEFAULT_CONFIG.items():
                cfg.setdefault(key, default)
            return cfg

        return self.db.transaction(_list)

    # Worker coordination -----------------------------------------------
    def register_worker(self, worker_id: str, pid: int) -> None:
        now_iso = to_iso(utcnow())

        def _register(conn):
            conn.execute(
                """
                INSERT INTO worker_heartbeats(worker_id, pid, state, started_at, last_heartbeat)
                VALUES(?, ?, 'running', ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    pid = excluded.pid,
                    state = 'running',
                    started_at = excluded.started_at,
                    last_heartbeat = excluded.last_heartbeat
                """,
                (worker_id, pid, now_iso, now_iso),
            )

        self.db.transaction(_register)

    def update_worker_state(self, worker_id: str, *, state: str, details: Optional[str] = None) -> None:
        now_iso = to_iso(utcnow())

        def _update(conn):
            conn.execute(
                """
                UPDATE worker_heartbeats
                SET state = ?, last_heartbeat = ?, details = ?
                WHERE worker_id = ?
                """,
                (state, now_iso, details, worker_id),
            )

        self.db.transaction(_update)

    def remove_worker(self, worker_id: str) -> None:
        def _remove(conn):
            conn.execute("DELETE FROM worker_heartbeats WHERE worker_id = ?", (worker_id,))

        self.db.transaction(_remove)

    def list_workers(self) -> List[dict]:
        def _list(conn):
            rows = conn.execute(
                "SELECT worker_id, pid, state, started_at, last_heartbeat, details FROM worker_heartbeats"
            ).fetchall()
            return [dict(row) for row in rows]

        return self.db.transaction(_list)

    def set_stop_requested(self, requested: bool) -> None:
        value = "1" if requested else "0"

        def _set(conn):
            conn.execute(
                "INSERT INTO worker_control(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                ("stop_requested", value),
            )

        self.db.transaction(_set)

    def clear_stop_requested(self) -> None:
        self.set_stop_requested(False)

    def stop_requested(self) -> bool:
        def _get(conn):
            row = conn.execute(
                "SELECT value FROM worker_control WHERE key = ?",
                ("stop_requested",),
            ).fetchone()
            return row and row["value"] == "1"

        return self.db.transaction(_get)

    def retry_dead_job(self, job_id: str) -> Job:
        now_iso = to_iso(utcnow())

        def _retry(conn):
            row = conn.execute(
                "SELECT * FROM jobs WHERE id = ? AND state = 'dead'",
                (job_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Job {job_id} is not in the dead letter queue")
            conn.execute(
                """
                UPDATE jobs SET
                    state = 'pending',
                    attempts = 0,
                    available_at = ?,
                    updated_at = ?,
                    last_error = NULL,
                    last_exit_code = NULL,
                    output = NULL
                WHERE id = ?
                """,
                (now_iso, now_iso, job_id),
            )
            refreshed = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return Job(**refreshed)

        return self.db.transaction(_retry)


__all__ = ["Job", "Storage"]

