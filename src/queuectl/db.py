from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import Lock
from typing import Callable, Optional, TypeVar

from .utils import to_iso, utcnow


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    last_error TEXT,
    last_exit_code INTEGER,
    output TEXT,
    metadata TEXT
);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    state TEXT NOT NULL,
    started_at TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    details TEXT
);

CREATE TABLE IF NOT EXISTS worker_control (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


DEFAULT_CONFIG = {
    "max_retries": "3",
    "backoff_base": "2",
    "poll_interval": "2",
    "heartbeat_interval": "5",
    "command_timeout": "0",
}


T = TypeVar("T")


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = Lock()
        self._initialized = False

    def init(self) -> None:
        with self._lock:
            if self._initialized:
                return
            conn = self._connect()
            try:
                conn.executescript(SCHEMA)
                self._ensure_defaults(conn)
                conn.commit()
            finally:
                conn.close()
            self._initialized = True

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def connection(self) -> sqlite3.Connection:
        self.init()
        return self._connect()

    def transaction(self, func: Callable[[sqlite3.Connection], "T"]) -> "T":
        conn = self.connection()
        try:
            result = func(conn)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_defaults(self, conn: sqlite3.Connection) -> None:
        now = to_iso(utcnow())
        for key, value in DEFAULT_CONFIG.items():
            conn.execute(
                "INSERT OR IGNORE INTO config(key, value) VALUES(?, ?)",
                (key, value),
            )
        conn.execute(
            "INSERT OR IGNORE INTO worker_control(key, value) VALUES(?, ?)",
            ("stop_requested", "0"),
        )


def get_database(db_path: Optional[Path] = None) -> Database:
    if db_path is None:
        db_path = Path("queuectl.db")
    db = Database(db_path)
    db.init()
    return db


__all__ = ["Database", "get_database", "DEFAULT_CONFIG"]

