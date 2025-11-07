from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(ISO_FORMAT)


def from_iso(value: str) -> datetime:
    return datetime.strptime(value, ISO_FORMAT).replace(tzinfo=timezone.utc)


def load_json(value: str) -> Any:
    return json.loads(value)


def dump_json(data: Any, *, indent: Optional[int] = None) -> str:
    return json.dumps(data, indent=indent, ensure_ascii=False)


def run_command(command: str, timeout: Optional[int] = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def ensure_directory(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def terminate_process(pid: int) -> None:
    try:
        if sys.platform == "win32":
            os.kill(pid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return


@dataclass
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str
    duration: float


def execute_with_timing(command: str, timeout: Optional[int] = None) -> CommandResult:
    start = time.perf_counter()
    try:
        proc = run_command(command, timeout=timeout)
        exit_code = proc.returncode
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
    except subprocess.TimeoutExpired as exc:
        exit_code = -1
        stdout = exc.stdout or ""
        stderr = (exc.stderr or "") + "\n[queuectl] command timed out"
    end = time.perf_counter()
    return CommandResult(exit_code=exit_code, stdout=stdout, stderr=stderr, duration=end - start)

