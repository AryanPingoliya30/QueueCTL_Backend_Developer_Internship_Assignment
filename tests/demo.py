"""Demonstration script covering the primary queuectl flows."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "queuectl.db"


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, "-m", "queuectl", *args]
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, text=True)


def main() -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()

    run_cli("config", "set", "max_retries", "2")
    run_cli("config", "set", "backoff_base", "2")

    python_cmd = f'"{sys.executable}"'
    success_job = {
        "id": "demo-success",
        "command": f"{python_cmd} -c \"print('demo success')\"",
        "max_retries": 2,
    }
    fail_job = {
        "id": "demo-fail",
        "command": f"{python_cmd} -c \"import sys; sys.exit(1)\"",
        "max_retries": 2,
    }

    run_cli("enqueue", json.dumps(success_job))
    run_cli("enqueue", json.dumps(fail_job))

    run_cli("worker", "start", "--count", "1")

    time.sleep(8)
    run_cli("status")

    run_cli("worker", "stop")

    time.sleep(1)
    run_cli("dlq", "list")

    run_cli("dlq", "retry", "demo-fail")
    run_cli("worker", "start", "--count", "1")
    time.sleep(6)
    run_cli("worker", "stop")
    run_cli("status")


if __name__ == "__main__":
    main()

