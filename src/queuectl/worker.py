from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from .storage import Job, Storage
from .utils import CommandResult, execute_with_timing


@dataclass
class WorkerConfig:
    poll_interval: float
    backoff_base: int
    command_timeout: Optional[int]


class WorkerRunner:
    def __init__(
        self,
        storage: Storage,
        worker_id: Optional[str] = None,
        config: Optional[WorkerConfig] = None,
    ):
        self.storage = storage
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        if config is None:
            poll_interval = float(self.storage.get_config("poll_interval"))
            backoff_base = int(self.storage.get_config("backoff_base"))
            timeout_val = int(self.storage.get_config("command_timeout"))
            config = WorkerConfig(
                poll_interval=poll_interval,
                backoff_base=backoff_base,
                command_timeout=timeout_val if timeout_val > 0 else None,
            )
        self.config = config
        self._should_stop = False

    def run(self) -> None:
        pid = os.getpid()
        self.storage.register_worker(self.worker_id, pid)
        try:
            self._loop()
        finally:
            self.storage.update_worker_state(self.worker_id, state="exited", details=None)
            self.storage.remove_worker(self.worker_id)

    def _loop(self) -> None:
        while True:
            if self._should_stop or self.storage.stop_requested():
                break

            job = self.storage.acquire_job()
            if job is None:
                self.storage.update_worker_state(self.worker_id, state="idle", details=None)
                time.sleep(self.config.poll_interval)
                continue

            self.storage.update_worker_state(
                self.worker_id,
                state="processing",
                details=f"job={job.id} attempts={job.attempts}/{job.max_retries}",
            )
            result = self._execute(job)
            if result.exit_code == 0:
                self.storage.mark_completed(job.id, result.stdout)
            else:
                error_summary = result.stderr.strip() or result.stdout.strip() or "command failed"
                self.storage.mark_failed(
                    job,
                    exit_code=result.exit_code,
                    error=error_summary[:5000],
                    backoff_base=self.config.backoff_base,
                )

            self.storage.update_worker_state(self.worker_id, state="running", details=None)

        self.storage.update_worker_state(self.worker_id, state="stopped", details="stop requested")

    def _execute(self, job: Job) -> CommandResult:
        return execute_with_timing(job.command, timeout=self.config.command_timeout)


def run_worker(worker_id: Optional[str] = None) -> None:
    storage = Storage()
    runner = WorkerRunner(storage=storage, worker_id=worker_id)
    runner.run()


__all__ = ["WorkerRunner", "WorkerConfig", "run_worker"]

