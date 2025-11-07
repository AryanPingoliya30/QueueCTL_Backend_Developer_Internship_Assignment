from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .config import ConfigService
from .storage import Storage
from .worker import run_worker
from .utils import terminate_process


console = Console()

app = typer.Typer(help="queuectl - CLI background job queue system", add_completion=False)
worker_app = typer.Typer(help="Manage worker processes", add_completion=False)
dlq_app = typer.Typer(help="Dead letter queue operations", add_completion=False)
config_app = typer.Typer(help="Configure queuectl runtime options", add_completion=False)

app.add_typer(worker_app, name="worker")
app.add_typer(dlq_app, name="dlq")
app.add_typer(config_app, name="config")


def _load_payload(payload: Optional[str], file: Optional[Path]) -> dict:
    if file:
        if payload:
            raise typer.BadParameter("Provide either payload JSON or --file, not both")
        with file.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    if payload:
        return json.loads(payload)
    if not sys.stdin.isatty():
        try:
            data = json.load(sys.stdin)
            return data
        except json.JSONDecodeError as exc:  # pragma: no cover
            raise typer.BadParameter("Provided JSON payload via stdin is invalid") from exc
    raise typer.BadParameter("No job payload provided. Use JSON, --file, or --command option.")


def _build_job_from_options(
    *,
    job_id: Optional[str],
    command: Optional[str],
    max_retries: Optional[int],
    priority: Optional[int],
    available_at: Optional[str],
    metadata: Optional[str],
) -> Optional[dict]:
    if not command:
        return None

    payload: dict[str, object] = {"command": command}
    if job_id:
        payload["id"] = job_id
    if max_retries is not None:
        payload["max_retries"] = max_retries
    if priority is not None:
        payload["priority"] = priority
    if available_at is not None:
        payload["available_at"] = available_at
    if metadata is not None:
        try:
            payload["metadata"] = json.loads(metadata)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter("--metadata must be valid JSON") from exc
    return payload


@app.command()
def version() -> None:
    """Show the queuectl version."""

    console.print(f"queuectl version {__version__}")


@app.command()
def enqueue(
    payload: Optional[str] = typer.Argument(
        None,
        help="Job payload as JSON string. If omitted, use --file or pipe JSON via stdin.",
    ),
    file: Optional[Path] = typer.Option(None, "--file", "-f", help="Path to JSON file with job payload"),
    job_id: Optional[str] = typer.Option(None, "--id", help="Job identifier (use with --command)"),
    command: Optional[str] = typer.Option(None, "--command", help="Shell command to execute"),
    max_retries: Optional[int] = typer.Option(None, "--max-retries", help="Max retries (with --command)"),
    priority: Optional[int] = typer.Option(None, "--priority", help="Higher numbers run first"),
    available_at: Optional[str] = typer.Option(
        None,
        "--available-at",
        help="ISO timestamp when the job becomes available",
    ),
    metadata: Optional[str] = typer.Option(
        None,
        "--metadata",
        help="Arbitrary JSON metadata (use with --command)",
    ),
) -> None:
    """Enqueue a new job for processing."""

    storage = Storage()
    job_data = _build_job_from_options(
        job_id=job_id,
        command=command,
        max_retries=max_retries,
        priority=priority,
        available_at=available_at,
        metadata=metadata,
    )
    if job_data is None:
        job_data = _load_payload(payload, file)
    elif payload or file:
        raise typer.BadParameter("Use either --command based options or JSON payload, not both")
    job = storage.enqueue(job_data)
    console.print(f"Enqueued job [bold]{job.id}[/bold] -> state={job.state}")


@app.command("list")
def list_jobs(
    state: Optional[str] = typer.Option(None, "--state", "-s", help="Filter by job state"),
) -> None:
    """List jobs filtered by state."""

    storage = Storage()
    jobs = storage.list_jobs(state=state)
    if not jobs:
        console.print("No jobs found")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("ID", overflow="fold")
    table.add_column("State")
    table.add_column("Attempts")
    table.add_column("Max")
    table.add_column("Command", overflow="fold")
    table.add_column("Available At")
    table.add_column("Updated")
    for job in jobs:
        table.add_row(
            job.id,
            job.state,
            str(job.attempts),
            str(job.max_retries),
            job.command,
            job.available_at,
            job.updated_at,
        )
    console.print(table)


@app.command()
def status() -> None:
    """Display queue summary and worker statuses."""

    storage = Storage()
    summary = storage.job_summary()
    workers = storage.list_workers()

    summary_table = Table(show_header=True, header_style="bold")
    summary_table.add_column("State")
    summary_table.add_column("Count")
    for state, count in summary.items():
        summary_table.add_row(state, str(count))
    console.print("[bold]Job Summary[/bold]")
    console.print(summary_table)

    worker_table = Table(show_header=True, header_style="bold")
    worker_table.add_column("Worker ID")
    worker_table.add_column("PID")
    worker_table.add_column("State")
    worker_table.add_column("Started")
    worker_table.add_column("Last Heartbeat")
    worker_table.add_column("Details")
    for worker in workers:
        worker_table.add_row(
            worker["worker_id"],
            str(worker["pid"]),
            worker["state"],
            worker["started_at"],
            worker["last_heartbeat"],
            worker.get("details") or "",
        )
    console.print("\n[bold]Active Workers[/bold]")
    if workers:
        console.print(worker_table)
    else:
        console.print("No active workers")


@worker_app.command("start")
def worker_start(
    count: int = typer.Option(1, "--count", "-c", min=1, help="Number of workers to start"),
    foreground: bool = typer.Option(False, "--foreground", help="Run a single worker in the foreground"),
) -> None:
    """Start worker processes to handle background jobs."""

    storage = Storage()
    storage.clear_stop_requested()

    if foreground and count != 1:
        raise typer.BadParameter("Foreground mode supports only a single worker")

    if foreground:
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        console.print(f"Starting foreground worker {worker_id}")
        run_worker(worker_id=worker_id)
        return

    worker_ids = []
    for _ in range(count):
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        worker_ids.append(worker_id)
        cmd = [sys.executable, "-m", "queuectl.worker_process", "--worker-id", worker_id]
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    console.print(
        f"Started {count} worker(s): " + ", ".join(worker_ids)
    )


@worker_app.command("stop")
def worker_stop(
    timeout: int = typer.Option(30, help="Seconds to wait for graceful shutdown"),
) -> None:
    """Request workers to stop gracefully."""

    storage = Storage()
    storage.set_stop_requested(True)
    console.print("Stop requested. Waiting for workers to exit...")

    deadline = time.time() + timeout
    while time.time() < deadline:
        workers = storage.list_workers()
        if not workers:
            console.print("All workers stopped gracefully")
            storage.clear_stop_requested()
            return
        time.sleep(1)

    workers = storage.list_workers()
    if not workers:
        console.print("All workers stopped.")
        storage.clear_stop_requested()
        return

    console.print("Timeout reached. Forcing remaining workers to exit...")
    for worker in workers:
        pid = worker["pid"]
        terminate_process(pid)
        storage.remove_worker(worker["worker_id"])
    storage.clear_stop_requested()
    console.print("Forced termination issued to remaining workers")


@dlq_app.command("list")
def dlq_list() -> None:
    """List jobs currently in the dead letter queue."""

    storage = Storage()
    jobs = storage.list_dead_jobs()
    if not jobs:
        console.print("Dead letter queue is empty")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("ID", overflow="fold")
    table.add_column("Attempts")
    table.add_column("Command", overflow="fold")
    table.add_column("Last Error", overflow="fold")
    table.add_column("Updated")
    for job in jobs:
        table.add_row(
            job.id,
            str(job.attempts),
            job.command,
            (job.last_error or "")[:120],
            job.updated_at,
        )
    console.print(table)


@dlq_app.command("retry")
def dlq_retry(job_id: str = typer.Argument(..., help="ID of the job to retry")) -> None:
    """Move a job from the DLQ back to the pending queue."""

    storage = Storage()
    try:
        job = storage.retry_dead_job(job_id)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)
    console.print(f"Requeued job [bold]{job.id}[/bold] for retry")


@config_app.command("list")
def config_list() -> None:
    """List current configuration values."""

    service = ConfigService()
    config_values = service.list()
    table = Table(show_header=True, header_style="bold")
    table.add_column("Key")
    table.add_column("Value")
    for key, value in sorted(config_values.items()):
        table.add_row(key, value)
    console.print(table)


@config_app.command("get")
def config_get(key: str = typer.Argument(..., help="Configuration key")) -> None:
    """Retrieve a configuration value."""

    service = ConfigService()
    value = service.get(key)
    console.print(f"{key} = {value}")


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Configuration key to set"),
    value: str = typer.Argument(..., help="New value"),
) -> None:
    """Update a configuration value."""

    service = ConfigService()
    service.set(key, value)
    console.print(f"Updated {key} = {value}")


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":  # pragma: no cover
    main()

