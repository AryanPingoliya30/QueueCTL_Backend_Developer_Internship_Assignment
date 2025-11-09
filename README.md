# queuectl

CLI-based background job queue with persistent storage, retry/backoff handling, and dead letter queue management.

## 1. Setup

- **Prerequisites:** Python 3.10+
- Clone this repository and create a virtual environment:
  ```powershell
  python -m venv .venv
  .venv\Scripts\Activate.ps1
  ```
- Install dependencies in editable mode (provides the `queuectl` command):
  ```powershell
  pip install -e .
  ```
- (Optional) Verify installation:
  ```powershell
  queuectl version
  ```

## 2. Usage Highlights

- Enqueue a job (no JSON fuss):
  ```powershell
  queuectl enqueue --id hello --command "cmd /c echo Hello Queue"
  ```
- Or enqueue via raw JSON / file:
  ```powershell
  '{"id":"job-004","command":"cmd /c echo Hello"}' | queuectl enqueue
  ```
- Or enqueue via raw JSON / file:
- Create job.json in main directory 
  ```powershell
      Set-Content job.json '{
      "id": "job-005",
      "command": "cmd /c echo Hello from file"
    }'
  ```
- Enqueue
  ```powershell
      queuectl enqueue --file job.json
  ```
- Start three detached workers:
  ```powershell
  queuectl worker start --count 3
  ```
- Stop workers gracefully (finishes in-flight jobs before exit):
  ```powershell
  queuectl worker stop
  ```
- Inspect queue and worker state:
  ```powershell
  queuectl status
  queuectl list --state failed
  ```
- Retry a job from the Dead Letter Queue:
  ```powershell
  queuectl dlq list
  queuectl dlq retry job-id-here
  ```
- Tune retry/backoff behaviour:
  ```powershell
  queuectl config list
  queuectl config set max_retries 5
  queuectl config set backoff_base 3
  ```
- delete all jobs
  ```powershell
    Remove-Item .\queuectl.db
  ```
- Testing Commands (All In One):
  ```powershell
  queuectl version
  queuectl enqueue --id job-001 --command "cmd /c echo Hello Queue"
  queuectl enqueue --id job-002 --command "cmd /c exit 1" --max-retries 3
  queuectl enqueue --id job-003 --command "cmd /c echo High Priority" --priority 10
  queuectl list
  queuectl list --state pending
  queuectl status
  queuectl worker start
  queuectl worker start --count 2
  queuectl worker stop
  queuectl worker stop --timeout 5
  queuectl dlq list
  queuectl dlq retry <job_id>
  queuectl config list
  queuectl config get retry_delay
  queuectl config set retry_delay 5

> 📹 **Demo:** Record a short CLI walkthrough (screen capture) and place the shareable link here: `<ADD_LINK>`

## 3. Architecture Overview

- **Storage:** SQLite (`queuectl.db`) stores jobs, configuration, worker heartbeats, and control flags. WAL mode ensures durability across restarts and safe multi-process access.
- **Job lifecycle:**
  1. Jobs enter with `state=pending` (or scheduled via `available_at`).
  2. Workers atomically transition jobs to `processing`, incrementing attempts.
  3. Successful runs store output and mark `completed`.
  4. Failures apply exponential backoff (`next_delay = base ** attempts`). Re-triable jobs move to `failed` with a future `available_at`; exhausted jobs end in `dead` (DLQ).
- **Worker processes:**
  - Spawned via `queuectl worker start`; background processes execute commands in new shells.
  - Each worker maintains heartbeats (`worker_heartbeats` table), exposing PID, state, and last activity.
  - Shutdown is coordinated through a control flag (`stop_requested`) enabling graceful completion before exit; stubborn workers are SIGTERM'ed as a fallback.
- **Configuration:** Stored centrally in the `config` table with sensible defaults (max retries, backoff base, poll interval, command timeout). CLI affords dynamic updates without restart.
- **Concurrency guardrails:**
  - SQLite transactions guarantee that only one worker acquires a given job (`UPDATE ... WHERE state IN ('pending','failed')`).
  - Worker status updates double as lightweight heartbeats for monitoring and cleanup.

## 4. Assumptions & Trade-offs

- **Shell execution:** Jobs run via the system shell (`subprocess.run(shell=True)`). Commands must be self-contained; advanced environments should wrap scripts themselves.
- **Scheduling granularity:** Exponential backoff uses whole seconds; no priority queues or cron-style scheduling yet (see Bonus ideas).
- **Timeout handling:** Configurable global command timeout (`config set command_timeout <seconds>`). Per-job overrides are left as future work.
- **Worker registry:** PIDs are persisted in SQLite only. If the host crashes, stale rows are pruned when `worker stop` runs or new workers overwrite entries.
- **Logging:** Captured stdout is stored on success; failures keep the last error string. Full log streaming/rotation is intentionally out of scope for the internship timebox.

## 5. Testing & Verification

- Run the demo script that exercises success, retry, DLQ, and retry-from-DLQ flows:
  ```powershell
  python tests\demo.py
  ```
  The script resets `queuectl.db`, enqueues two jobs, starts workers, inspects status, and demonstrates DLQ retry.
- Manual smoke-tests:
  - `queuectl enqueue` simple echo jobs
  - `queuectl worker start --foreground` to observe logs in the foreground
  - `queuectl dlq list` / `retry`


---

Questions or feedback? Open an issue or reach out — happy to iterate! :)



