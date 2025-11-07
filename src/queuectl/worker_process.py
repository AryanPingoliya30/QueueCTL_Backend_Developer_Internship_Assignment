from __future__ import annotations

import argparse

from .worker import run_worker


def main() -> None:
    parser = argparse.ArgumentParser(description="queuectl worker process")
    parser.add_argument("--worker-id", dest="worker_id", help="Identifier for this worker", default=None)
    args = parser.parse_args()
    run_worker(worker_id=args.worker_id)


if __name__ == "__main__":
    main()

