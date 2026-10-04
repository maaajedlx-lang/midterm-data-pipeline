from __future__ import annotations

import datetime
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase2.jobs import JOBS_REGISTRY, run_job_by_name


class BackgroundJobScheduler:
    """
    Lightweight, thread-safe in-process periodic job scheduler.
    Requires zero external C-dependencies, highly stable, and runs as a background daemon.
    """

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._job_metadata: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._init_metadata()

    def _init_metadata(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        for name, meta in JOBS_REGISTRY.items():
            interval = meta["interval_seconds"]
            self._job_metadata[name] = {
                "name": name,
                "schedule": meta["schedule"],
                "interval_seconds": interval,
                "description": meta["description"],
                "last_run": None,
                "next_run": (now + datetime.timedelta(seconds=interval)).isoformat(),
                "last_status": "NEVER_RUN",
                "run_count": 0,
            }

    def _scheduler_loop(self) -> None:
        while not self._stop_event.is_set():
            now = datetime.datetime.now(datetime.timezone.utc)
            jobs_to_run = []

            with self._lock:
                for name, meta in self._job_metadata.items():
                    next_run_str = meta.get("next_run")
                    if next_run_str:
                        next_dt = datetime.datetime.fromisoformat(next_run_str)
                        if now >= next_dt:
                            jobs_to_run.append(name)

            for job_name in jobs_to_run:
                try:
                    result = run_job_by_name(job_name, trigger_type="SCHEDULED")
                    with self._lock:
                        meta = self._job_metadata[job_name]
                        meta["last_run"] = result["end_time"]
                        meta["last_status"] = result["status"]
                        meta["run_count"] += 1
                        meta["next_run"] = (
                            datetime.datetime.now(datetime.timezone.utc)
                            + datetime.timedelta(seconds=meta["interval_seconds"])
                        ).isoformat()
                except Exception as exc:
                    with self._lock:
                        meta = self._job_metadata[job_name]
                        meta["last_status"] = f"ERROR: {str(exc)}"

            # Sleep in small slices to respond quickly to stop event
            self._stop_event.wait(timeout=5.0)

    def start(self) -> None:
        """Starts the scheduler background daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._scheduler_loop, daemon=True, name="JobSchedulerThread")
        self._thread.start()

    def stop(self) -> None:
        """Gracefully signals the scheduler thread to terminate."""
        if self._thread is not None:
            self._stop_event.set()
            self._thread.join(timeout=3.0)
            self._thread = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def get_status(self) -> List[Dict[str, Any]]:
        """Returns live status of all registered jobs in the scheduler."""
        with self._lock:
            return list(self._job_metadata.values())

    def trigger_now(self, job_name: str) -> Dict[str, Any]:
        """Manually runs a job immediately and updates in-memory scheduler metadata."""
        result = run_job_by_name(job_name, trigger_type="MANUAL")
        with self._lock:
            if job_name in self._job_metadata:
                meta = self._job_metadata[job_name]
                meta["last_run"] = result["end_time"]
                meta["last_status"] = result["status"]
                meta["run_count"] += 1
                meta["next_run"] = (
                    datetime.datetime.now(datetime.timezone.utc)
                    + datetime.timedelta(seconds=meta["interval_seconds"])
                ).isoformat()
        return result


# Global Singleton Scheduler Instance
GLOBAL_SCHEDULER = BackgroundJobScheduler()


if __name__ == "__main__":
    print("Testing BackgroundJobScheduler...")
    sched = BackgroundJobScheduler()
    sched.start()
    print("Scheduler started. Running:", sched.is_running())
    status = sched.get_status()
    for s in status:
        print(f" - [{s['name']}] Next Run: {s['next_run']}, Interval: {s['interval_seconds']}s")
    
    print("\nTriggering job 1 manually via scheduler...")
    res = sched.trigger_now("refresh_materialized_views_job")
    print("Manual trigger result status:", res["status"])
    
    sched.stop()
    print("Scheduler stopped cleanly.")
