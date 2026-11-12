
"""
JACKFRUIT - Worker Registry
Tracks worker registration, heartbeat health and workload.
"""

import threading
import time


class WorkerRegistry:

    def __init__(self, heartbeat_timeout=8):
        self.workers = {}
        self.lock = threading.RLock()
        self.heartbeat_timeout = heartbeat_timeout

    def register(self, worker_id, host, port, pid=None):
        now = time.time()

        with self.lock:
            existing = self.workers.get(worker_id)

            self.workers[worker_id] = {
                "worker_id": worker_id,
                "host": host,
                "port": int(port),
                "pid": pid,
                "status": "idle",
                "busy": False,
                "active_jobs": 0,
                "jobs_completed": (
                    existing["jobs_completed"] if existing else 0
                ),
                "jobs_failed": (
                    existing["jobs_failed"] if existing else 0
                ),
                "registered_at": (
                    existing["registered_at"] if existing
                    else now
                ),
                "last_heartbeat": now,
                "last_assigned": 0,
                "current_job": None
            }

            return self._copy(self.workers[worker_id])

    def heartbeat(self, worker_id, busy=False, active_job=None,
                  jobs_completed=None, jobs_failed=None):

        with self.lock:
            worker = self.workers.get(worker_id)

            if worker is None:
                return False

            worker["last_heartbeat"] = time.time()
            worker["busy"] = bool(busy)
            worker["status"] = "busy" if busy else "idle"
            worker["current_job"] = active_job

            if jobs_completed is not None:
                worker["jobs_completed"] = int(jobs_completed)

            if jobs_failed is not None:
                worker["jobs_failed"] = int(jobs_failed)

            return True

    def get(self, worker_id):
        with self.lock:
            worker = self.workers.get(worker_id)
            return self._copy(worker) if worker else None

    def get_all(self):
        with self.lock:
            return {
                worker_id: self._copy(worker)
                for worker_id, worker in self.workers.items()
            }

    def get_available(self):
        with self.lock:
            return [
                self._copy(worker)
                for worker in self.workers.values()
                if worker["status"] == "idle"
                and not worker["busy"]
            ]

    def assign_job(self, worker_id, job_id):
        with self.lock:
            worker = self.workers.get(worker_id)

            if not worker or worker["status"] != "idle":
                return False

            worker["busy"] = True
            worker["status"] = "busy"
            worker["active_jobs"] += 1
            worker["current_job"] = job_id
            worker["last_assigned"] = time.time()

            return True

    def finish_job(self, worker_id, success=True):
        with self.lock:
            worker = self.workers.get(worker_id)

            if not worker:
                return False

            worker["active_jobs"] = max(
                0, worker["active_jobs"] - 1
            )

            if success:
                worker["jobs_completed"] += 1
            else:
                worker["jobs_failed"] += 1

            worker["busy"] = worker["active_jobs"] > 0
            worker["status"] = (
                "busy" if worker["busy"] else "idle"
            )
            worker["current_job"] = None

            return True

    def mark_offline(self, worker_id):
        with self.lock:
            worker = self.workers.get(worker_id)

            if not worker:
                return False

            worker["status"] = "offline"
            worker["busy"] = False
            worker["active_jobs"] = 0
            worker["current_job"] = None

            return True

    def find_expired(self):
        now = time.time()

        with self.lock:
            return [
                worker_id
                for worker_id, worker in self.workers.items()
                if worker["status"] != "offline"
                and now - worker["last_heartbeat"]
                > self.heartbeat_timeout
            ]

    def mark_all_expired_offline(self):
        expired = self.find_expired()

        for worker_id in expired:
            self.mark_offline(worker_id)

        return expired

    def get_stats(self):
        with self.lock:
            workers = list(self.workers.values())

            return {
                "total_workers": len(workers),
                "online_workers": sum(
                    w["status"] != "offline" for w in workers
                ),
                "busy_workers": sum(
                    w["status"] == "busy" for w in workers
                ),
                "idle_workers": sum(
                    w["status"] == "idle" for w in workers
                ),
                "completed_jobs": sum(
                    w["jobs_completed"] for w in workers
                ),
                "failed_jobs": sum(
                    w["jobs_failed"] for w in workers
                )
            }

    @staticmethod
    def _copy(worker):
        return dict(worker) if worker else None

