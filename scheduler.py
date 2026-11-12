
"""
JACKFRUIT - Worker Scheduling Policies

FIFO:
Selects the worker that has been idle for the longest time.

Least-loaded:
Selects the worker with the fewest active jobs.
"""

import threading
import time


class Scheduler:

    POLICIES = {"fifo", "least_loaded"}

    def __init__(self, policy="fifo"):
        if policy not in self.POLICIES:
            raise ValueError(f"Unsupported policy: {policy}")

        self.policy = policy
        self.lock = threading.RLock()
        self.history = []

    def set_policy(self, policy):
        if policy not in self.POLICIES:
            raise ValueError(
                f"Policy must be one of {sorted(self.POLICIES)}"
            )

        with self.lock:
            self.policy = policy

    def get_policy(self):
        with self.lock:
            return self.policy

    def select_worker(self, workers):
        """
        Accepts either a list of worker dictionaries
        or a dictionary mapping worker IDs to worker dictionaries.
        """

        if isinstance(workers, dict):
            candidates = list(workers.values())
        else:
            candidates = list(workers)

        candidates = [
            worker for worker in candidates
            if worker.get("status") == "idle"
            and not worker.get("busy", False)
        ]

        if not candidates:
            return None

        with self.lock:
            policy = self.policy

        if policy == "fifo":
            # Longest-idle worker first.
            selected = min(
                candidates,
                key=lambda worker: (
                    worker.get("last_assigned", 0),
                    worker.get("worker_id", "")
                )
            )

        else:
            # Lowest active workload first.
            selected = min(
                candidates,
                key=lambda worker: (
                    worker.get("active_jobs", 0),
                    worker.get("last_assigned", 0),
                    worker.get("worker_id", "")
                )
            )

        with self.lock:
            self.history.append({
                "timestamp": time.time(),
                "policy": policy,
                "worker_id": selected.get("worker_id")
            })

        return selected

    def get_history(self):
        with self.lock:
            return list(self.history)

