
"""
JACKFRUIT - Thread-Safe Distributed Job Queue
FIFO queue with timing, pause/resume and statistics.
"""

import threading
import time
from collections import deque


class JobQueue:

    def __init__(self):
        self.items = deque()
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.paused = False
        self.total_enqueued = 0
        self.total_dequeued = 0
        self.total_wait_ms = 0.0

    def enqueue(self, job):
        """Add a job to the end of the FIFO queue."""

        with self.condition:
            job_id = job["job_id"]

            if any(item["job_id"] == job_id for item in self.items):
                return False

            job["_queue_entered_at"] = time.perf_counter()
            self.items.append(job)
            self.total_enqueued += 1

            self.condition.notify_all()
            return True

    def dequeue(self, timeout=None):
        """
        Remove the oldest job.
        Waits while the queue is empty or paused.
        """

        with self.condition:
            available = self.condition.wait_for(
                lambda: self.paused is False and bool(self.items),
                timeout=timeout
            )

            if not available:
                return None

            job = self.items.popleft()

            wait_ms = (
                time.perf_counter() - job["_queue_entered_at"]
            ) * 1000

            job["queue_wait_ms"] = round(wait_ms, 3)

            self.total_dequeued += 1
            self.total_wait_ms += wait_ms

            return job

    def peek(self):
        """Return the next job without removing it."""

        with self.lock:
            return self.items[0] if self.items else None

    def get_position(self, job_id):
        """Return a job's current 1-based queue position."""

        with self.lock:
            for index, job in enumerate(self.items):
                if job["job_id"] == job_id:
                    return index + 1

            return None

    def remove(self, job_id):
        """Remove a queued job by ID."""

        with self.condition:
            for job in self.items:
                if job["job_id"] == job_id:
                    self.items.remove(job)
                    self.condition.notify_all()
                    return True

            return False

    def pause(self):
        with self.condition:
            self.paused = True
            self.condition.notify_all()

    def resume(self):
        with self.condition:
            self.paused = False
            self.condition.notify_all()

    def is_paused(self):
        with self.lock:
            return self.paused

    def get_items(self):
        """Return a snapshot of the current queue."""

        with self.lock:
            return list(self.items)

    def get_stats(self):
        with self.lock:
            average_wait = (
                self.total_wait_ms / self.total_dequeued
                if self.total_dequeued else 0
            )

            return {
                "queue_depth": len(self.items),
                "total_enqueued": self.total_enqueued,
                "total_dequeued": self.total_dequeued,
                "paused": self.paused,
                "average_wait_ms": round(average_wait, 3)
            }

    def clear(self):
        """Clear pending jobs. Does not affect running jobs."""

        with self.condition:
            count = len(self.items)
            self.items.clear()
            self.condition.notify_all()
            return count

    def __len__(self):
        with self.lock:
            return len(self.items)

