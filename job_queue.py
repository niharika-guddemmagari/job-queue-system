
from collections import deque
from threading import Lock


class JobQueue:
    def __init__(self):
        self.jobs = {}
        self.queue = deque()
        self.lock = Lock()

    def submit(self, job_id, description):
        with self.lock:
            if job_id in self.jobs:
                raise ValueError(f"Job {job_id} already exists")

            job = {
                "job_id": job_id,
                "description": description,
                "status": "QUEUED",
                "worker_id": None,
                "result": None,
                "error": None,
            }

            self.jobs[job_id] = job
            self.queue.append(job_id)
            return job.copy()

    def get_next_job(self):
        with self.lock:
            while self.queue:
                job_id = self.queue.popleft()
                job = self.jobs[job_id]

                if job["status"] == "QUEUED":
                    job["status"] = "RUNNING"
                    return job.copy()

            return None

    def update_status(
        self, job_id, status, worker_id=None, result=None, error=None
    ):
        allowed_statuses = {
            "QUEUED", "RUNNING", "COMPLETED", "FAILED"
        }

        if status not in allowed_statuses:
            raise ValueError(f"Invalid status: {status}")

        with self.lock:
            if job_id not in self.jobs:
                raise KeyError(f"Job {job_id} not found")

            job = self.jobs[job_id]
            job["status"] = status

            if worker_id is not None:
                job["worker_id"] = worker_id

            if result is not None:
                job["result"] = result

            if error is not None:
                job["error"] = error

            return job.copy()

    def get_job(self, job_id):
        with self.lock:
            job = self.jobs.get(job_id)
            return job.copy() if job else None

    def get_all_jobs(self):
        with self.lock:
            return [job.copy() for job in self.jobs.values()]

    def get_queue_size(self):
        with self.lock:
            return sum(
                1 for job_id in self.queue
                if self.jobs[job_id]["status"] == "QUEUED"
            )
