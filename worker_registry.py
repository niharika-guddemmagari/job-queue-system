
from threading import Lock


class WorkerRegistry:
    def __init__(self):
        self.workers = {}
        self.lock = Lock()

    def register_worker(self, worker_id, host, port):
        with self.lock:
            if worker_id in self.workers:
                raise ValueError(
                    f"Worker {worker_id} is already registered"
                )

            self.workers[worker_id] = {
                "worker_id": worker_id,
                "host": host,
                "port": port,
                "status": "AVAILABLE",
                "current_load": 0,
            }

            return self.workers[worker_id].copy()

    def get_worker(self, worker_id):
        with self.lock:
            worker = self.workers.get(worker_id)
            return worker.copy() if worker else None

    def get_available_workers(self):
        with self.lock:
            return [
                worker.copy()
                for worker in self.workers.values()
                if worker["status"] == "AVAILABLE"
            ]

    def update_status(self, worker_id, status):
        allowed_statuses = {
            "AVAILABLE", "BUSY", "OFFLINE"
        }

        if status not in allowed_statuses:
            raise ValueError(f"Invalid worker status: {status}")

        with self.lock:
            if worker_id not in self.workers:
                raise KeyError(
                    f"Worker {worker_id} is not registered"
                )

            self.workers[worker_id]["status"] = status
            return self.workers[worker_id].copy()

    def change_load(self, worker_id, amount):
        with self.lock:
            if worker_id not in self.workers:
                raise KeyError(
                    f"Worker {worker_id} is not registered"
                )

            worker = self.workers[worker_id]
            new_load = worker["current_load"] + amount

            if new_load < 0:
                raise ValueError("Worker load cannot be negative")

            worker["current_load"] = new_load

            if worker["status"] != "OFFLINE":
                worker["status"] = (
                    "BUSY" if new_load > 0 else "AVAILABLE"
                )

            return worker.copy()

    def get_all_workers(self):
        with self.lock:
            return [
                worker.copy()
                for worker in self.workers.values()
            ]
