
class Scheduler:
    def __init__(self, worker_registry):
        self.worker_registry = worker_registry

    def select_worker(self):
        workers = self.worker_registry.get_available_workers()

        if not workers:
            return None

        # Choose the available worker with the lowest load.
        selected_worker = min(
            workers,
            key=lambda worker: (
                worker["current_load"],
                worker["worker_id"],
            ),
        )

        return selected_worker

    def assign_job(self, job):
        worker = self.select_worker()

        if worker is None:
            return None

        # Reserve one unit of worker capacity for this job.
        self.worker_registry.change_load(
            worker["worker_id"], 1
        )

        return {
            "job_id": job["job_id"],
            "worker_id": worker["worker_id"],
            "host": worker["host"],
            "port": worker["port"],
        }

    def release_worker(self, worker_id):
        worker = self.worker_registry.get_worker(worker_id)

        if worker is None:
            raise KeyError(f"Worker {worker_id} not found")

        if worker["current_load"] > 0:
            return self.worker_registry.change_load(
                worker_id, -1
            )

        return worker
