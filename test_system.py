
from job_queue import JobQueue
from worker_registry import WorkerRegistry
from scheduler import Scheduler
from worker_simulator import WorkerSimulator


def main():
    job_queue = JobQueue()
    registry = WorkerRegistry()
    scheduler = Scheduler(registry)

    # Register two workers.
    registry.register_worker("worker-1", "127.0.0.1", 6001)
    registry.register_worker("worker-2", "127.0.0.1", 6002)

    # Submit jobs.
    job_queue.submit(1, "ADD 20 30")
    job_queue.submit(2, "MUL 4 5")
    job_queue.submit(3, "DIV 10 0")

    # Simulated worker objects.
    workers = {
        "worker-1": WorkerSimulator("worker-1"),
        "worker-2": WorkerSimulator("worker-2"),
    }

    while job_queue.get_queue_size() > 0:
        job = job_queue.get_next_job()

        if job is None:
            break

        assignment = scheduler.assign_job(job)

        if assignment is None:
            print("No worker available. Job remains unassigned.")
            break

        worker_id = assignment["worker_id"]

        # Record the assigned worker in the job.
        job_queue.update_status(
            job["job_id"],
            "RUNNING",
            worker_id=worker_id,
        )

        print(
            f"\nScheduling job {job['job_id']} "
            f"to {worker_id}"
        )

        result = workers[worker_id].execute_job(job)

        # Record the execution result.
        job_queue.update_status(
            job["job_id"],
            result["status"],
            worker_id=worker_id,
            result=result["result"],
            error=result["error"],
        )

        # Release the worker after execution.
        scheduler.release_worker(worker_id)

    print("\n========== FINAL JOB REPORT ==========")

    for job in job_queue.get_all_jobs():
        print(job)


if __name__ == "__main__":
    main()

