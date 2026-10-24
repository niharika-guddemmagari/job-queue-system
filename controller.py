
import socket

from job_queue import JobQueue
from worker_registry import WorkerRegistry
from scheduler import Scheduler
from worker_simulator import WorkerSimulator

HOST = "127.0.0.1"
PORT = 5000


def handle_message(message, job_queue, registry, scheduler, workers):
    parts = message.split("|", 2)

    if len(parts) != 3 or parts[0] != "SUBMIT":
        return "JOB_FAILED|0|Invalid message format"

    try:
        job_id = int(parts[1])
        if job_id < 1:
            raise ValueError
    except ValueError:
        return "JOB_FAILED|0|Invalid job ID"

    description = parts[2].strip()

    if not description:
        return f"JOB_FAILED|{job_id}|Empty job description"

    # Add the job to the queue.
    try:
        job_queue.submit(job_id, description)
    except ValueError:
        return f"JOB_FAILED|{job_id}|Duplicate job ID"

    responses = [
        f"ACK|{job_id}",
        f"JOB_QUEUED|{job_id}"
    ]

    job = job_queue.get_job(job_id)

    # Select a worker.
    worker = scheduler.assign_job(job)

    if worker is None:
        job_queue.update_status(
            job_id, "FAILED", error="No available workers"
        )
        responses.append(
            f"JOB_FAILED|{job_id}|No available workers"
        )
        return "\n".join(responses)

    # assign_job may return a worker dictionary or a worker ID.
    worker_id = (
        worker["worker_id"] if isinstance(worker, dict) else worker
    )

    if worker_id not in workers:
        job_queue.update_status(
            job_id, "FAILED", error="Worker simulator not found"
        )
        scheduler.release_worker(worker_id)
        responses.append(
            f"JOB_FAILED|{job_id}|Worker simulator not found"
        )
        return "\n".join(responses)

    job_queue.update_status(
        job_id, "RUNNING", worker_id=worker_id
    )
    responses.append(f"JOB_RUNNING|{job_id}")

    try:
        result = workers[worker_id].execute_job(job)

        if result["status"] == "COMPLETED":
            job_queue.update_status(
                job_id,
                "COMPLETED",
                result=result["result"]
            )
            responses.append(
                f"JOB_COMPLETED|{job_id}|{result['result']}"
            )
        else:
            error = str(
                result.get("error", "Job execution failed")
            ).replace("|", "/").replace("\n", " ")

            job_queue.update_status(
                job_id, "FAILED", error=error
            )
            responses.append(
                f"JOB_FAILED|{job_id}|{error}"
            )

    except Exception as exc:
        error = str(exc).replace("|", "/").replace("\n", " ")
        job_queue.update_status(
            job_id, "FAILED", error=error
        )
        responses.append(
            f"JOB_FAILED|{job_id}|{error}"
        )

    finally:
        scheduler.release_worker(worker_id)

    return "\n".join(responses)


def main():
    job_queue = JobQueue()
    registry = WorkerRegistry()
    scheduler = Scheduler(registry)

    # Create simulated workers.
    workers = {
        "worker-1": WorkerSimulator("worker-1"),
        "worker-2": WorkerSimulator("worker-2")
    }

    # Register workers with the registry.
    registry.register_worker("worker-1", "127.0.0.1", 6001)
    registry.register_worker("worker-2", "127.0.0.1", 6002)

    print("Registered workers:", registry.get_all_workers())

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(5)

        print(f"Controller listening on {HOST}:{PORT}")

        try:
            while True:
                connection, address = server.accept()
                print(f"Client connected: {address}")

                with connection:
                    reader = connection.makefile(
                        "r", encoding="utf-8"
                    )

                    with reader:
                        for line in reader:
                            message = line.rstrip("\r\n")
                            print("Received:", message)

                            if message == "QUIT":
                                connection.sendall(b"BYE\n")
                                break

                            response = handle_message(
                                message,
                                job_queue,
                                registry,
                                scheduler,
                                workers
                            )

                            connection.sendall(
                                (response + "\n").encode("utf-8")
                            )

                            print(
                                "Current jobs:",
                                job_queue.get_all_jobs()
                            )
                            print(
                                "Worker status:",
                                registry.get_all_workers()
                            )

                print("Client disconnected.")

        except KeyboardInterrupt:
            print("\nController stopped.")


if __name__ == "__main__":
    main()


