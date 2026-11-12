
"""
JACKFRUIT - TCP Job Client
Supports multiple independent client hosts and concurrent submissions.
"""

import argparse
import json
import socket
import threading
import time
import uuid


MANAGER_HOST = "10.0.0.1"
MANAGER_PORT = 5000


def send_message(message, host, port, timeout=10):
    """Send one JSON message and receive one JSON response."""

    with socket.create_connection(
        (host, port), timeout=timeout
    ) as sock:
        sock.settimeout(timeout)

        file = sock.makefile("rwb")

        file.write(
            (json.dumps(message) + "\n").encode()
        )
        file.flush()

        response = file.readline()

        if not response:
            raise ConnectionError("Manager closed the connection")

        return json.loads(response.decode())


def submit_job(client_id, job_number, job_type, payload,
               host, port):

    job_id = f"{client_id}-{uuid.uuid4().hex[:8]}"

    message = {
        "type": "submit_job",
        "job_id": job_id,
        "client_id": client_id,
        "job_type": job_type,
        "payload": payload
    }

    submitted_at = time.time()

    try:
        response = send_message(message, host, port)

        acknowledged_at = time.time()
        latency_ms = (acknowledged_at - submitted_at) * 1000

        print(
            f"[{client_id}] Job {job_number} | "
            f"ID={job_id} | "
            f"ACK={response.get('status', 'unknown')} | "
            f"ACK latency={latency_ms:.2f} ms"
        )

        return {
            "job_id": job_id,
            "status": response.get("status"),
            "ack_latency_ms": round(latency_ms, 3)
        }

    except (OSError, ValueError, ConnectionError) as error:
        print(f"[{client_id}] Job {job_id} failed: {error}")
        return {
            "job_id": job_id,
            "status": "submission_failed",
            "error": str(error)
        }


def build_payload(job_type, size):
    if job_type == "sum":
        return {"start": 1, "end": size}

    if job_type == "factorial":
        return {"number": min(size, 100)}

    if job_type == "max":
        return {
            "numbers": list(range(size, 0, -1))
        }

    raise ValueError(f"Unsupported job type: {job_type}")


def main():
    parser = argparse.ArgumentParser(
        description="Jackfruit distributed job client"
    )

    parser.add_argument(
        "--client-id",
        default="client",
        help="Unique name for this client host"
    )

    parser.add_argument(
        "--host",
        default=MANAGER_HOST
    )

    parser.add_argument(
        "--port",
        type=int,
        default=MANAGER_PORT
    )

    parser.add_argument(
        "--count",
        type=int,
        default=5
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=1
    )

    parser.add_argument(
        "--job-type",
        choices=["sum", "factorial", "max"],
        default="sum"
    )

    parser.add_argument(
        "--size",
        type=int,
        default=100
    )

    args = parser.parse_args()

    if args.count < 1 or args.concurrency < 1 or args.size < 1:
        parser.error("count, concurrency and size must be positive")

    print(
        f"\nClient: {args.client_id}\n"
        f"Manager: {args.host}:{args.port}\n"
        f"Jobs: {args.count}\n"
        f"Concurrency: {args.concurrency}\n"
    )

    results = []
    results_lock = threading.Lock()
    next_job = 0

    def submit_batch():
        nonlocal next_job

        while True:
            with results_lock:
                if next_job >= args.count:
                    return

                job_number = next_job + 1
                next_job += 1

            payload = build_payload(
                args.job_type,
                args.size
            )

            result = submit_job(
                args.client_id,
                job_number,
                args.job_type,
                payload,
                args.host,
                args.port
            )

            with results_lock:
                results.append(result)

    threads = []

    for _ in range(min(args.concurrency, args.count)):
        thread = threading.Thread(target=submit_batch)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    successful = sum(
        result["status"] == "queued"
        for result in results
    )

    print("\n--- CLIENT SUMMARY ---")
    print(f"Submitted: {len(results)}")
    print(f"Accepted: {successful}")
    print(f"Failed: {len(results) - successful}")


if __name__ == "__main__":
    main()

