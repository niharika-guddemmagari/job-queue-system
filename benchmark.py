
"""
JACKFRUIT - Client Benchmark
Submits jobs from one client host and records ACK latency.
"""

import argparse
import csv
import statistics
import time

from client import send_message, build_payload


def run_benchmark(client_id, host, port, count,
                  job_type, size, delay):

    results = []

    print(f"\nStarting benchmark: {client_id}")
    print(f"Manager: {host}:{port}")
    print(f"Jobs: {count}\n")

    for number in range(1, count + 1):
        message = {
            "type": "submit_job",
            "client_id": client_id,
            "job_id": f"{client_id}-bench-{number}",
            "job_type": job_type,
            "payload": build_payload(job_type, size)
        }

        start = time.perf_counter()

        try:
            response = send_message(message, host, port)
            elapsed_ms = (
                time.perf_counter() - start
            ) * 1000

            status = response.get("status", "unknown")

            results.append({
                "client_id": client_id,
                "job_number": number,
                "job_id": message["job_id"],
                "status": status,
                "ack_latency_ms": round(elapsed_ms, 3)
            })

            print(
                f"Job {number}/{count}: "
                f"{status}, ACK {elapsed_ms:.2f} ms"
            )

        except (OSError, ValueError, ConnectionError) as error:
            results.append({
                "client_id": client_id,
                "job_number": number,
                "job_id": message["job_id"],
                "status": "submission_failed",
                "ack_latency_ms": None
            })

            print(f"Job {number} failed: {error}")

        if delay > 0:
            time.sleep(delay)

    return results


def save_csv(results, filename):
    if not results:
        print("No benchmark results to save.")
        return

    with open(filename, "w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(results[0].keys())
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nResults saved to {filename}")


def print_summary(results):
    successful = [
        row for row in results
        if row["status"] == "queued"
    ]

    latencies = [
        row["ack_latency_ms"]
        for row in successful
        if row["ack_latency_ms"] is not None
    ]

    print("\n--- BENCHMARK SUMMARY ---")
    print(f"Submitted: {len(results)}")
    print(f"Accepted: {len(successful)}")
    print(f"Failed: {len(results) - len(successful)}")

    if latencies:
        print(
            f"Average ACK latency: "
            f"{statistics.mean(latencies):.2f} ms"
        )
        print(
            f"Minimum ACK latency: "
            f"{min(latencies):.2f} ms"
        )
        print(
            f"Maximum ACK latency: "
            f"{max(latencies):.2f} ms"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Jackfruit job-system benchmark"
    )

    parser.add_argument("--client-id", default="benchmark-client")
    parser.add_argument("--host", default="10.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument(
        "--job-type",
        choices=["sum", "factorial", "max"],
        default="sum"
    )
    parser.add_argument("--size", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.05)
    parser.add_argument("--output", default="benchmark_results.csv")

    args = parser.parse_args()

    if args.count < 1 or args.size < 1 or args.delay < 0:
        parser.error("count and size must be positive; delay cannot be negative")

    results = run_benchmark(
        args.client_id,
        args.host,
        args.port,
        args.count,
        args.job_type,
        args.size,
        args.delay
    )

    print_summary(results)
    save_csv(results, args.output)


if __name__ == "__main__":
    main()

