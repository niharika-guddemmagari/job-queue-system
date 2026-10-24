
"""
Person 1: TCP Client and Job Submission
Distributed Job Queue and Worker Management System

Run:
    python client.py

Protocol:
    JSON messages terminated by a newline over TCP.
"""

import json
import socket

# --------------------------------------------------
# Controller configuration
# --------------------------------------------------

MANAGER_HOST = "127.0.0.1"
MANAGER_PORT = 5000
CONNECTION_TIMEOUT = 5


# --------------------------------------------------
# TCP communication
# --------------------------------------------------

def send_request(message):
    """Send one JSON request and receive one JSON response."""

    try:
        with socket.create_connection(
            (MANAGER_HOST, MANAGER_PORT),
            timeout=CONNECTION_TIMEOUT
        ) as sock:

            sock.settimeout(CONNECTION_TIMEOUT)

            request_data = (
                json.dumps(message) + "\n"
            ).encode("utf-8")

            sock.sendall(request_data)

            # The manager returns one JSON response per line.
            with sock.makefile("r", encoding="utf-8") as reader:
                response_line = reader.readline()

            if not response_line:
                raise ConnectionError(
                    "The manager closed the connection "
                    "without sending a response."
                )

            return json.loads(response_line)

    except ConnectionRefusedError:
        print(
            "\nERROR: Could not connect to the manager.\n"
            "Check that the manager is running and that "
            "the host and port are correct."
        )

    except socket.timeout:
        print("\nERROR: The connection or response timed out.")

    except json.JSONDecodeError:
        print("\nERROR: The manager returned an invalid JSON response.")

    except (OSError, ConnectionError) as error:
        print(f"\nNETWORK ERROR: {error}")

    return None


# --------------------------------------------------
# Response display
# --------------------------------------------------

def display_response(response):
    """Display acknowledgments, results, and server errors."""

    if response is None:
        return

    response_type = response.get("type")

    if response_type == "ack":
        print("\nJOB SUBMISSION ACKNOWLEDGED")
        print("Job ID:", response.get("job_id"))
        print("Status:", response.get("status"))

    elif response_type == "job_info":
        print("\nJOB INFORMATION")
        print("Job ID:", response.get("job_id"))
        print("Job type:", response.get("job_type"))
        print("Status:", response.get("status"))
        print("Assigned worker:", response.get("worker_id") or "Not assigned")
        print("Attempts:", response.get("attempts"))

        if response.get("status") == "COMPLETED":
            print("Result:", response.get("result"))

        if response.get("error"):
            print("Error:", response["error"])

    elif response_type == "workers":
        workers = response.get("workers", {})

        print("\nREGISTERED WORKERS")

        if not workers:
            print("No workers are registered.")
            return

        for worker_id, details in workers.items():
            print(f"\nWorker ID: {worker_id}")
            print("Alive:", details.get("alive"))
            print("Busy:", details.get("busy"))
            print("Active jobs:", details.get("active_jobs"))
            print(
                "Last heartbeat age (seconds):",
                details.get("last_heartbeat_age")
            )

    elif response_type == "error":
        print("\nSERVER ERROR:", response.get("error", "Unknown error"))

    else:
        print("\nSERVER RESPONSE")
        print(json.dumps(response, indent=4))


# --------------------------------------------------
# Job submission
# --------------------------------------------------

def submit_sum_job():
    """Submit a job that sums integers in an inclusive range."""

    try:
        start = int(input("Start integer: "))
        end = int(input("End integer: "))

        if start > end:
            print("ERROR: Start must be less than or equal to end.")
            return

        message = {
            "type": "submit_job",
            "job_type": "sum",
            "payload": {
                "start": start,
                "end": end
            }
        }

        display_response(send_request(message))

    except ValueError:
        print("ERROR: Please enter valid integers.")


def submit_factorial_job():
    """Submit a factorial job."""

    try:
        n = int(input("Enter n (0 to 500): "))

        if not 0 <= n <= 500:
            print("ERROR: n must be between 0 and 500.")
            return

        message = {
            "type": "submit_job",
            "job_type": "factorial",
            "payload": {
                "n": n
            }
        }

        display_response(send_request(message))

    except ValueError:
        print("ERROR: Please enter a valid integer.")


def submit_max_job():
    """Submit a job that finds the maximum number in a list."""

    raw_values = input(
        "Enter numbers separated by commas: "
    ).strip()

    try:
        if not raw_values:
            raise ValueError

        values = [
            float(value.strip())
            for value in raw_values.split(",")
        ]

        message = {
            "type": "submit_job",
            "job_type": "max",
            "payload": {
                "values": values
            }
        }

        display_response(send_request(message))

    except ValueError:
        print(
            "ERROR: Enter one or more valid numbers, "
            "separated by commas."
        )


# --------------------------------------------------
# Job status and result
# --------------------------------------------------

def check_job_status():
    """Retrieve the current status or result of a submitted job."""

    job_id = input("Enter the job ID: ").strip()

    if not job_id:
        print("ERROR: Job ID cannot be empty.")
        return

    message = {
        "type": "get_status",
        "job_id": job_id
    }

    display_response(send_request(message))


# --------------------------------------------------
# Worker listing
# --------------------------------------------------

def list_workers():
    """Request registered worker information from the manager."""

    message = {
        "type": "list_workers"
    }

    display_response(send_request(message))


# --------------------------------------------------
# Main menu
# --------------------------------------------------

def main():
    print("=" * 45)
    print(" DISTRIBUTED JOB QUEUE - TCP CLIENT")
    print("=" * 45)
    print(f"Manager: {MANAGER_HOST}:{MANAGER_PORT}")

    while True:
        print("\n--------------- MAIN MENU ---------------")
        print("1. Submit sum job")
        print("2. Submit factorial job")
        print("3. Submit maximum job")
        print("4. Check job status/result")
        print("5. List registered workers")
        print("6. Exit")
        print("-----------------------------------------")

        choice = input("Enter your choice (1-6): ").strip()

        if choice == "1":
            submit_sum_job()

        elif choice == "2":
            submit_factorial_job()

        elif choice == "3":
            submit_max_job()

        elif choice == "4":
            check_job_status()

        elif choice == "5":
            list_workers()

        elif choice == "6":
            print("Exiting the client. Goodbye!")
            break

        else:
            print("Invalid choice. Please select 1 to 6.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nClient terminated.")
