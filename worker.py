
"""
Worker node for the Mininet distributed job queue.

Usage:
    python3 worker.py worker-1 9001 10.0.0.2 10.0.0.1

Arguments:
    1. Worker ID
    2. Worker listening port
    3. This worker's Mininet IP
    4. Manager's Mininet IP
"""

import json
import math
import socket
import sys
import threading
import time

WORKER_ID = sys.argv[1] if len(sys.argv) > 1 else "worker-1"
WORKER_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9001
WORKER_HOST = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"

MANAGER_HOST = sys.argv[4] if len(sys.argv) > 4 else "127.0.0.1"
MANAGER_PORT = 5000
HEARTBEAT_INTERVAL = 2.0

busy_lock = threading.Lock()
busy = False
stop_event = threading.Event()


def send_json_line(sock, obj):
    data = (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")
    sock.sendall(data)


def request_manager(message):
    with socket.create_connection(
        (MANAGER_HOST, MANAGER_PORT), timeout=3
    ) as sock:
        send_json_line(sock, message)
        with sock.makefile("r", encoding="utf-8") as reader:
            line = reader.readline()

        if not line:
            raise ConnectionError("Manager closed connection without replying.")

        return json.loads(line)


def heartbeat_loop():
    while not stop_event.wait(HEARTBEAT_INTERVAL):
        with busy_lock:
            current_busy = busy

        try:
            request_manager({
                "type": "heartbeat",
                "worker_id": WORKER_ID,
                "busy": current_busy,
            })
        except (OSError, ConnectionError) as exc:
            print(f"[{WORKER_ID}] Heartbeat failed: {exc}")


def execute(job_type, payload):
    if job_type == "sum":
        return sum(range(payload["start"], payload["end"] + 1))

    if job_type == "factorial":
        return math.factorial(payload["n"])

    if job_type == "max":
        return max(payload["values"])

    raise ValueError(f"Unsupported job type: {job_type}")


def report_result(job_id, attempt, status, result=None, error=None):
    message = {
        "type": "job_result",
        "worker_id": WORKER_ID,
        "job_id": job_id,
        "attempt": attempt,
        "status": status,
    }

    if status == "completed":
        message["result"] = result
    else:
        message["error"] = error or "Unknown worker error."

    try:
        response = request_manager(message)
        print(f"[{WORKER_ID}] Manager response: {response}")
    except (OSError, ConnectionError) as exc:
        print(f"[{WORKER_ID}] Could not report result: {exc}")


def handle_assignment(conn):
    global busy

    with conn:
        with conn.makefile("r", encoding="utf-8") as reader:
            line = reader.readline()

        if not line:
            return

        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            print(f"[{WORKER_ID}] Received invalid JSON.")
            return

        if message.get("type") != "execute_job":
            return

        job_id = message["job_id"]
        attempt = message["attempt"]

        with busy_lock:
            busy = True

        print(f"[{WORKER_ID}] Running job {job_id}, attempt {attempt}")

        try:
            result = execute(message["job_type"], message["payload"])
            report_result(job_id, attempt, "completed", result=result)
        except Exception as exc:
            report_result(job_id, attempt, "failed", error=str(exc))
        finally:
            with busy_lock:
                busy = False


def main():
    # Register this worker using its reachable Mininet IP.
    response = request_manager({
        "type": "register_worker",
        "worker_id": WORKER_ID,
        "host": WORKER_HOST,
        "port": WORKER_PORT,
    })

    print(f"[{WORKER_ID}] Registration: {response}")

    threading.Thread(target=heartbeat_loop, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", WORKER_PORT))
    server.listen()

    print(
        f"[{WORKER_ID}] Listening on {WORKER_HOST}:{WORKER_PORT}; "
        f"manager={MANAGER_HOST}:{MANAGER_PORT}"
    )

    try:
        while True:
            conn, address = server.accept()
            threading.Thread(
                target=handle_assignment,
                args=(conn,),
                daemon=True,
            ).start()
    except KeyboardInterrupt:
        print(f"\n[{WORKER_ID}] Stopping.")
    finally:
        stop_event.set()
        server.close()


if __name__ == "__main__":
    main()
