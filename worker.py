
import json
import math
import os
import socket
import sys
import threading
import time
import traceback

WORKER_ID = sys.argv[1] if len(sys.argv) > 1 else "worker-1"
WORKER_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9001
WORKER_HOST = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
MANAGER_HOST = sys.argv[4] if len(sys.argv) > 4 else "127.0.0.1"
MANAGER_PORT = 5000

HEARTBEAT_INTERVAL = 2
busy = False
active_job = None
lock = threading.RLock()
stop_event = threading.Event()


def request_manager(message):
    with socket.create_connection(
        (MANAGER_HOST, MANAGER_PORT), timeout=5
    ) as sock:
        sock.sendall((json.dumps(message) + "\n").encode())
        data = b""
        while b"\n" not in data:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
        if not data:
            return {}
        return json.loads(data.split(b"\n", 1)[0].decode())


def get_numbers(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("numbers", "data", "values", "items"):
            if key in payload and isinstance(payload[key], list):
                return payload[key]
        if "number" in payload:
            return [payload["number"]]
        if "n" in payload:
            return [payload["n"]]
    return []


def execute_job(assignment):
    job_type = str(
        assignment.get("job_type")
        or assignment.get("operation")
        or assignment.get("task")
        or assignment.get("type")
        or ""
    ).lower()

    payload = assignment.get("payload", {})
    numbers = get_numbers(payload)

    if not numbers:
        numbers = get_numbers(assignment)

    if job_type in ("sum", "addition"):
        return sum(numbers)

    if job_type in ("max", "maximum"):
        if not numbers:
            raise ValueError("No numbers supplied for max job")
        return max(numbers)

    if job_type in ("factorial", "fact"):
        n = payload.get("n", payload.get("number")) if isinstance(payload, dict) else None
        if n is None and numbers:
            n = numbers[0]
        if n is None:
            n = assignment.get("n")
        if n is None:
            raise ValueError("No number supplied for factorial")
        n = int(n)
        if n < 0:
            raise ValueError("Factorial requires a non-negative integer")
        return math.factorial(n)

    raise ValueError("Unsupported job type: " + job_type)


def send_result(assignment, status, result=None, error=None,
                started_at=None, execution_ms=None):
    message = {
        "type": "job_result",
        "job_id": assignment.get("job_id"),
        "worker_id": WORKER_ID,
        "attempt": assignment.get("attempt", 1),
        "status": status,
        "result": result,
        "error": error,
        "execution_started_at": started_at,
        "execution_ms": execution_ms
    }
    try:
        response = request_manager(message)
        print(f"[{WORKER_ID}] Result response: {response}", flush=True)
    except Exception as exc:
        print(f"[{WORKER_ID}] Could not report result: {exc}", flush=True)


def handle_assignment(connection):
    global busy, active_job

    try:
        data = b""
        while b"\n" not in data:
            chunk = connection.recv(65536)
            if not chunk:
                break
            data += chunk

        if not data:
            return

        assignment = json.loads(data.split(b"\n", 1)[0].decode())
        job_id = assignment.get("job_id")

        with lock:
            if busy:
                print(f"[{WORKER_ID}] Already busy; ignoring assignment", flush=True)
                return
            busy = True
            active_job = job_id

        print(f"[{WORKER_ID}] Received job {job_id}", flush=True)

        started_at = time.time()
        try:
            result = execute_job(assignment)
            execution_ms = round((time.time() - started_at) * 1000, 3)
            print(
                f"[{WORKER_ID}] Job {job_id} completed: {result}",
                flush=True
            )
            send_result(
                assignment, "completed", result=result,
                started_at=started_at, execution_ms=execution_ms
            )
        except Exception as exc:
            execution_ms = round((time.time() - started_at) * 1000, 3)
            print(f"[{WORKER_ID}] Job failed: {exc}", flush=True)
            send_result(
                assignment, "failed", error=str(exc),
                started_at=started_at, execution_ms=execution_ms
            )
        finally:
            with lock:
                busy = False
                active_job = None

    except Exception:
        traceback.print_exc()
    finally:
        connection.close()


def heartbeat_loop():
    while not stop_event.is_set():
        with lock:
            current_status = "busy" if busy else "idle"
            current_job = active_job

        try:
            request_manager({
                "type": "heartbeat",
                "worker_id": WORKER_ID,
                "status": current_status,
                "active_job": current_job
            })
        except Exception as exc:
            print(f"[{WORKER_ID}] Heartbeat error: {exc}", flush=True)

        stop_event.wait(HEARTBEAT_INTERVAL)


def main():
    global busy

    print("=" * 50, flush=True)
    print(f"JACKFRUIT WORKER: {WORKER_ID}", flush=True)
    print(f"Worker IP: {WORKER_HOST}", flush=True)
    print(f"Worker port: {WORKER_PORT}", flush=True)
    print(f"Manager: {MANAGER_HOST}:{MANAGER_PORT}", flush=True)
    print(f"PID: {os.getpid()}", flush=True)
    print("=" * 50, flush=True)

    registration = request_manager({
        "type": "register_worker",
        "worker_id": WORKER_ID,
        "host": WORKER_HOST,
        "port": WORKER_PORT,
        "pid": os.getpid()
    })

    print(f"[{WORKER_ID}] Registration: {registration}", flush=True)

    if registration.get("status") != "registered":
        raise RuntimeError(f"Registration failed: {registration}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", WORKER_PORT))
    server.listen(20)
    server.settimeout(1)

    threading.Thread(target=heartbeat_loop, daemon=True).start()

    print(f"[{WORKER_ID}] Listening on port {WORKER_PORT}", flush=True)

    try:
        while True:
            try:
                connection, address = server.accept()
                threading.Thread(
                    target=handle_assignment,
                    args=(connection,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
    except KeyboardInterrupt:
        print(f"[{WORKER_ID}] Shutting down", flush=True)
    finally:
        stop_event.set()
        server.close()


if __name__ == "__main__":
    main()
PY
