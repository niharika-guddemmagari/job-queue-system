"""
Person 3: Distributed Job Queue and Worker Management
Python 3.10+
Run: python job_manager.py

Protocol: one JSON object per line over TCP.
Client messages:
  {"type":"submit_job","job_type":"sum","payload":{"start":1,"end":100}}
  {"type":"get_status","job_id":"..."}
  {"type":"get_result","job_id":"..."}

Worker messages:
  {"type":"register_worker","worker_id":"worker-1","host":"127.0.0.1","port":9001}
  {"type":"heartbeat","worker_id":"worker-1","busy":false}
  {"type":"job_result","worker_id":"worker-1","job_id":"...","attempt":1,
   "status":"completed","result":5050}
  {"type":"job_result","worker_id":"worker-1","job_id":"...","attempt":1,
   "status":"failed","error":"..."}
"""
import json
import socket
import threading
import time
import uuid
from queue import Queue, Empty

HOST = "127.0.0.1"
PORT = 5000
HEARTBEAT_TIMEOUT = 8.0
MONITOR_INTERVAL = 2.0
MAX_ATTEMPTS = 3

job_queue = Queue()
jobs = {}       # job_id -> job record
workers = {}    # worker_id -> worker record
lock = threading.RLock()
stopping = threading.Event()


def send_json_line(sock, obj):
    data = (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")
    sock.sendall(data)


def recv_json_lines(sock):
    """Yield complete newline-delimited JSON messages from a TCP stream."""
    buffer = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            if line.strip():
                yield json.loads(line.decode("utf-8"))


def public_job(job):
    """Return only fields safe/useful to expose to a client."""
    return {
        "job_id": job["job_id"],
        "job_type": job["job_type"],
        "status": job["status"],
        "worker_id": job.get("worker_id"),
        "attempts": job["attempts"],
        "result": job.get("result"),
        "error": job.get("error"),
    }


def add_job(message):
    job_type = message.get("job_type")
    payload = message.get("payload")
    if job_type not in {"sum", "factorial", "max"}:
        raise ValueError("Unsupported job_type. Use sum, factorial, or max.")
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object.")

    # Validate payload before queuing.
    if job_type == "sum":
        start, end = payload.get("start"), payload.get("end")
        if not isinstance(start, int) or not isinstance(end, int) or start > end:
            raise ValueError("sum requires integer start/end with start <= end.")
    elif job_type == "factorial":
        n = payload.get("n")
        if not isinstance(n, int) or n < 0 or n > 500:
            raise ValueError("factorial requires integer n in [0, 500].")
    elif job_type == "max":
        values = payload.get("values")
        if not isinstance(values, list) or not values or not all(isinstance(x, (int, float)) for x in values):
            raise ValueError("max requires a non-empty numeric values list.")

    job_id = str(uuid.uuid4())
    record = {
        "job_id": job_id,
        "job_type": job_type,
        "payload": payload,
        "status": "QUEUED",
        "worker_id": None,
        "attempts": 0,
        "result": None,
        "error": None,
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    with lock:
        jobs[job_id] = record
    job_queue.put(job_id)
    return job_id


def choose_idle_worker():
    """Least-loaded policy for this demo: choose an alive idle worker."""
    candidates = [
        (wid, w) for wid, w in workers.items()
        if w["alive"] and not w["busy"] and w.get("host") and w.get("port")
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[1].get("active_jobs", 0), item[0]))
    return candidates[0]


def dispatch_loop():
    while not stopping.is_set():
        try:
            job_id = job_queue.get(timeout=0.5)
        except Empty:
            continue

        with lock:
            job = jobs.get(job_id)
            if not job or job["status"] in {"COMPLETED", "FAILED"}:
                job_queue.task_done()
                continue

            selected = choose_idle_worker()
            if selected is None:
                # Put it back and wait briefly for worker registration/heartbeat.
                job_queue.put(job_id)
                job_queue.task_done()
                time.sleep(0.5)
                continue

            worker_id, worker = selected
            job["status"] = "RUNNING"
            job["worker_id"] = worker_id
            job["attempts"] += 1
            job["updated_at"] = time.time()
            worker["busy"] = True
            worker["active_jobs"] = worker.get("active_jobs", 0) + 1
            attempt = job["attempts"]
            host, port = worker["host"], worker["port"]
            payload = {
                "type": "execute_job",
                "job_id": job_id,
                "attempt": attempt,
                "job_type": job["job_type"],
                "payload": job["payload"],
            }

        try:
            with socket.create_connection((host, port), timeout=3) as worker_sock:
                send_json_line(worker_sock, payload)
        except OSError as exc:
            # Worker became unreachable before accepting the job.
            with lock:
                worker = workers.get(worker_id)
                if worker:
                    worker["alive"] = False
                    worker["busy"] = False
                    worker["active_jobs"] = max(0, worker.get("active_jobs", 1) - 1)
                job = jobs.get(job_id)
                if job and job["status"] == "RUNNING" and job["attempts"] == attempt:
                    job["worker_id"] = None
                    job["status"] = "QUEUED" if job["attempts"] < MAX_ATTEMPTS else "FAILED"
                    job["error"] = f"Could not dispatch to worker: {exc}"
                    job["updated_at"] = time.time()
                    if job["status"] == "QUEUED":
                        job_queue.put(job_id)
        finally:
            job_queue.task_done()


def mark_worker_failure(worker_id, reason):
    """Mark worker unavailable and requeue its running jobs, bounded by MAX_ATTEMPTS."""
    with lock:
        worker = workers.get(worker_id)
        if not worker or not worker["alive"]:
            return
        worker["alive"] = False
        worker["busy"] = False
        worker["active_jobs"] = 0
        print(f"[MONITOR] Worker {worker_id} marked unavailable: {reason}")

        for job in jobs.values():
            if job["worker_id"] == worker_id and job["status"] == "RUNNING":
                # A timeout cannot prove whether the worker finished just before failure.
                # This demo retries at-least-once; tasks should be idempotent.
                job["worker_id"] = None
                job["error"] = reason
                job["updated_at"] = time.time()
                if job["attempts"] < MAX_ATTEMPTS:
                    job["status"] = "QUEUED"
                    job_queue.put(job["job_id"])
                else:
                    job["status"] = "FAILED"


def monitor_loop():
    while not stopping.wait(MONITOR_INTERVAL):
        now = time.time()
        stale = []
        with lock:
            for worker_id, worker in workers.items():
                if worker["alive"] and now - worker["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                    stale.append(worker_id)
        for worker_id in stale:
            mark_worker_failure(worker_id, f"Heartbeat timeout after {HEARTBEAT_TIMEOUT:.0f}s")


def handle_worker_message(message):
    kind = message.get("type")
    worker_id = message.get("worker_id")
    if kind == "register_worker":
        if not worker_id:
            raise ValueError("worker_id is required.")
        with lock:
            workers[worker_id] = {
                "host": message.get("host", "127.0.0.1"),
                "port": int(message["port"]),
                "alive": True,
                "busy": False,
                "active_jobs": 0,
                "last_heartbeat": time.time(),
            }
        print(f"[WORKER] Registered {worker_id} at {workers[worker_id]['host']}:{workers[worker_id]['port']}")
        return {"type": "registered", "worker_id": worker_id}

    if kind == "heartbeat":
        with lock:
            worker = workers.get(worker_id)
            if not worker:
                return {"type": "error", "error": "Worker is not registered."}
            worker["alive"] = True
            worker["last_heartbeat"] = time.time()
            worker["busy"] = bool(message.get("busy", worker["busy"]))
        return {"type": "heartbeat_ack", "worker_id": worker_id}

    if kind == "job_result":
        job_id = message.get("job_id")
        attempt = message.get("attempt")
        with lock:
            worker = workers.get(worker_id)
            if worker:
                worker["busy"] = False
                worker["active_jobs"] = max(0, worker.get("active_jobs", 1) - 1)
                worker["last_heartbeat"] = time.time()
                worker["alive"] = True

            job = jobs.get(job_id)
            if not job:
                return {"type": "error", "error": "Unknown job_id."}
            # Ignore late results from an earlier attempt.
            if job["status"] != "RUNNING" or job["attempts"] != attempt or job["worker_id"] != worker_id:
                return {"type": "result_ignored", "job_id": job_id}
            if message.get("status") == "completed":
                job["status"] = "COMPLETED"
                job["result"] = message.get("result")
                job["error"] = None
            else:
                job["error"] = message.get("error", "Worker reported failure.")
                job["worker_id"] = None
                if job["attempts"] < MAX_ATTEMPTS:
                    job["status"] = "QUEUED"
                    job_queue.put(job_id)
                else:
                    job["status"] = "FAILED"
            job["updated_at"] = time.time()
        return {"type": "result_ack", "job_id": job_id}

    return {"type": "error", "error": f"Unknown worker message type: {kind}"}


def handle_client_message(message):
    kind = message.get("type")
    if kind == "submit_job":
        job_id = add_job(message)
        return {"type": "ack", "job_id": job_id, "status": "QUEUED"}
    if kind in {"get_status", "get_result"}:
        job_id = message.get("job_id")
        with lock:
            job = jobs.get(job_id)
            if not job:
                return {"type": "error", "error": "Unknown job_id."}
            return {"type": "job_info", **public_job(job)}
    if kind == "list_workers":
        with lock:
            return {
                "type": "workers",
                "workers": {
                    wid: {
                        "alive": w["alive"],
                        "busy": w["busy"],
                        "active_jobs": w.get("active_jobs", 0),
                        "last_heartbeat_age": round(time.time() - w["last_heartbeat"], 2),
                    }
                    for wid, w in workers.items()
                },
            }
    return {"type": "error", "error": f"Unknown client message type: {kind}"}


def connection_thread(conn, address):
    with conn:
        try:
            for message in recv_json_lines(conn):
                try:
                    if message.get("type") in {"register_worker", "heartbeat", "job_result"}:
                        response = handle_worker_message(message)
                    else:
                        response = handle_client_message(message)
                except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                    response = {"type": "error", "error": str(exc)}
                send_json_line(conn, response)
        except (ConnectionError, OSError, json.JSONDecodeError) as exc:
            print(f"[CONNECTION] {address} closed/error: {exc}")


def main():
    threading.Thread(target=dispatch_loop, daemon=True).start()
    threading.Thread(target=monitor_loop, daemon=True).start()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    print(f"[MANAGER] Listening on {HOST}:{PORT}")
    print("[MANAGER] Ctrl+C to stop.")
    try:
        while True:
            conn, address = server.accept()
            threading.Thread(target=connection_thread, args=(conn, address), daemon=True).start()
    except KeyboardInterrupt:
        print("\n[MANAGER] Shutting down...")
    finally:
        stopping.set()
        server.close()


if __name__ == "__main__":
    main()
