
"""
JACKFRUIT - Distributed Job Manager

TCP manager integrating:
- Multiple independent clients
- Thread-safe job queue
- FIFO / least-loaded worker selection
- Worker registration and heartbeats
- Job dispatch and result collection
- Worker failure detection and job retry
- Dashboard status and control commands

Protocol: one newline-delimited JSON message per TCP connection.
"""

import json
import os
import socket
import sys
import threading
import time
import uuid
from collections import deque


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from nih.job_queue import JobQueue
from nih.scheduler import Scheduler
from nih.worker_registry import WorkerRegistry


MANAGER_HOST = "10.0.0.1"
MANAGER_PORT = 5000

HEARTBEAT_TIMEOUT = 8
MONITOR_INTERVAL = 2
MAX_ATTEMPTS = 3


class JobManager:

    def __init__(self):
        self.queue = JobQueue()
        self.scheduler = Scheduler()
        self.registry = WorkerRegistry(HEARTBEAT_TIMEOUT)

        self.jobs = {}
        self.events = deque(maxlen=3000)

        self.lock = threading.RLock()
        self.dispatch_condition = threading.Condition(self.lock)

        self.dispatch_paused = False
        self.running = True

        self.server = None

        self.add_event("manager_started", {
            "host": MANAGER_HOST,
            "port": MANAGER_PORT
        })

    # --------------------------------------------------
    # EVENTS
    # --------------------------------------------------

    def add_event(self, event_type, details=None):
        event = {
            "event_id": str(uuid.uuid4()),
            "type": event_type,
            "timestamp": time.time(),
            "details": details or {}
        }

        with self.lock:
            self.events.append(event)

        print(
            f"[EVENT] {event_type}: "
            f"{json.dumps(event['details'])}",
            flush=True
        )

        return event

    # --------------------------------------------------
    # JOB SUBMISSION
    # --------------------------------------------------

    def submit_job(self, message, client_ip):
        job_id = message.get("job_id") or str(uuid.uuid4())

        with self.lock:
            if job_id in self.jobs:
                return {
                    "status": "duplicate",
                    "job_id": job_id
                }

            job = {
                "job_id": job_id,
                "client_id": message.get("client_id", client_ip),
                "client_ip": client_ip,
                "job_type": message.get("job_type", "sum"),
                "payload": message.get("payload", {}),
                "status": "queued",
                "attempt": 0,
                "worker_id": None,
                "created_at": time.time(),
                "queued_at": time.time(),
                "dispatched_at": None,
                "execution_started_at": None,
                "completed_at": None,
                "queue_wait_ms": None,
                "execution_ms": None,
                "total_ms": None,
                "result": None,
                "error": None,
                "history": []
            }

            self.jobs[job_id] = job

            self.queue.enqueue(job)

            job["history"].append({
                "event": "submitted",
                "timestamp": job["created_at"],
                "client_ip": client_ip
            })

        self.add_event("job_submitted", {
            "job_id": job_id,
            "client_id": job["client_id"],
            "client_ip": client_ip,
            "job_type": job["job_type"]
        })

        with self.dispatch_condition:
            self.dispatch_condition.notify_all()

        return {
            "status": "queued",
            "job_id": job_id
        }

    # --------------------------------------------------
    # WORKER REGISTRATION AND HEARTBEATS
    # --------------------------------------------------

    def register_worker(self, message):
        worker_id = message.get("worker_id")
        host = message.get("host")
        port = message.get("port")

        if not worker_id or not host or not port:
            return {"status": "error", "message": "Missing worker details"}

        try:
            port = int(port)
        except (ValueError, TypeError):
            return {"status": "error", "message": "Invalid worker port"}

        worker = self.registry.register(
            worker_id,
            host,
            port,
            message.get("pid")
        )

        self.add_event("worker_registered", {
            "worker_id": worker_id,
            "host": host,
            "port": port
        })

        with self.dispatch_condition:
            self.dispatch_condition.notify_all()

        return {
            "status": "registered",
            "worker_id": worker_id
        }

    def heartbeat(self, message):
        worker_id = message.get("worker_id")

        if not worker_id:
            return {"status": "error"}

        updated = self.registry.heartbeat(
            worker_id,
            busy=message.get("busy", False),
            active_job=message.get("active_job"),
            jobs_completed=message.get("jobs_completed"),
            jobs_failed=message.get("jobs_failed")
        )

        if updated:
            with self.dispatch_condition:
                self.dispatch_condition.notify_all()

        return {
            "status": "ok" if updated else "unknown_worker"
        }

    # --------------------------------------------------
    # JOB RESULTS
    # --------------------------------------------------

    def handle_job_result(self, message):
        job_id = message.get("job_id")
        worker_id = message.get("worker_id")

        with self.lock:
            job = self.jobs.get(job_id)

            if not job:
                return {"status": "unknown_job"}

            if job["status"] in ("completed", "failed"):
                return {"status": "already_finished"}

            if job["worker_id"] != worker_id:
                return {"status": "stale_result"}

            if message.get("attempt", job["attempt"]) != job["attempt"]:
                return {"status": "stale_attempt"}

            success = message.get("status") == "completed"

            job["completed_at"] = time.time()
            job["execution_started_at"] = (
                message.get("execution_started_at")
                or job["dispatched_at"]
            )

            job["execution_ms"] = message.get("execution_ms")

            if job["execution_ms"] is None:
                if job["execution_started_at"]:
                    job["execution_ms"] = round(
                        (job["completed_at"] -
                         job["execution_started_at"]) * 1000,
                        3
                    )

            job["total_ms"] = round(
                (job["completed_at"] - job["created_at"]) * 1000,
                3
            )

            job["result"] = message.get("result")
            job["error"] = message.get("error")

            job["status"] = "completed" if success else "retry_pending"

            job["history"].append({
                "event": "completed" if success else "execution_failed",
                "timestamp": job["completed_at"],
                "worker_id": worker_id,
                "execution_ms": job["execution_ms"]
            })

        self.registry.finish_job(worker_id, success=success)

        self.add_event(
            "job_completed" if success else "job_execution_failed",
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "execution_ms": job["execution_ms"],
                "result": job["result"]
            }
        )

        if not success:
            self.retry_job(job_id, "worker reported execution failure")

        with self.dispatch_condition:
            self.dispatch_condition.notify_all()

        return {"status": "acknowledged"}

    # --------------------------------------------------
    # RETRY AND WORKER FAILURE
    # --------------------------------------------------

    def retry_job(self, job_id, reason):
        with self.lock:
            job = self.jobs.get(job_id)

            if not job or job["status"] == "completed":
                return

            if job["attempt"] >= MAX_ATTEMPTS:
                job["status"] = "failed"
                job["error"] = reason
                job["completed_at"] = time.time()
                job["total_ms"] = round(
                    (job["completed_at"] - job["created_at"]) * 1000,
                    3
                )

                self.add_event("job_permanently_failed", {
                    "job_id": job_id,
                    "reason": reason
                })
                return

            job["status"] = "queued"
            job["worker_id"] = None
            job["queued_at"] = time.time()
            job["dispatched_at"] = None
            job["execution_started_at"] = None

            job["history"].append({
                "event": "requeued",
                "timestamp": time.time(),
                "reason": reason
            })

            self.queue.enqueue(job)

        self.add_event("job_requeued", {
            "job_id": job_id,
            "reason": reason
        })

        with self.dispatch_condition:
            self.dispatch_condition.notify_all()

    def handle_worker_failure(self, worker_id):
        worker = self.registry.get(worker_id)

        if not worker or worker["status"] == "offline":
            return

        self.registry.mark_offline(worker_id)

        self.add_event("worker_offline", {
            "worker_id": worker_id,
            "host": worker["host"]
        })

        with self.lock:
            affected = [
                job["job_id"]
                for job in self.jobs.values()
                if job["worker_id"] == worker_id
                and job["status"] == "running"
            ]

        for job_id in affected:
            self.retry_job(
                job_id,
                f"Worker {worker_id} heartbeat timeout"
            )

    # --------------------------------------------------
    # DISPATCH
    # --------------------------------------------------

    def dispatch_loop(self):
        while self.running:

            with self.dispatch_condition:
                self.dispatch_condition.wait_for(
                    lambda: (
                        not self.running
                        or (
                            not self.dispatch_paused
                            and not self.queue.is_paused()
                            and len(self.queue) > 0
                            and len(self.registry.get_available()) > 0
                        )
                    ),
                    timeout=1
                )

                if not self.running:
                    break

                if self.dispatch_paused or self.queue.is_paused():
                    continue

            job = self.queue.peek()

            if not job:
                continue

            workers = self.registry.get_available()

            if not workers:
                continue

            worker = self.scheduler.select_worker(workers)

            if not worker:
                continue

            job_id = job["job_id"]
            worker_id = worker["worker_id"]

            if not self.registry.assign_job(worker_id, job_id):
                continue

            if not self.queue.remove(job_id):
                self.registry.finish_job(worker_id, success=False)
                continue

            with self.lock:
                current = self.jobs.get(job_id)

                if not current or current["status"] != "queued":
                    self.registry.finish_job(worker_id, success=False)
                    continue

                current["attempt"] += 1
                current["status"] = "running"
                current["worker_id"] = worker_id
                current["dispatched_at"] = time.time()

                current["queue_wait_ms"] = round(
                    (current["dispatched_at"] -
                     current["queued_at"]) * 1000,
                    3
                )

                current["history"].append({
                    "event": "dispatched",
                    "timestamp": current["dispatched_at"],
                    "worker_id": worker_id,
                    "attempt": current["attempt"]
                })

                attempt = current["attempt"]

            self.add_event("job_dispatched", {
                "job_id": job_id,
                "worker_id": worker_id,
                "worker_host": worker["host"],
                "worker_port": worker["port"],
                "attempt": attempt
            })

            assignment = {
                "type": "execute_job",
                "job_id": job_id,
                "job_type": job["job_type"],
                "payload": job["payload"],
                "attempt": attempt
            }

            threading.Thread(
                target=self.send_assignment,
                args=(worker, assignment),
                daemon=True
            ).start()

    def send_assignment(self, worker, assignment):
        try:
            with socket.create_connection(
                (worker["host"], worker["port"]),
                timeout=4
            ) as connection:

                connection.sendall(
                    (json.dumps(assignment) + "\n").encode()
                )

            self.add_event("assignment_sent", {
                "job_id": assignment["job_id"],
                "worker_id": worker["worker_id"]
            })

        except OSError as error:
            worker_id = worker["worker_id"]

            self.add_event("assignment_failed", {
                "job_id": assignment["job_id"],
                "worker_id": worker_id,
                "error": str(error)
            })

            self.registry.finish_job(worker_id, success=False)

            with self.lock:
                job = self.jobs.get(assignment["job_id"])

                if job and job["status"] == "running":
                    job["status"] = "queued"
                    job["worker_id"] = None
                    job["history"].append({
                        "event": "dispatch_failed",
                        "timestamp": time.time(),
                        "worker_id": worker_id,
                        "error": str(error)
                    })
                    job["queued_at"] = time.time()
                    self.queue.enqueue(job)

            with self.dispatch_condition:
                self.dispatch_condition.notify_all()

    # --------------------------------------------------
    # HEARTBEAT MONITOR
    # --------------------------------------------------

    def monitor_loop(self):
        while self.running:
            time.sleep(MONITOR_INTERVAL)

            expired = self.registry.find_expired()

            for worker_id in expired:
                self.handle_worker_failure(worker_id)

    # --------------------------------------------------
    # DASHBOARD DATA
    # --------------------------------------------------

    def get_jobs(self):
        with self.lock:
            return [
                dict(job, history=list(job["history"]))
                for job in self.jobs.values()
            ]

    def get_events(self):
        with self.lock:
            return list(self.events)

    def get_metrics(self):
        with self.lock:
            jobs = list(self.jobs.values())

        completed = [
            job for job in jobs
            if job["status"] == "completed"
        ]

        latencies = [
            job["total_ms"] for job in completed
            if job["total_ms"] is not None
        ]

        execution_times = [
            job["execution_ms"] for job in completed
            if job["execution_ms"] is not None
        ]

        queue_times = [
            job["queue_wait_ms"] for job in completed
            if job["queue_wait_ms"] is not None
        ]

        return {
            "total_jobs": len(jobs),
            "queued_jobs": sum(
                job["status"] == "queued" for job in jobs
            ),
            "running_jobs": sum(
                job["status"] == "running" for job in jobs
            ),
            "completed_jobs": len(completed),
            "failed_jobs": sum(
                job["status"] == "failed" for job in jobs
            ),
            "average_latency_ms": (
                round(sum(latencies) / len(latencies), 3)
                if latencies else 0
            ),
            "average_execution_ms": (
                round(sum(execution_times) / len(execution_times), 3)
                if execution_times else 0
            ),
            "average_queue_wait_ms": (
                round(sum(queue_times) / len(queue_times), 3)
                if queue_times else 0
            ),
            "queue": self.queue.get_stats(),
            "workers": self.registry.get_stats()
        }

    # --------------------------------------------------
    # TCP MESSAGE HANDLER
    # --------------------------------------------------

    def handle_message(self, message, client_ip):
        message_type = message.get("type")

        if message_type == "submit_job":
            return self.submit_job(message, client_ip)

        if message_type == "register_worker":
            return self.register_worker(message)

        if message_type == "heartbeat":
            return self.heartbeat(message)

        if message_type == "job_result":
            return self.handle_job_result(message)

        if message_type == "list_jobs":
            return {"jobs": self.get_jobs()}

        if message_type == "list_workers":
            return {"workers": self.registry.get_all()}

        if message_type == "list_events":
            return {"events": self.get_events()}

        if message_type == "get_metrics":
            return self.get_metrics()

        if message_type == "manager_status":
            return {
                "status": "running",
                "dispatch_paused": self.dispatch_paused,
                "queue_paused": self.queue.is_paused()
            }

        if message_type == "set_policy":
            policy = message.get("policy")

            try:
                self.scheduler.set_policy(policy)
                self.add_event("scheduling_policy_changed", {
                    "policy": policy
                })
                return {"status": "ok", "policy": policy}
            except ValueError as error:
                return {"status": "error", "message": str(error)}

        if message_type == "pause_dispatch":
            with self.dispatch_condition:
                self.dispatch_paused = True

            self.add_event("dispatch_paused")
            return {"status": "ok"}

        if message_type == "resume_dispatch":
            with self.dispatch_condition:
                self.dispatch_paused = False
                self.dispatch_condition.notify_all()

            self.add_event("dispatch_resumed")
            return {"status": "ok"}

        if message_type == "pause_queue":
            self.queue.pause()
            self.add_event("queue_paused")
            return {"status": "ok"}

        if message_type == "resume_queue":
            self.queue.resume()

            with self.dispatch_condition:
                self.dispatch_condition.notify_all()

            self.add_event("queue_resumed")
            return {"status": "ok"}

        return {
            "status": "error",
            "message": f"Unknown message type: {message_type}"
        }

    def handle_connection(self, connection, address):
        try:
            connection.settimeout(10)

            data = b""

            while b"\n" not in data and len(data) < 1_000_000:
                chunk = connection.recv(4096)

                if not chunk:
                    break

                data += chunk

            if not data:
                return

            message = json.loads(data.split(b"\n", 1)[0].decode())

            response = self.handle_message(
                message,
                address[0]
            )

            connection.sendall(
                (json.dumps(response, default=str) + "\n").encode()
            )

        except (
            OSError,
            ValueError,
            json.JSONDecodeError
        ) as error:
            print(f"[TCP ERROR] {error}", flush=True)

            try:
                connection.sendall(
                    b'{"status":"error","message":"invalid request"}\n'
                )
            except OSError:
                pass

        finally:
            connection.close()

    # --------------------------------------------------
    # START
    # --------------------------------------------------

    def start(self):
        threading.Thread(
            target=self.dispatch_loop,
            daemon=True
        ).start()

        threading.Thread(
            target=self.monitor_loop,
            daemon=True
        ).start()

        self.server = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self.server.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        self.server.bind((MANAGER_HOST, MANAGER_PORT))
        self.server.listen(100)

        print(
            f"[MANAGER] Listening on "
            f"{MANAGER_HOST}:{MANAGER_PORT}",
            flush=True
        )

        while self.running:
            try:
                connection, address = self.server.accept()

                threading.Thread(
                    target=self.handle_connection,
                    args=(connection, address),
                    daemon=True
                ).start()

            except OSError:
                if self.running:
                    raise
                break

    def stop(self):
        self.running = False

        with self.dispatch_condition:
            self.dispatch_condition.notify_all()

        if self.server:
            self.server.close()


if __name__ == "__main__":
    manager = JobManager()

    try:
        manager.start()
    except KeyboardInterrupt:
        print("\n[MANAGER] Shutting down")
        manager.stop()

