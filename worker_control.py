
"""
JACKFRUIT - Mininet Worker Process Control

Uses explicit worker PID files. Refuses to kill a process
unless its command line matches the expected worker script.
"""

import os
import signal
import subprocess
import time

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PID_DIR = "/tmp"

WORKERS = {
    "worker-1": {"host": "h2", "port": 9001, "ip": "10.0.0.2"},
    "worker-2": {"host": "h3", "port": 9002, "ip": "10.0.0.3"}
}


def host_pid(host):
    result = subprocess.run(
        ["mnexec", "-a", "1", "true"],
        capture_output=True,
        text=True
    )

    # Find the Mininet host process by its exact host marker.
    result = subprocess.run(
        ["pgrep", "-af", f"mininet:{host}"],
        capture_output=True,
        text=True
    )

    for line in result.stdout.splitlines():
        parts = line.strip().split(maxsplit=1)
        if len(parts) == 2 and f"mininet:{host}" in parts[1]:
            return int(parts[0])

    return None


def pid_file(worker_id):
    return os.path.join(PID_DIR, f"jackfruit-{worker_id}.pid")


def worker_pid(worker_id):
    path = pid_file(worker_id)

    try:
        with open(path) as file:
            pid = int(file.read().strip())

        os.kill(pid, 0)
        return pid

    except (OSError, ValueError):
        return None


def stop_worker(worker_id):
    if worker_id not in WORKERS:
        return {"status": "error", "message": "Unknown worker"}

    pid = worker_pid(worker_id)

    if pid is None:
        return {"status": "error", "message": "Worker PID not found"}

    try:
        with open(f"/proc/{pid}/cmdline", "rb") as file:
            command = file.read().decode(errors="replace")

        if "ank/worker.py" not in command or worker_id not in command:
            return {
                "status": "error",
                "message": "PID does not match the expected worker"
            }

        os.kill(pid, signal.SIGTERM)

        return {"status": "stopping", "worker_id": worker_id, "pid": pid}

    except OSError as error:
        return {"status": "error", "message": str(error)}


def start_worker(worker_id):
    if worker_id not in WORKERS:
        return {"status": "error", "message": "Unknown worker"}

    if worker_pid(worker_id):
        return {"status": "error", "message": "Worker already running"}

    config = WORKERS[worker_id]
    namespace_pid = host_pid(config["host"])

    if namespace_pid is None:
        return {"status": "error", "message": "Mininet host not found"}

    command = (
        f"cd {PROJECT} && "
        f"exec python3 ank/worker.py "
        f"{worker_id} {config['port']} "
        f"{config['ip']} 10.0.0.1"
    )

    try:
        process = subprocess.Popen(
            [
                "mnexec", "-a", str(namespace_pid),
                "sh", "-c",
                f"echo $$ > {pid_file(worker_id)}; {command}"
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )

        time.sleep(0.5)

        pid = worker_pid(worker_id)

        if pid is None:
            return {
                "status": "error",
                "message": "Worker did not create its PID file"
            }

        return {
            "status": "started",
            "worker_id": worker_id,
            "pid": pid
        }

    except OSError as error:
        return {"status": "error", "message": str(error)}
