import json
import socket
import time

HOST = "10.0.0.1"
PORT = 5000
N = 5
SIZE = 1000000

def request(msg):
    with socket.create_connection((HOST, PORT), timeout=5) as s:
        s.sendall((json.dumps(msg) + "\n").encode())
        f = s.makefile("r", encoding="utf-8")
        return json.loads(f.readline())

jobs = {}
start = time.monotonic()

for i in range(N):
    t = time.monotonic()
    ack = request({
        "type": "submit_job",
        "job_type": "sum",
        "payload": {"start": 1, "end": SIZE}
    })
    jid = ack["job_id"]
    jobs[jid] = {"submitted": t, "result": None}
    print("Submitted:", jid)

while any(j["result"] is None for j in jobs.values()):
    for jid, info in jobs.items():
        if info["result"] is not None:
            continue
        status = request({"type": "get_status", "job_id": jid})
        if status.get("status") == "COMPLETED":
            info["result"] = status
            info["elapsed"] = time.monotonic() - info["submitted"]
    time.sleep(0.1)

total = time.monotonic() - start

print("\n========== BENCHMARK RESULTS ==========")
for jid, info in jobs.items():
    r = info["result"]
    print("Job:", jid)
    print("Worker:", r.get("worker_id"))
    print("Attempts:", r.get("attempts"))
    print("Result:", r.get("result"))
    print("End-to-end latency: %.4f seconds" % info["elapsed"])
    print("---------------------------------------")

print("Total elapsed: %.4f seconds" % total)
print("Throughput: %.4f jobs/second" % (N / total))
