# JACKFRUIT: Distributed Job Queue over an SDN (Mininet + Ryu)

A TCP job-queue system (clients, job manager, workers) running on a
multi-switch Mininet network controlled by a Ryu SDN controller, with a web
dashboard for monitoring jobs, workers and the topology.

## Layout

```
.
├── topology.py          # Mininet topology: 6 hosts, 4 switches (redundant path, STP)
├── dashboard.py         # Web dashboard on http://localhost:8080
├── worker/                 # Job manager + worker
│   ├── job_manager.py   #   listens on 10.0.0.1:5000, scheduling, retries
│   ├── worker.py        #   registers with the manager, sends heartbeats, runs jobs
│   └── README.md        #   standalone (no-Mininet) run instructions
├── client/                  # Client side
│   ├── client.py        #   interactive TCP client (submit job, query status/result)
│   └── benchmark.py     #   benchmark client
└── controller/                 # Queue, scheduling, registry, SDN integration
    ├── job_queue.py
    ├── scheduler.py
    ├── worker_registry.py
    ├── worker_simulator.py
    ├── worker_control.py  # start/stop workers inside Mininet hosts
    ├── sdn_adapter.py
    ├── controller.py      # Ryu ofctl_rest API client (127.0.0.1:8081)
    └── test_system.py
```

## Network

| Host  | IP         | Role                 |
|-------|------------|----------------------|
| h1    | 10.0.0.1   | Manager + Client 1   |
| h2    | 10.0.0.2   | worker-1 (port 9001) |
| h3    | 10.0.0.3   | worker-2 (port 9002) |
| h4-h6 | 10.0.0.4-6 | Clients 2-4          |

Switches s1-s4 form a ring (s1-s2, s1-s3, s2-s4, s3-s4) with STP enabled.

## Requirements

- Linux (developed in a VirtualBox VM) with root access
- Python 3.10+ (developed on 3.14)
- [Mininet](http://mininet.org/) and Open vSwitch
  (`sudo apt install mininet openvswitch-switch`)
- [Ryu](https://ryu-sdn.org/) (`pip install ryu`)
- The job system itself uses only the Python standard library

## Running

1. **Start the Ryu controller** (OpenFlow on 6633, REST API on 8081). Use the
   Ryu apps you developed with; for example:
   ```bash
   ryu-manager ryu.app.simple_switch_stp_13 ryu.app.ofctl_rest \
       --ofp-tcp-listen-port 6633 --wsapi-port 8081
   ```
2. **Start Mininet:**
   ```bash
   sudo python3 topology.py
   ```
3. **Start the manager and workers** from the Mininet CLI:
   ```
   mininet> h1 python3 ank/job_manager.py &
   mininet> h2 python3 ank/worker.py worker-1 9001 10.0.0.2 10.0.0.1 &
   mininet> h3 python3 ank/worker.py worker-2 9002 10.0.0.3 10.0.0.1 &
   ```
4. **Submit jobs** from a client host:
   ```
   mininet> h4 python3 me/client.py
   ```
5. **Dashboard.** It reaches the manager through its Mininet network
   namespace, so pass the PID of the manager process running on h1:
   ```bash
   sudo MANAGER_NAMESPACE_PID=<pid> python3 dashboard.py
   ```
   then open http://localhost:8080.

Supported job types: `sum`, `max`, `factorial`.

## Notes

- Components exchange newline-terminated JSON over TCP.
- Worker start/stop from the dashboard uses `mnexec` and PID files in `/tmp`.
- `MANAGER_NAMESPACE_PID` falls back to the value hardcoded in `dashboard.py`;
  it changes every Mininet session, so set it each time.
## team contribution
Name	SRN	Part	Folder
Jyotirmayee Pradhan	PES1UG25CS232	Client: client.py submits jobs and queries results, benchmark.py measures ACK latency	me/
Guddemma Gari Niharika	PES1UG25CS193	Queue, scheduler, worker registry, worker_control.py, SDN adapter and Ryu client	nih/
KV Ankush	PES1UG25CS235	Job manager and workers: connections, dispatch, heartbeats, retries	ank/
