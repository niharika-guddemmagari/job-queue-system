"""JACKFRUIT dashboard v2: dashboard-only UI for the existing Mininet job system.

Same wiring as before (manager protocol over mnexec, nih.worker_control, nih.controller).
New: live animated topology, worker STOPPED/OFFLINE detection, labelled graph,
experiment recorder (FIFO vs Least Loaded, 2 vs 1 workers) built from real job records.
"""
import json, os, re, subprocess, time, threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from nih.worker_control import stop_worker, start_worker, WORKERS, host_pid
from nih.controller import get_controller_status

HOST, PORT = "0.0.0.0", 8080
MANAGER_NAMESPACE_PID = int(os.environ.get("MANAGER_NAMESPACE_PID", 6713))  # Existing manager namespace PID; do not restart it.
HEARTBEAT_TIMEOUT = 10.0      # seconds without heartbeat before a worker is shown OFFLINE

CLIENTS = {
    "h1": ("Client 1", "10.0.0.1"),
    "h4": ("Client 2", "10.0.0.4"),
    "h5": ("Client 3", "10.0.0.5"),
    "h6": ("Client 4", "10.0.0.6"),
}
JOB_TYPES = {"sum", "max", "factorial"}

# ---------------------------------------------------------------------------
# TOPOLOGY DRAWING CONFIG  (edit "hosts" and "links" to match your Mininet script)
# hosts: host -> switch it is plugged into.  links: switch <-> switch cables.
# This only affects the drawing and the packet routes, nothing else.
# ---------------------------------------------------------------------------
TOPOLOGY = {
    "switches": ["s1", "s2", "s3", "s4"],
    "links": [["s1", "s2"], ["s2", "s3"], ["s3", "s4"]],
    "hosts": {"h1": "s1", "h2": "s1", "h3": "s2", "h4": "s3", "h5": "s4", "h6": "s4"},
    "info": {
        "h1": {"label": "Manager + Client 1", "ip": "10.0.0.1", "role": "manager"},
        "h2": {"label": "worker-1", "ip": "10.0.0.2", "role": "worker", "worker_id": "worker-1"},
        "h3": {"label": "worker-2", "ip": "10.0.0.3", "role": "worker", "worker_id": "worker-2"},
        "h4": {"label": "Client 2", "ip": "10.0.0.4", "role": "client"},
        "h5": {"label": "Client 3", "ip": "10.0.0.5", "role": "client"},
        "h6": {"label": "Client 4", "ip": "10.0.0.6", "role": "client"},
    },
}
WORKER_HOST_FALLBACK = {"worker-1": "h2", "worker-2": "h3"}
WORKER_PORT_FALLBACK = {"worker-1": 9001, "worker-2": 9002}

# Workers the dashboard stopped on purpose (fallback truth if the port probe cannot run)
STOPPED = set()
STOPPED_LOCK = threading.Lock()
# START/STOP just pressed: worker_id -> (action, time).  Drives the STARTING / STOPPING state.
PENDING = {}
PENDING_SECONDS = 20.0


def socket_code():
    return "\n".join([
        "import sys,json,socket",
        "m=json.loads(sys.stdin.read())",
        "s=socket.create_connection(('10.0.0.1',5000),6)",
        "s.settimeout(6)",
        "s.sendall((json.dumps(m)+'\\n').encode())",
        "d=b''",
        "while b'\\n' not in d:",
        " c=s.recv(65536)",
        " if not c: break",
        " d+=c",
        "s.close()",
        "print(d.split(b'\\n',1)[0].decode())"
    ])


def send_manager(message, namespace_pid=MANAGER_NAMESPACE_PID):
    try:
        p = subprocess.run(
            ["mnexec", "-a", str(namespace_pid), "python3", "-c", socket_code()],
            input=json.dumps(message), capture_output=True, text=True, timeout=10
        )
        if p.returncode:
            return {"status": "error", "message": p.stderr.strip() or "Manager request failed"}
        return json.loads(p.stdout.strip())
    except Exception as e:
        return {"status": "error", "message": str(e)}


def submit_from_client(host, client_id, job_type, payload):
    try:
        pid = host_pid(host)
        if not pid:
            return {"status": "error", "message": "Mininet namespace not found for " + host}
        return send_manager({
            "type": "submit_job", "client_id": client_id,
            "job_type": job_type, "payload": payload
        }, pid)
    except Exception as e:
        return {"status": "error", "message": str(e)}


def to_seconds(v):
    try:
        n = float(v)
    except Exception:
        return None
    return n / 1000.0 if n > 1e12 else n


def worker_target(wid, rec):
    """Work out which Mininet host and TCP port a worker lives on."""
    info = WORKERS.get(wid) if isinstance(WORKERS, dict) else None
    info = info if isinstance(info, dict) else {}
    rec = rec if isinstance(rec, dict) else {}
    host = None
    for cand in (info.get("host"), rec.get("host"), info.get("ip"), rec.get("ip")):
        if not isinstance(cand, str):
            continue
        if re.fullmatch(r"h\d+", cand):
            host = cand; break
        m = re.fullmatch(r"10\.0\.0\.(\d+)", cand)
        if m:
            host = "h" + m.group(1); break
    host = host or WORKER_HOST_FALLBACK.get(wid)
    try:
        port = int(rec.get("port") or info.get("port") or WORKER_PORT_FALLBACK.get(wid))
    except Exception:
        port = None
    return host, port


def is_listening(host, port):
    """True/False = is the worker process listening on its port (read-only, makes no connection).
    None = could not check."""
    if not host or not port:
        return None
    try:
        pid = host_pid(host)
        if not pid:
            return None
        p = subprocess.run(["mnexec", "-a", str(pid), "cat", "/proc/net/tcp", "/proc/net/tcp6"],
                           capture_output=True, text=True, timeout=3)
        if p.returncode:
            return None
        for line in p.stdout.splitlines():
            f = line.split()
            if len(f) > 3 and f[3] == "0A" and ":" in f[1] and int(f[1].rsplit(":", 1)[1], 16) == port:
                return True
        return False
    except Exception:
        return None


def annotate_workers(resp):
    """Effective worker status, in this order:
    STOPPING/STARTING (button just pressed) > STOPPED (port not listening) > manager status,
    with OFFLINE if the manager's heartbeat is stale."""
    if not isinstance(resp, dict):
        return resp
    ws = resp.get("workers")
    if not isinstance(ws, dict):
        ws = resp["workers"] = {}
    now = time.time()
    with STOPPED_LOCK:
        stopped = set(STOPPED)
        pending = {k: v for k, v in PENDING.items() if now - v[1] < PENDING_SECONDS}
        for k in [k for k in PENDING if k not in pending]:
            PENDING.pop(k, None)

    # make sure every known worker is listed, even if the manager dropped it
    have = {(w.get("worker_id", k) if isinstance(w, dict) else k) for k, w in ws.items()}
    try:
        known = list(WORKERS)
    except Exception:
        known = []
    for wid in known:
        if wid not in have:
            ws[wid] = {"worker_id": wid, "status": "unknown", "active_jobs": 0,
                       "jobs_completed": 0, "jobs_failed": 0}

    items = [(k, w) for k, w in ws.items() if isinstance(w, dict)]
    targets = {}
    for k, w in items:
        wid = w.get("worker_id", k)
        w.setdefault("worker_id", wid)
        targets[wid] = worker_target(wid, w)
    with ThreadPoolExecutor(max_workers=max(1, len(targets))) as ex:
        futs = {wid: ex.submit(is_listening, *t) for wid, t in targets.items()}
        alive = {wid: f.result() for wid, f in futs.items()}

    for k, w in items:
        wid = w["worker_id"]
        raw = str(w.get("status", "unknown")).lower()
        w["raw_status"] = raw
        host, port = targets[wid]
        if host and not (isinstance(w.get("host"), str) and w["host"]):
            w["host"] = host
        if not w.get("port") and port:
            w["port"] = port
        w["process_listening"] = alive[wid]
        hb = to_seconds(w.get("last_heartbeat"))
        stale = bool(hb and now - hb > HEARTBEAT_TIMEOUT)
        act = pending.get(wid, (None,))[0]
        up = alive[wid]
        if up is False:
            status = "starting" if act == "start" else "stopped"
        elif up is True:
            if act == "stop":
                status = "stopping"
            elif raw in ("offline", "stopped", "unknown"):
                status = "starting" if act == "start" else "offline"
            else:
                status = "offline" if stale and raw != "busy" else raw
        else:  # could not probe: fall back to what the dashboard did + manager data
            if act == "start":
                status = "starting" if raw in ("offline", "stopped", "unknown") else raw
            elif wid in stopped:
                status = "stopped"
            else:
                status = "offline" if stale else raw
        w["status"] = status
    return resp


def build_state():
    reqs = {"jobs": {"type": "list_jobs"}, "workers": {"type": "list_workers"},
            "events": {"type": "list_events"}, "metrics": {"type": "get_metrics"},
            "manager": {"type": "manager_status"}}
    with ThreadPoolExecutor(max_workers=len(reqs)) as ex:
        futs = {k: ex.submit(send_manager, v) for k, v in reqs.items()}
        out = {k: f.result() for k, f in futs.items()}
    out["workers"] = annotate_workers(out["workers"])
    out["health"] = {"dashboard": "running",
                     "manager_connected": out["manager"].get("status") == "running"}
    with STOPPED_LOCK:
        out["stopped"] = sorted(STOPPED)
    out["timestamp"] = time.time()
    return out


HTML = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>JACKFRUIT | Control Plane</title>
<style>
:root{--bg:#070c16;--p:#111d30;--p2:#15253d;--line:#293d58;--t:#edf4ff;--m:#91a5c1;--c:#55d7e8;--g:#45d6a0;--a:#f3b85e;--r:#ff7185;--b:#75aaff;--v:#a78bfa}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(ellipse at 10% 0%,#193050 0,transparent 36%),var(--bg);color:var(--t);font:14px Segoe UI,Arial,sans-serif}
header{position:sticky;top:0;z-index:5;padding:16px 3%;background:#091321ee;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;align-items:center;gap:12px;backdrop-filter:blur(6px)}
.brand{font-size:19px;font-weight:800;letter-spacing:1.5px}.brand span{color:var(--c)}.sub{font-size:10px;color:var(--m);letter-spacing:1.5px;margin-top:5px}
.status{border:1px solid var(--line);border-radius:30px;padding:9px 13px;color:var(--a);font-size:11px;font-weight:bold}
.live{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--g);margin-right:6px;animation:pulse 1.2s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 #45d6a088}100%{box-shadow:0 0 0 9px #45d6a000}}
main{max-width:1700px;margin:auto;padding:22px 3% 50px}.eyebrow{font-size:10px;color:var(--c);letter-spacing:1.8px;font-weight:bold;margin:2px 0 10px}
h2{font-size:15px;margin:0 0 14px}.section{margin-top:24px}.head{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap}.hint,.muted{font-size:10px;color:var(--m)}
.grid{display:grid;gap:12px}.metrics{grid-template-columns:repeat(6,1fr)}.four{grid-template-columns:repeat(4,1fr)}.two{grid-template-columns:1.15fr .85fr}.twoeq{grid-template-columns:1fr 1fr}.three{grid-template-columns:repeat(3,1fr)}
.metric,.panel,.client{background:linear-gradient(145deg,#122138,#0e192a);border:1px solid var(--line);border-radius:12px;padding:16px;min-width:0;box-shadow:0 8px 25px #0002}
.metric label,.small{display:block;font-size:9px;letter-spacing:1px;color:var(--m)}.value{font-size:26px;font-weight:750;margin-top:9px;font-variant-numeric:tabular-nums}.cyan{color:var(--c)}.green{color:var(--g)}.amber{color:var(--a)}.red{color:var(--r)}.blue{color:var(--b)}
button,select,input,textarea{background:#0a1525;color:var(--t);border:1px solid #334c6b;border-radius:7px;padding:10px;font:inherit;font-size:11px}
button{cursor:pointer;font-weight:bold}button:hover:not(:disabled){border-color:var(--c)}button:disabled{opacity:.35;cursor:not-allowed}button.primary{background:#17436b}button.good{color:var(--g)}button.danger{color:var(--r)}
.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.field{margin-top:12px}.field select,.field input,.field textarea{width:100%}textarea{min-height:70px;resize:vertical;font:12px Consolas,monospace}
.formgrid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.notice{font-size:10px;color:var(--m);margin-top:10px;min-height:16px;word-break:break-word}
.bar{height:7px;background:#263a54;border-radius:8px;margin-top:12px;overflow:hidden}.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--b),var(--c));width:0;transition:width .4s}
.tablewrap{overflow:auto}table{width:100%;border-collapse:collapse;min-width:760px;font-size:10px}th,td{text-align:left;padding:10px 8px;border-bottom:1px solid #263951;vertical-align:top}th{font-size:9px;color:var(--m);letter-spacing:.8px;white-space:nowrap}
tr.sel td{background:#1d3a5c}tr.flash td{animation:fl 1.4s}@keyframes fl{0%{background:#3a6ea8}100%{background:transparent}}
.pill{padding:4px 8px;border-radius:5px;background:#243752;font-size:9px;font-weight:bold;letter-spacing:.5px;text-transform:uppercase}
.pill.completed,.pill.online,.pill.idle,.pill.running.w{color:var(--g);background:#14372f}.pill.queued,.pill.paused{color:var(--a);background:#3c2e19}.pill.running,.pill.busy{color:var(--b);background:#1a3150}.pill.failed,.pill.offline,.pill.stopped{color:var(--r);background:#3b202a}.pill.starting,.pill.stopping{color:var(--a);background:#3c2e19;animation:blink 1s infinite}@keyframes blink{50%{opacity:.45}}
.client{position:relative}.client:before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--c)}.client h3{font-size:13px;margin:0}.clientstats{display:grid;grid-template-columns:repeat(3,1fr);margin-top:14px;gap:4px}.clientstats b{display:block;font-size:19px}.clientstats span{font-size:8px;color:var(--m)}
.chart{height:210px;position:relative}.chart canvas{width:100%;height:100%;cursor:pointer}
.events{max-height:300px;overflow:auto}.event{padding:9px 0;border-bottom:1px solid #263951}.event small{display:block;color:var(--m);font-size:9px;margin:4px 0}.event pre,pre{white-space:pre-wrap;word-break:break-word;background:#091422;border:1px solid #22354e;border-radius:6px;padding:8px;font-size:9px;color:#bfd0e5;max-height:160px;overflow:auto}
details summary{cursor:pointer;color:var(--c)}.flow{padding:10px;background:var(--p2);border:1px solid var(--line);border-radius:8px;margin-top:8px}
.loadbar{display:inline-block;width:46px;height:5px;background:#263a54;border-radius:4px;vertical-align:middle;margin-left:6px;overflow:hidden}.loadbar i{display:block;height:100%;background:var(--b)}
/* topology */
#topo{width:100%;height:auto;display:block;background:radial-gradient(circle at 50% 50%,#0f2036,#0a1423);border:1px solid var(--line);border-radius:10px}
.lnk{stroke:#2f4a6b;stroke-width:2;fill:none;transition:stroke .15s}.lnk.ctl{stroke-dasharray:3 5;stroke:#3b3566;stroke-width:1.2}.lnk.hot{stroke-width:4}.lnk.dead{stroke:#7a2f3d;stroke-dasharray:5 5}
.nd rect,.nd circle.body{fill:#11263b;stroke:#32657b;stroke-width:1.6;transition:all .3s}.nd.sw rect{fill:#1a2340;stroke:#655a9a}.nd.ctrl rect{fill:#241a3d;stroke:var(--v)}
.nd.manager rect{stroke:var(--c);stroke-width:2.4}.nd text{fill:var(--t);font-size:11px;font-weight:700;text-anchor:middle;pointer-events:none}.nd .s2{fill:var(--m);font-size:9px;font-weight:400}.nd .bdg{font-size:9px;font-weight:700;fill:var(--g)}
.nd.busy rect{stroke:var(--b);filter:drop-shadow(0 0 7px #75aaff99)}.nd.idle rect,.nd.online rect{stroke:var(--g)}
.nd.starting rect,.nd.stopping rect{stroke:var(--a);stroke-dasharray:5 4}
.nd.stopped rect,.nd.offline rect{stroke:var(--r);stroke-dasharray:5 4;fill:#2a141c;opacity:.85}.nd.stopped text,.nd.offline text{fill:#e8a0ab}
.nd .cross{display:none}.nd.stopped .cross,.nd.offline .cross{display:block}.cross line{stroke:var(--r);stroke-width:5;stroke-linecap:round}
.nd.ctrl.bad rect{stroke:var(--r)}.nd.ctrl.ok rect{stroke:var(--g)}
.plabel{fill:#fff;font-size:9px;font-weight:700;text-anchor:middle;paint-order:stroke;stroke:#05101c;stroke-width:3px}
.ring{transform-box:fill-box;transform-origin:center;animation:ringa .9s ease-out forwards}@keyframes ringa{from{transform:scale(1);opacity:.9}to{transform:scale(3.4);opacity:0}}
.legend{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px;font-size:10px;color:var(--m)}.legend i{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px;vertical-align:middle}
.legend b{color:var(--t);margin-left:4px}
@media(max-width:1100px){.metrics{grid-template-columns:repeat(3,1fr)}.two,.twoeq{grid-template-columns:1fr}}
@media(max-width:650px){header{position:static}.metrics{grid-template-columns:repeat(2,1fr)}.four{grid-template-columns:1fr 1fr}.formgrid{grid-template-columns:1fr}main{padding:18px 12px}}
#toast{position:fixed;right:18px;bottom:18px;max-width:380px;background:#122a44;border:1px solid var(--c);border-radius:10px;padding:12px 14px;font-size:11px;opacity:0;transform:translateY(10px);transition:.25s;z-index:9;pointer-events:none}#toast.show{opacity:1;transform:none}
</style></head><body>
<header><div><div class="brand">JACKFRUIT <span>/</span> CONTROL PLANE</div><div class="sub">DISTRIBUTED COMPUTING · MININET · OPENFLOW · RYU</div></div><div id="connection" class="status">● CHECKING MANAGER</div></header>
<main>
<div class="eyebrow">01 / SYSTEM OVERVIEW</div><div class="grid metrics">
<div class="metric"><label>TOTAL JOBS</label><div id="total" class="value">—</div></div><div class="metric"><label>QUEUED</label><div id="queued" class="value amber">—</div></div><div class="metric"><label>RUNNING</label><div id="running" class="value blue">—</div></div><div class="metric"><label>COMPLETED</label><div id="completed" class="value green">—</div></div><div class="metric"><label>FAILED</label><div id="failed" class="value red">—</div></div><div class="metric"><label>WORKERS ONLINE</label><div id="online" class="value">—</div></div></div>

<section class="section"><div class="head"><div><div class="eyebrow">02 / LIVE NETWORK TOPOLOGY</div><h2>Job flow across the Mininet network</h2></div>
<div class="toolbar"><label class="hint"><input type="checkbox" id="animOn" checked style="vertical-align:middle"> Animate packets</label></div></div>
<div class="panel"><svg id="topo" viewBox="0 0 960 470"></svg>
<div class="legend"><span><i style="background:#55d7e8"></i>Submit<b id="cSubmit">0</b></span><span><i style="background:#75aaff"></i>Dispatch<b id="cDispatch">0</b></span><span><i style="background:#45d6a0"></i>Result<b id="cResult">0</b></span><span><i style="background:#b7f5dc"></i>Ack to client<b id="cAck">0</b></span><span><i style="background:#f3b85e"></i>Retry<b id="cRetry">0</b></span><span><i style="background:#ff7185"></i>Failure<b id="cFail">0</b></span></div>
<div class="hint" style="margin-top:8px">Each packet is drawn when the manager reports a real job state change (submit → dispatch → result). It is job-level flow, not a wire capture. A red ✕ on a worker means it is STOPPED or OFFLINE.</div></div></section>

<section class="section"><div class="head"><div><div class="eyebrow">03 / CLIENT NETWORK</div><h2>Four independent client monitors</h2></div><span class="hint">Counts derived from actual manager job records</span></div><div class="grid four" id="clients"></div></section>

<section class="section grid two"><div class="panel"><div class="eyebrow">04 / JOB CONSOLE</div><h2>Submit jobs from a Mininet host</h2>
<div class="formgrid"><div><label class="small">CLIENT HOST</label><select id="clientHost"><option value="h1">h1 · Client 1 · 10.0.0.1</option><option value="h4">h4 · Client 2 · 10.0.0.4</option><option value="h5">h5 · Client 3 · 10.0.0.5</option><option value="h6">h6 · Client 4 · 10.0.0.6</option></select></div><div><label class="small">JOB TYPE</label><select id="jobType" onchange="payloadHint()"><option value="sum">Sum</option><option value="max">Maximum</option><option value="factorial">Factorial</option></select></div></div>
<div class="field"><label class="small">JSON PAYLOAD</label><textarea id="payload" spellcheck="false">{"numbers":[5,10,15,20]}</textarea><div class="hint" id="payloadHint">Example: {"numbers":[5,10,15]}</div></div>
<div class="toolbar" style="margin-top:10px"><button class="primary" onclick="submitJob()">＋ SUBMIT JOB</button><button onclick="payloadHint()">RESET EXAMPLE</button><span style="flex:1"></span><input id="burstN" type="number" min="1" max="50" value="6" style="width:60px"><button onclick="burstSubmit()">BURST × N (ALL CLIENTS)</button></div>
<div id="submitNotice" class="notice">The selected host namespace sends the real TCP request; the manager ACK is shown here. Burst rotates through h1, h4, h5, h6 with the same payload, which is handy for repeatable experiments.</div></div>

<div class="panel"><div class="eyebrow">05 / SCHEDULER CONTROL</div><h2>Queue & dispatch</h2><label class="small">SCHEDULING POLICY <span id="policyNow" class="cyan"></span></label><div class="toolbar"><select id="policy"><option value="fifo">FIFO</option><option value="least_loaded">Least Loaded</option></select><button class="primary" onclick="setPolicy()">APPLY</button></div>
<div class="toolbar" style="margin-top:15px"><button onclick="control('/api/dispatch/pause')">PAUSE DISPATCH</button><button class="good" onclick="control('/api/dispatch/resume')">RESUME DISPATCH</button></div><div class="toolbar" style="margin-top:8px"><button onclick="control('/api/queue/pause')">PAUSE QUEUE</button><button class="good" onclick="control('/api/queue/resume')">RESUME QUEUE</button></div><div class="bar"><i id="queueBar"></i></div><div id="queueInfo" class="notice">Queue state loading…</div><div id="controlNotice" class="notice"></div></div></section>

<section class="section"><div class="head"><div><div class="eyebrow">06 / WORKER FLEET</div><h2>Worker lifecycle management</h2></div><span class="hint">STOPPED = stopped from this dashboard · OFFLINE = heartbeat lost</span></div><div class="panel tablewrap"><table><thead><tr><th>WORKER</th><th>HOST</th><th>IP:PORT</th><th>STATUS</th><th>ACTIVE</th><th>COMPLETED</th><th>FAILED</th><th>HEARTBEAT</th><th>CONTROLS</th></tr></thead><tbody id="workers"></tbody></table></div></section>

<section class="section grid two"><div class="panel"><div class="eyebrow">07 / PERFORMANCE ANALYTICS</div><div class="head"><h2 style="margin:0">Measured performance</h2><select id="chartMetric" onchange="draw()"><option value="total">Total latency</option><option value="exec">Execution time</option><option value="wait">Queue wait</option></select></div><div class="grid three"><div><label class="small">AVG TOTAL LATENCY</label><div id="latency" class="value cyan">—</div></div><div><label class="small">AVG EXECUTION</label><div id="execution" class="value">—</div></div><div><label class="small">AVG QUEUE WAIT</label><div id="wait" class="value">—</div></div></div><div class="chart" style="margin-top:14px"><canvas id="latencyChart"></canvas></div><div class="hint">Last 30 completed jobs, each point labelled with its job ID. Click a point to jump to its row in the history table.</div></div>
<div class="panel"><div class="eyebrow">08 / SDN TELEMETRY</div><h2>Ryu · live REST response</h2><div id="sdn" class="muted">Reading controller…</div></div></section>

<section class="section"><div class="panel"><div class="head"><div><div class="eyebrow">09 / EXPERIMENT RECORDER</div><h2>Compare runs using real measured jobs</h2></div>
<div class="toolbar"><input id="runLabel" placeholder="Run label e.g. FIFO · 2 workers" style="width:230px"><button class="primary" id="runStart" onclick="startRun()">▶ START RUN</button><button id="runStop" class="good" onclick="stopRun()" disabled>■ STOP & SAVE</button><button onclick="exportRuns()">EXPORT RUNS CSV</button><button class="danger" onclick="clearRuns()">CLEAR</button></div></div>
<div id="runState" class="notice">How to use: set policy and worker count, press START RUN, submit the same workload (use Burst), wait until jobs finish, press STOP & SAVE. Repeat with the other setting. Only jobs created during the run are counted.</div>
<div class="tablewrap"><table><thead><tr><th>RUN</th><th>POLICY</th><th>WORKERS ONLINE</th><th>DONE / FAILED</th><th>RETRIED</th><th>AVG WAIT</th><th>AVG EXEC</th><th>AVG TOTAL</th><th>THROUGHPUT</th><th>PER WORKER</th></tr></thead><tbody id="runs"></tbody></table></div></div></section>

<section class="section"><div class="head"><div><div class="eyebrow">10 / EXECUTION HISTORY</div><h2>Job records & per-job details</h2></div><div class="toolbar"><input id="jobSearch" placeholder="Filter jobs…" oninput="renderJobs(true)" style="width:150px"><button onclick="exportCSV()">EXPORT CSV</button></div></div><div class="panel tablewrap"><table><thead><tr><th>JOB ID</th><th>CLIENT / SOURCE</th><th>TYPE</th><th>STATUS</th><th>WORKER</th><th>QUEUE WAIT</th><th>EXECUTION</th><th>TOTAL</th><th>ATTEMPT</th><th>DETAILS</th></tr></thead><tbody id="jobs"></tbody></table></div></section>
<section class="section"><div class="panel"><div class="head"><div><div class="eyebrow">11 / EVENT STREAM</div><h2>Live manager events</h2></div><span class="hint">Auto-refresh · 1 second</span></div><div id="events" class="events"></div></div></section>
<div class="notice" style="text-align:center;margin-top:25px">JACKFRUIT · Dashboard-only process · Existing Ryu, Mininet, manager, workers and flows are left untouched.</div>
</main><div id="toast"></div>
<script>
const $=id=>document.getElementById(id);
let TOPO=null,data={jobs:[],workers:{},events:[],metrics:{},manager:{},health:{}},sdn=null,busy=false,appliedPolicy=null,selectedJob=null,lastJobSig="";
const MGR="h1",NS="http://www.w3.org/2000/svg";
const CLIENT_BY_LABEL={"Client 1":"h1","Client 2":"h4","Client 3":"h5","Client 4":"h6"},CLIENT_BY_IP={"10.0.0.1":"h1","10.0.0.4":"h4","10.0.0.5":"h5","10.0.0.6":"h6"};
const C={sub:"#55d7e8",disp:"#75aaff",res:"#45d6a0",ack:"#b7f5dc",fail:"#ff7185",retry:"#f3b85e"};
function esc(v){return String(v??"—").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
function ms(v){return v===null||v===undefined||v===""||isNaN(Number(v))?"—":Number(v).toFixed(2)+" ms";}
function sec(v){let n=Number(v);if(!isFinite(n)||!n)return null;return n>1e12?n/1000:n;}
function clock(v){let n=sec(v);return n===null?(v?esc(v):"—"):new Date(n*1000).toLocaleTimeString();}
function toast(t){let e=$("toast");e.textContent=t;e.classList.add("show");clearTimeout(toast.t);toast.t=setTimeout(()=>e.classList.remove("show"),3200);}
async function api(path,opts={}){let r=await fetch(path,{cache:"no-store",headers:{"Content-Type":"application/json"},...opts}),d=await r.json();if(!r.ok)throw Error(d.message||"HTTP "+r.status);return d;}
function S(tag,attrs,parent){let e=document.createElementNS(NS,tag);for(const k in attrs||{})e.setAttribute(k,attrs[k]);if(parent)parent.appendChild(e);return e;}
function shortId(id){return String(id).slice(-6);}

/* ---------- topology ---------- */
let pos={},adj={},packets=[],hot={},counters={submit:0,dispatch:0,result:0,ack:0,retry:0,fail:0},layer=null;
function buildTopo(){
  const svg=$("topo");svg.innerHTML="";const sw=TOPO.switches,n=sw.length;
  sw.forEach((s,i)=>pos[s]=[n>1?110+i*(740/(n-1)):480,235]);
  const per={};Object.entries(TOPO.hosts).forEach(([h,s])=>{(per[s]=per[s]||[]).push(h);});
  Object.entries(per).forEach(([s,hs])=>hs.forEach((h,j)=>{let top=j%2===0,col=Math.floor(j/2),sx=pos[s][0],cnt=Math.ceil(hs.length/2);pos[h]=[sx+(col-(cnt-1)/2)*95,top?95:375];}));
  pos.ctrl=[480,28];
  [...Object.keys(TOPO.hosts)].forEach(h=>link(h,TOPO.hosts[h]));
  TOPO.links.forEach(([a,b])=>link(a,b));
  sw.forEach(s=>{let l=S("line",{class:"lnk ctl",x1:pos.ctrl[0],y1:pos.ctrl[1]+16,x2:pos[s][0],y2:pos[s][1]-20},svg);});
  sw.forEach(s=>node(s,"sw",s,"OVS · OpenFlow",44,32));
  node("ctrl","ctrl","Ryu Controller","REST 8081",150,34,true);
  Object.keys(TOPO.hosts).forEach(h=>{let i=TOPO.info[h]||{};node(h,"host "+(i.role||""),h+" · "+(i.label||""),i.ip||"",132,44);});
  layer=S("g",{id:"packets"},svg);
  Object.keys(TOPO.hosts).concat(sw).forEach(a=>{adj[a]=adj[a]||[];});
  Object.entries(TOPO.hosts).forEach(([h,s])=>{adj[h].push(s);adj[s].push(h);});
  TOPO.links.forEach(([a,b])=>{adj[a].push(b);adj[b].push(a);});
}
function lid(a,b){return "l-"+[a,b].sort().join("-");}
function link(a,b){S("line",{class:"lnk",id:lid(a,b),x1:pos[a][0],y1:pos[a][1],x2:pos[b][0],y2:pos[b][1]},$("topo"));}
function node(id,cls,t1,t2,w,h,ctl){
  let g=S("g",{class:"nd "+cls,id:"n-"+id,transform:`translate(${pos[id][0]},${pos[id][1]})`},$("topo"));
  S("rect",{x:-w/2,y:-h/2,width:w,height:h,rx:9},g);
  S("text",{y:-2},g).textContent=t1;S("text",{class:"s2",y:11},g).textContent=t2;S("text",{class:"bdg",y:h/2+13},g).id="b-"+id;
  let x=S("g",{class:"cross"},g);S("line",{x1:-w/2+8,y1:-h/2-6,x2:w/2-8,y2:h/2+6},x);S("line",{x1:w/2-8,y1:-h/2-6,x2:-w/2+8,y2:h/2+6},x);
}
function route(a,b){
  if(a===b)return null;let prev={[a]:null},q=[a];
  while(q.length){let u=q.shift();if(u===b)break;for(const v of adj[u]||[])if(!(v in prev)){prev[v]=u;q.push(v);}}
  if(!(b in prev))return null;let names=[],u=b;while(u){names.unshift(u);u=prev[u];}return names;
}
function ring(host,color){if(!pos[host])return;let c=S("circle",{class:"ring",cx:pos[host][0],cy:pos[host][1],r:16,fill:"none",stroke:color,"stroke-width":3},layer);setTimeout(()=>c.remove(),950);}
function fire(src,dst,color,label,delay,kind){setTimeout(()=>launch(src,dst,color,label,kind),delay);}
function launch(src,dst,color,label,kind){
  counters[kind]++;for(const [k,id] of [["submit","cSubmit"],["dispatch","cDispatch"],["result","cResult"],["ack","cAck"],["retry","cRetry"],["fail","cFail"]])$(id).textContent=counters[k];
  if(!$("animOn").checked||!layer)return;
  let names=route(src,dst);if(!names){ring(src,color);return;}
  let pts=names.map(n=>pos[n]),cum=[0];for(let i=1;i<pts.length;i++)cum.push(cum[i-1]+Math.hypot(pts[i][0]-pts[i-1][0],pts[i][1]-pts[i-1][1]));
  let g=S("g",{},layer);S("circle",{r:12,fill:color,opacity:.22},g);S("circle",{r:5,fill:color,stroke:"#fff","stroke-width":1},g);S("text",{class:"plabel",y:-12},g).textContent=label;
  packets.push({g,pts,names,cum,t0:performance.now(),dur:Math.max(800,cum[cum.length-1]*2.4),color,dst,kind});
}
function tick(now){
  packets=packets.filter(p=>{let k=(now-p.t0)/p.dur;if(k>=1){p.g.remove();ring(p.dst,p.color);return false;}
    let e=k<.5?2*k*k:1-Math.pow(-2*k+2,2)/2,L=p.cum[p.cum.length-1],d=e*L,i=1;while(i<p.cum.length-1&&p.cum[i]<d)i++;
    let a=p.pts[i-1],b=p.pts[i],f=(d-p.cum[i-1])/((p.cum[i]-p.cum[i-1])||1);
    p.g.setAttribute("transform",`translate(${a[0]+(b[0]-a[0])*f},${a[1]+(b[1]-a[1])*f})`);
    let id=lid(p.names[i-1],p.names[i]),el=document.getElementById(id);if(el){el.classList.add("hot");el.style.stroke=p.color;hot[id]=now+300;}return true;});
  for(const id in hot)if(now>hot[id]){let el=document.getElementById(id);if(el){el.classList.remove("hot");el.style.stroke="";}delete hot[id];}
  requestAnimationFrame(tick);
}
function workerRec(wid){return Object.values(data.workers||{}).find(w=>(w.worker_id||"")===wid)||(data.workers||{})[wid];}
function workerHost(wid){if(!wid)return null;let r=workerRec(wid);if(r&&r.host&&TOPO.hosts[r.host])return r.host;for(const [h,i] of Object.entries(TOPO.info))if(i.worker_id===wid)return h;return null;}
function clientHost(j){return CLIENT_BY_LABEL[j.client_id]||CLIENT_BY_IP[j.client_ip]||MGR;}
const jf=new Map();let firstLoad=true;
function processJobs(){
  let d=0;const step=()=>{d+=480;return d-480;};
  for(const j of data.jobs){
    const st=String(j.status||"").toLowerCase(),att=Number(j.attempt)||0;let f=jf.get(j.job_id);
    if(!f){f={sub:false,disp:false,res:false,fail:false,att:att};jf.set(j.job_id,f);
      if(firstLoad){f.sub=f.disp=true;f.res=st==="completed";f.fail=st==="failed";continue;}}
    const ch=clientHost(j),wh=workerHost(j.worker_id),lab="#"+shortId(j.job_id);
    if(!f.sub){f.sub=true;fire(ch,MGR,C.sub,lab+" submit",step(),"submit");}
    if(att>f.att&&wh){f.att=att;fire(MGR,wh,C.retry,lab+" retry",step(),"retry");}
    if(!f.disp&&wh&&["running","completed","failed"].includes(st)){f.disp=true;fire(MGR,wh,C.disp,lab+" dispatch",step(),"dispatch");}
    if(!f.res&&st==="completed"){f.res=true;if(wh)fire(wh,MGR,C.res,lab+" result",step(),"result");fire(MGR,ch,C.ack,lab+" ack",step(),"ack");}
    if(!f.fail&&st==="failed"){f.fail=true;if(wh){fire(MGR,wh,C.fail,lab+" FAILED",step(),"fail");setTimeout(()=>ring(wh,C.fail),d);}}
  }
  firstLoad=false;
}
function workerStatus(host){let i=TOPO.info[host]||{};if(i.role!=="worker")return null;let w=workerRec(i.worker_id);return w?String(w.status||"unknown").toLowerCase():"offline";}
function updateTopo(){
  if(!TOPO)return;
  let active={};data.jobs.forEach(j=>{if(["queued","running"].includes(String(j.status).toLowerCase())){let h=clientHost(j);active[h]=(active[h]||0)+1;}});
  let q=data.metrics?.queue?.queue_depth??data.metrics?.queued_jobs??0;
  for(const h of Object.keys(TOPO.hosts)){
    let el=$("n-"+h),b=$("b-"+h),info=TOPO.info[h]||{},role=info.role,ws=workerStatus(h);
    el.setAttribute("class","nd host "+(role||"")+(ws?" "+ws:""));
    if(role==="worker"){let w=workerRec(info.worker_id)||{};b.textContent=ws.toUpperCase()+" · "+(w.active_jobs??0)+" active · "+(w.jobs_completed??0)+" done";
      b.style.fill=ws==="stopped"||ws==="offline"?"#ff7185":ws==="starting"||ws==="stopping"?"#f3b85e":ws==="busy"?"#75aaff":"#45d6a0";
      let l=document.getElementById(lid(h,TOPO.hosts[h]));if(l)l.classList.toggle("dead",ws==="stopped"||ws==="offline");}
    else if(role==="manager"){b.textContent="Queue "+q+" · "+(policyName()||"policy ?")+(data.manager?.dispatch_paused?" · DISPATCH PAUSED":"");b.style.fill="#55d7e8";}
    else{b.textContent=(active[h]||0)+" active jobs";b.style.fill="#91a5c1";}
  }
  let c=$("n-ctrl");c.setAttribute("class","nd ctrl "+(sdn?(sdn.connected?"ok":"bad"):""));$("b-ctrl").textContent=sdn?(sdn.connected?"● "+sdn.switch_count+" switches connected":"● not connected"):"…";$("b-ctrl").style.fill=sdn&&sdn.connected?"#45d6a0":"#ff7185";
  for(const s of TOPO.switches){let el=sdn?.switches?.find(x=>String(x.dpid)===String(parseInt(s.slice(1))));$("b-"+s).textContent=el?el.flow_count+" flows":"";$("b-"+s).style.fill="#a78bfa";}
}

/* ---------- controls ---------- */
function payloadHint(){let t=$("jobType").value;$("payload").value=t==="factorial"?'{"number":5}':'{"numbers":[5,10,15,20]}';$("payloadHint").textContent=t==="factorial"?'Expected example: {"number":5}':'Expected example: {"numbers":[5,10,15]}';}
function getPayload(){let p=JSON.parse($("payload").value);if(!p||Array.isArray(p)||typeof p!=="object")throw Error("Payload must be a JSON object");return p;}
async function submitJob(){let payload;try{payload=getPayload();}catch(e){$("submitNotice").textContent="Invalid JSON: "+e.message;return;}let host=$("clientHost").value;$("submitNotice").textContent="Submitting from "+host+"…";try{let r=await api("/api/submit",{method:"POST",body:JSON.stringify({host,job_type:$("jobType").value,payload})});$("submitNotice").textContent="Manager ACK: "+JSON.stringify(r);await refresh();}catch(e){$("submitNotice").textContent="Submission failed: "+e.message;}}
async function burstSubmit(){let payload;try{payload=getPayload();}catch(e){$("submitNotice").textContent="Invalid JSON: "+e.message;return;}let n=Math.max(1,Math.min(50,+$("burstN").value||6)),hosts=["h1","h4","h5","h6"],ok=0,bad=0;
  for(let i=0;i<n;i++){try{await api("/api/submit",{method:"POST",body:JSON.stringify({host:hosts[i%4],job_type:$("jobType").value,payload})});ok++;}catch(e){bad++;}$("submitNotice").textContent="Burst: "+(i+1)+"/"+n+" sent";await new Promise(r=>setTimeout(r,120));}
  $("submitNotice").textContent="Burst done: "+ok+" accepted, "+bad+" rejected.";toast("Burst finished: "+ok+" jobs submitted");refresh();}
async function control(path){$("controlNotice").textContent="Sending control…";try{let r=await api(path,{method:"POST",body:"{}"});$("controlNotice").textContent=JSON.stringify(r);toast(path.split("/").slice(2).join(" ")+": "+(r.status||"ok"));await refresh();}catch(e){$("controlNotice").textContent="Control failed: "+e.message;}}
async function setPolicy(){try{let p=$("policy").value,r=await api("/api/policy",{method:"POST",body:JSON.stringify({policy:p})});if(r.status!=="error")appliedPolicy=p;$("controlNotice").textContent=JSON.stringify(r);toast("Policy: "+p);await refresh();}catch(e){$("controlNotice").textContent=e.message;}}
async function workerAction(action,id){wbusy[id]=action;renderWorkers();$("controlNotice").textContent=(action==="stop"?"Stopping ":"Starting ")+id+"…";
  try{let r=await api("/api/worker/"+action,{method:"POST",body:JSON.stringify({worker_id:id})});$("controlNotice").textContent=id+": "+JSON.stringify(r);toast(id+(action==="stop"?" stopped":" start requested"));}
  catch(e){$("controlNotice").textContent=id+": "+e.message;toast(id+" "+action+" failed: "+e.message);}
  finally{delete wbusy[id];busy=false;await refresh();}}
function policyName(){let m=data.manager||{},p=m.policy||m.scheduling_policy||m.scheduler_policy||m.scheduler||data.metrics?.policy||data.metrics?.queue?.policy||appliedPolicy;return p?String(p):null;}

/* ---------- renderers ---------- */
function renderClients(){let cfg=[["h1","Client 1","10.0.0.1"],["h4","Client 2","10.0.0.4"],["h5","Client 3","10.0.0.5"],["h6","Client 4","10.0.0.6"]];$("clients").innerHTML=cfg.map(([host,label,ip])=>{let a=data.jobs.filter(j=>j.client_id===label||j.client_ip===ip),done=a.filter(j=>j.status==="completed").length,act=a.filter(j=>["queued","running"].includes(j.status)).length,fail=a.filter(j=>j.status==="failed").length;return `<div class="client"><h3>${host} · ${label}</h3><div class="hint">${ip} · ${a.length} submitted</div><div class="clientstats"><div><b>${act}</b><span>ACTIVE</span></div><div><b class="green">${done}</b><span>COMPLETED</span></div><div><b class="red">${fail}</b><span>FAILED</span></div></div></div>`}).join("");}
function onlineCount(){return Object.values(data.workers||{}).filter(w=>["online","idle","busy","running"].includes(String(w.status).toLowerCase())).length;}
const wbusy={};
function renderWorkers(){let ws=Object.values(data.workers||{});$("online").textContent=onlineCount()+" / "+ws.length;
  $("workers").innerHTML=ws.map(w=>{let s=String(w.status||"unknown").toLowerCase(),id=w.worker_id,act=Number(w.active_jobs)||0,b=wbusy[id],ip=w.ip||(/^\d+\.\d+\.\d+\.\d+$/.test(String(w.host))?w.host:"—");
    if(b)s=b==="stop"?"stopping":"starting";
    return `<tr><td><b>${esc(id)}</b></td><td>${esc(w.host)}</td><td>${esc(ip)}:${esc(w.port)}</td><td><span class="pill ${esc(s)}">${esc(s)}</span></td><td>${act}<span class="loadbar"><i style="width:${Math.min(act*25,100)}%"></i></span></td><td>${esc(w.jobs_completed??0)}</td><td>${esc(w.jobs_failed??0)}</td><td>${s==="stopped"?"—":clock(w.last_heartbeat)}</td><td><div class="toolbar"><button class="danger" ${b?"disabled":""} onclick="workerAction('stop','${esc(id)}')">STOP</button><button class="good" ${b?"disabled":""} onclick="workerAction('start','${esc(id)}')">START</button></div></td></tr>`}).join("")||'<tr><td colspan="9">No worker records</td></tr>';}
function renderJobs(force){let q=($("jobSearch")?.value||"").toLowerCase(),sig=q+"|"+selectedJob+"|"+data.jobs.map(j=>[j.job_id,j.status,j.worker_id,j.total_ms,j.attempt].join(":")).join(",");if(!force&&sig===lastJobSig)return;lastJobSig=sig;
  let a=data.jobs.filter(j=>JSON.stringify([j.job_id,j.client_id,j.client_ip,j.job_type,j.status,j.worker_id]).toLowerCase().includes(q));
  $("jobs").innerHTML=a.slice().reverse().map(j=>{let d={payload:j.payload,result:j.result,error:j.error,history:j.history,created_at:j.created_at,queued_at:j.queued_at,dispatched_at:j.dispatched_at,execution_started_at:j.execution_started_at,completed_at:j.completed_at},s=String(j.status||"unknown").toLowerCase();
    return `<tr id="row-${esc(j.job_id)}" class="${selectedJob===j.job_id?"sel":""}"><td>${esc(j.job_id)}</td><td>${esc(j.client_id)}<br><span class="muted">${esc(j.client_ip)}</span></td><td>${esc(j.job_type)}</td><td><span class="pill ${esc(s)}">${esc(s)}</span></td><td>${esc(j.worker_id)}</td><td>${ms(j.queue_wait_ms)}</td><td>${ms(j.execution_ms)}</td><td>${ms(j.total_ms)}</td><td>${esc(j.attempt??0)}</td><td><details><summary>Inspect</summary><pre>${esc(JSON.stringify(d,null,2))}</pre></details></td></tr>`}).join("")||'<tr><td colspan="10">No matching jobs</td></tr>';}
function renderEvents(){let a=data.events.slice().reverse().slice(0,60);$("events").innerHTML=a.map(e=>`<div class="event"><b>${esc(e.type||"event")}</b><small>${clock(e.timestamp)}</small><pre>${esc(JSON.stringify(e.details||{},null,2))}</pre></div>`).join("")||'<div class="muted">No events recorded</div>';}
function renderSDN(s){if(!s||!s.connected){$("sdn").innerHTML='<span class="red">● Controller not reporting connected switches</span><div class="hint">'+esc(s?.error||"")+'</div>';return;}$("sdn").innerHTML='<span class="green">● Controller responding</span><div class="value">'+esc(s.switch_count)+' <span class="muted">switches</span></div>'+(s.switches||[]).map(x=>`<div class="flow"><b>DPID ${esc(x.dpid)}</b><div class="hint">${esc(x.flow_count)} flow entries · ${(x.ports||[]).length} port-stat records</div></div>`).join("")+`<div class="hint" style="margin-top:10px">Discovered links: ${Array.isArray(s.links)?s.links.length:"unavailable"}</div>`;}

/* ---------- chart with labelled points ---------- */
let chartPts=[];
function draw(){let c=$("latencyChart"),ctx=c.getContext("2d"),r=c.getBoundingClientRect(),dpr=devicePixelRatio||1;c.width=r.width*dpr;c.height=r.height*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);
  let w=r.width,h=r.height,key={total:"total_ms",exec:"execution_ms",wait:"queue_wait_ms"}[$("chartMetric").value],L=44,B=30,T=10,R=12;
  let js=data.jobs.filter(j=>j.status==="completed"&&Number.isFinite(Number(j[key]))).slice(-30);chartPts=[];ctx.clearRect(0,0,w,h);ctx.font="9px Arial";
  if(!js.length){ctx.fillStyle="#91a5c1";ctx.font="11px Arial";ctx.fillText("Completed job timings appear here",14,h/2);return;}
  let vals=js.map(j=>Number(j[key])),max=Math.max(...vals,1)*1.1,pw=w-L-R,ph=h-T-B,gap=pw/Math.max(js.length-1,1);
  ctx.strokeStyle="#293e58";ctx.fillStyle="#91a5c1";ctx.lineWidth=1;for(let i=0;i<=4;i++){let y=T+ph*i/4;ctx.beginPath();ctx.moveTo(L,y);ctx.lineTo(w-R,y);ctx.stroke();ctx.textAlign="right";ctx.fillText((max*(1-i/4)).toFixed(1),L-5,y+3);}
  ctx.beginPath();vals.forEach((v,i)=>{let x=L+i*gap,y=T+ph-v/max*ph;i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.strokeStyle="#55d7e8";ctx.lineWidth=2;ctx.stroke();
  let every=Math.max(1,Math.ceil(30/Math.max(gap,1)));ctx.textAlign="center";
  js.forEach((j,i)=>{let x=L+i*gap,y=T+ph-vals[i]/max*ph,sel=selectedJob===j.job_id;chartPts.push({x,y,j});ctx.beginPath();ctx.arc(x,y,sel?6:3.5,0,7);ctx.fillStyle=sel?"#f3b85e":(j.worker_id==="worker-2"?"#a78bfa":"#75aaff");ctx.fill();
    if(i%every===0||sel){ctx.fillStyle="#91a5c1";ctx.save();ctx.translate(x,h-B+8);ctx.rotate(-.5);ctx.textAlign="right";ctx.fillText(shortId(j.job_id),0,0);ctx.restore();}});
}
function nearestPt(ev){let r=$("latencyChart").getBoundingClientRect(),x=ev.clientX-r.left,y=ev.clientY-r.top,best=null,bd=1e9;for(const p of chartPts){let d=Math.hypot(p.x-x,p.y-y);if(d<bd){bd=d;best=p;}}return bd<16?best:null;}
function selectJob(id){selectedJob=id;renderJobs(true);draw();let row=document.getElementById("row-"+id);if(row){row.scrollIntoView({behavior:"smooth",block:"center"});row.classList.add("flash");}}

/* ---------- experiment recorder ---------- */
let runs=[],activeRun=null;try{runs=JSON.parse(localStorage.getItem("jf_runs")||"[]");}catch(e){}
function saveRuns(){try{localStorage.setItem("jf_runs",JSON.stringify(runs));}catch(e){}}
function avg(a){a=a.map(Number).filter(Number.isFinite);return a.length?a.reduce((x,y)=>x+y,0)/a.length:null;}
function startRun(){if(activeRun)return;activeRun={label:$("runLabel").value.trim()||("Run "+(runs.length+1)),base:new Set(data.jobs.map(j=>j.job_id)),t0:Date.now()/1000,policy:policyName()||"unknown",workers:onlineCount()};$("runStart").disabled=true;$("runStop").disabled=false;renderRunState();toast("Run started: "+activeRun.label);}
function runJobs(){return activeRun?data.jobs.filter(j=>!activeRun.base.has(j.job_id)):[];}
function stopRun(){if(!activeRun)return;let js=runJobs(),done=js.filter(j=>j.status==="completed"),fail=js.filter(j=>j.status==="failed"),pend=js.filter(j=>["queued","running"].includes(j.status)).length;
  let st=js.map(j=>sec(j.created_at)||sec(j.queued_at)).filter(Number.isFinite),en=done.map(j=>sec(j.completed_at)).filter(Number.isFinite),span=st.length&&en.length?Math.max(...en)-Math.min(...st):null;
  let dist={};done.forEach(j=>{dist[j.worker_id]=(dist[j.worker_id]||0)+1;});
  runs.push({label:activeRun.label,policy:activeRun.policy,workers:activeRun.workers,done:done.length,failed:fail.length,retried:js.filter(j=>(Number(j.attempt)||0)>1).length,wait:avg(done.map(j=>j.queue_wait_ms)),exec:avg(done.map(j=>j.execution_ms)),total:avg(done.map(j=>j.total_ms)),thr:span&&span>0?done.length/span:null,dist});
  saveRuns();toast(pend?("Saved. Note: "+pend+" jobs were still unfinished"):"Run saved");activeRun=null;$("runStart").disabled=false;$("runStop").disabled=true;renderRunState();renderRuns();}
function renderRunState(){let e=$("runState");if(!activeRun){e.textContent="How to use: set policy and worker count, press START RUN, submit the same workload (use Burst), wait until jobs finish, press STOP & SAVE. Repeat with the other setting. Only jobs created during the run are counted.";return;}
  let js=runJobs();e.innerHTML='<span class="green">● RECORDING</span> '+esc(activeRun.label)+' · policy '+esc(activeRun.policy)+' · '+activeRun.workers+' workers online · '+js.filter(j=>j.status==="completed").length+'/'+js.length+' jobs completed so far';}
function renderRuns(){$("runs").innerHTML=runs.map(r=>`<tr><td><b>${esc(r.label)}</b></td><td>${esc(r.policy)}</td><td>${r.workers}</td><td>${r.done} / ${r.failed}</td><td>${r.retried}</td><td>${ms(r.wait)}</td><td>${ms(r.exec)}</td><td class="cyan">${ms(r.total)}</td><td>${r.thr==null?"—":r.thr.toFixed(2)+" jobs/s"}</td><td>${Object.entries(r.dist||{}).map(([k,v])=>esc(k)+": "+v).join(" · ")||"—"}</td></tr>`).join("")||'<tr><td colspan="10" class="muted">No saved runs yet</td></tr>';}
function clearRuns(){if(!runs.length||!confirm("Delete all saved runs?"))return;runs=[];saveRuns();renderRuns();}
function dl(name,text){let a=document.createElement("a"),u=URL.createObjectURL(new Blob([text],{type:"text/csv"}));a.href=u;a.download=name;a.click();URL.revokeObjectURL(u);}
function exportRuns(){let cols=["label","policy","workers","done","failed","retried","wait","exec","total","thr"];dl("jackfruit-runs.csv",[cols.join(","),...runs.map(r=>cols.map(k=>JSON.stringify(r[k]??"")).join(",")+","+JSON.stringify(JSON.stringify(r.dist||{})))].join("\n").replace(cols.join(","),cols.join(",")+",per_worker"));}
function exportCSV(){let cols=["job_id","client_id","client_ip","job_type","status","worker_id","queue_wait_ms","execution_ms","total_ms","attempt","created_at","queued_at","dispatched_at","completed_at"];dl("jackfruit-job-history.csv",[cols.join(","),...data.jobs.map(j=>cols.map(k=>JSON.stringify(j[k]??"")).join(","))].join("\n"));}

/* ---------- main loop ---------- */
async function refresh(){if(busy)return;busy=true;try{
  let s=await api("/api/state"),m=s.metrics||{};
  $("connection").innerHTML=s.health?.manager_connected?'<span class="live"></span>MANAGER CONNECTED':"● MANAGER DISCONNECTED";$("connection").style.color=s.health?.manager_connected?"var(--g)":"var(--r)";
  data={jobs:s.jobs?.jobs||[],workers:s.workers?.workers||{},events:s.events?.events||[],metrics:m,manager:s.manager||{},health:s.health||{}};
  $("total").textContent=m.total_jobs??0;$("queued").textContent=m.queued_jobs??0;$("running").textContent=m.running_jobs??0;$("completed").textContent=m.completed_jobs??0;$("failed").textContent=m.failed_jobs??0;
  $("latency").textContent=ms(m.average_latency_ms);$("execution").textContent=ms(m.average_execution_ms);$("wait").textContent=ms(m.average_queue_wait_ms);
  let q=m.queue?.queue_depth??m.queued_jobs??0;$("queueBar").style.width=Math.min(q*10,100)+"%";
  $("queueInfo").textContent=q+" waiting · "+(m.queue?.paused?"queue paused":"queue active")+" · "+(s.manager?.dispatch_paused?"dispatch paused":"dispatch active");
  let pn=policyName();$("policyNow").textContent=pn?"· active: "+pn:"";if(pn&&!refresh.synced&&["fifo","least_loaded"].includes(pn)){$("policy").value=pn;refresh.synced=true;}
  renderClients();renderWorkers();renderJobs();renderEvents();draw();renderRunState();
  if(TOPO){processJobs();updateTopo();}
 }catch(e){$("connection").textContent="● DASHBOARD DATA ERROR";$("connection").style.color="var(--r)";$("controlNotice").textContent=e.message;}finally{busy=false;}}
async function refreshSDN(){try{sdn=await api("/api/sdn");}catch(e){sdn={connected:false,error:e.message};}renderSDN(sdn);updateTopo();}
async function init(){
  try{TOPO=await api("/api/topology");buildTopo();}catch(e){$("controlNotice").textContent="Topology config failed: "+e.message;}
  $("latencyChart").addEventListener("click",ev=>{let p=nearestPt(ev);if(p)selectJob(p.j.job_id);});
  $("latencyChart").addEventListener("mousemove",ev=>{let p=nearestPt(ev);$("latencyChart").title=p?(p.j.job_id+" · "+p.j.worker_id+" · "+Number(p.j[{total:"total_ms",exec:"execution_ms",wait:"queue_wait_ms"}[$("chartMetric").value]]).toFixed(2)+" ms"):"";});
  renderRuns();await refresh();refreshSDN();setInterval(refresh,1000);setInterval(refreshSDN,3000);window.addEventListener("resize",draw);requestAnimationFrame(tick);
}
init();
</script></body></html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store")
        self.end_headers(); self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            body = HTML.encode(); self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store")
            self.end_headers(); self.wfile.write(body); return
        if path == "/api/state":
            self.send_json(build_state()); return
        if path == "/api/topology":
            self.send_json(TOPOLOGY); return
        endpoints = {"/api/jobs": {"type": "list_jobs"}, "/api/events": {"type": "list_events"},
                     "/api/metrics": {"type": "get_metrics"}, "/api/status": {"type": "manager_status"}}
        if path in endpoints: self.send_json(send_manager(endpoints[path])); return
        if path == "/api/workers":
            self.send_json(annotate_workers(send_manager({"type": "list_workers"}))); return
        if path == "/api/health":
            m = send_manager({"type": "manager_status"})
            self.send_json({"dashboard": "running", "manager_connected": m.get("status") == "running",
                            "manager": m, "timestamp": time.time()}); return
        if path == "/api/sdn":
            try: self.send_json(get_controller_status())
            except Exception as e: self.send_json({"connected": False, "error": str(e)})
            return
        self.send_json({"status": "error", "message": "Endpoint not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            n = int(self.headers.get("Content-Length", 0))
            if n > 100000: self.send_json({"status": "error", "message": "Request too large"}, 413); return
            d = json.loads(self.rfile.read(n).decode() if n else "{}")
            if not isinstance(d, dict): raise ValueError()
        except Exception:
            self.send_json({"status": "error", "message": "Invalid JSON"}, 400); return
        if path == "/api/submit":
            host, jtype, payload = d.get("host"), d.get("job_type"), d.get("payload")
            if host not in CLIENTS or jtype not in JOB_TYPES or not isinstance(payload, dict):
                self.send_json({"status": "error", "message": "Invalid host, job type or payload"}, 400); return
            result = submit_from_client(host, CLIENTS[host][0], jtype, payload)
            self.send_json(result, 200 if result.get("status") == "queued" else 502); return
        if path == "/api/policy":
            policy = d.get("policy")
            if policy not in ("fifo", "least_loaded"): self.send_json({"status": "error", "message": "Invalid policy"}, 400); return
            self.send_json(send_manager({"type": "set_policy", "policy": policy})); return
        controls = {"/api/dispatch/pause": "pause_dispatch", "/api/dispatch/resume": "resume_dispatch",
                    "/api/queue/pause": "pause_queue", "/api/queue/resume": "resume_queue"}
        if path in controls: self.send_json(send_manager({"type": controls[path]})); return
        if path in ("/api/worker/stop", "/api/worker/start"):
            wid = d.get("worker_id")
            if wid not in WORKERS: self.send_json({"status": "error", "message": "Unknown worker"}, 400); return
            stopping = path.endswith("/stop")
            try:
                result = stop_worker(wid) if stopping else start_worker(wid)
            except Exception as e:
                self.send_json({"status": "error", "message": str(e)}, 500); return
            failed = isinstance(result, dict) and result.get("status") == "error"
            if not failed:
                with STOPPED_LOCK:
                    (STOPPED.add if stopping else STOPPED.discard)(wid)
                    PENDING[wid] = ("stop" if stopping else "start", time.time())
            self.send_json(result if isinstance(result, dict) else {"status": "ok", "result": result}); return
        self.send_json({"status": "error", "message": "Endpoint not found"}, 404)

    def log_message(self, fmt, *args): print("[DASHBOARD]", fmt % args)


def main():
    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print("JACKFRUIT DASHBOARD: http://localhost:8080")
    print("Uses existing manager namespace PID", MANAGER_NAMESPACE_PID)
    print("Dashboard server only. No Ryu/Mininet/manager/worker restart.")
    try: server.serve_forever()
    except KeyboardInterrupt: print("Dashboard stopped.")
    finally: server.server_close()


if __name__ == "__main__":
    main()
