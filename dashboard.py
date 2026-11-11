#!/usr/bin/env python3
"""Live dashboard for the Mininet TCP manager through localhost bridge 127.0.0.1:5001."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json, socket, urllib.parse

HOST, PORT = "127.0.0.1", 5001
HTML = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Mininet Job Dashboard</title><style>
*{box-sizing:border-box}body{margin:0;background:#0b1220;color:#e7edf7;font:15px Arial}header{padding:22px 28px;background:#111d30}main{max-width:1050px;margin:auto;padding:20px}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.row{display:grid;grid-template-columns:1fr 1fr;gap:14px}.card{background:#142238;border:1px solid #293c56;border-radius:12px;padding:18px;margin-bottom:15px}.muted{color:#9aabc2}.metric{font-size:23px;font-weight:bold;margin-top:10px}.good{color:#65dfb1}.bad{color:#ff8585}label{display:block;margin:12px 0 6px;color:#9aabc2}input,select{width:100%;padding:11px;background:#0b1525;color:white;border:1px solid #405875;border-radius:7px}button{margin-top:15px;padding:11px 17px;background:#2878cf;color:white;border:0;border-radius:7px;font-weight:bold}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:10px;border-bottom:1px solid #293c56}th{color:#9aabc2}.job{padding:12px 0;border-bottom:1px solid #293c56;overflow-wrap:anywhere}.small{font-size:12px;color:#9aabc2}@media(max-width:700px){.grid,.row{grid-template-columns:1fr}}
</style></head><body><header><h1>◈ Mininet TCP Job Dashboard</h1><div class="muted">Live manager · h1 / h2 / h3 · auto refresh</div></header><main>
<div class="grid"><div class="card"><div class="muted">Manager</div><div id="mgr" class="metric">Checking…</div><div class="small">127.0.0.1:5001 → 10.0.0.1:5000</div></div><div class="card"><div class="muted">worker-1 · h2 · :9001</div><div id="w1" class="metric">Checking…</div></div><div class="card"><div class="muted">worker-2 · h3 · :9002</div><div id="w2" class="metric">Checking…</div></div></div>
<div class="row"><section class="card"><h2>Submit a job</h2><label>Job type</label><select id="kind" onchange="fields()"><option value="sum">Sum range</option><option value="factorial">Factorial</option><option value="max">Maximum</option></select>
<div id="sum"><label>Start</label><input id="start" type="number" value="1"><label>End</label><input id="end" type="number" value="100"></div><div id="fact" hidden><label>n (0–500)</label><input id="n" type="number" value="5"></div><div id="max" hidden><label>Numbers, comma-separated</label><input id="values" value="4,9,2,17"></div><button id="send" onclick="submitJob()">Submit to Mininet</button><p id="msg" class="small">Submits through your existing TCP manager.</p></section>
<section class="card"><h2>Workers</h2><table><thead><tr><th>Worker</th><th>State</th><th>Busy</th></tr></thead><tbody id="workers"><tr><td colspan="3">Waiting…</td></tr></tbody></table></section></div>
<section class="card"><h2>Jobs submitted from dashboard</h2><div id="jobs" class="muted">No jobs submitted yet.</div></section><p id="updated" class="small">Waiting for manager…</p></main>
<script>
const submittedJobs=[];
const el=id=>document.getElementById(id);
async function api(url,data){
  const opts=data?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}:{};
  const r=await fetch(url,opts); const x=await r.json();
  if(!r.ok) throw Error(x.error||('HTTP '+r.status)); return x;
}
function fields(){
  const k=el('kind').value;
  el('sum').hidden=k!=='sum'; el('fact').hidden=k!=='factorial'; el('max').hidden=k!=='max';
}
function esc(s){
  return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function metric(id,t,ok){
  const e=el(id); e.textContent=t; e.className='metric '+(ok?'good':'bad');
}
async function refresh(){
  try{
    const d=await api('/api/workers'), w=d.workers||{};
    metric('mgr','CONNECTED',true);
    el('workers').innerHTML=Object.entries(w).map(([id,x])=>
      `<tr><td>${esc(id)}</td><td class="${x.alive?'good':'bad'}">${x.alive?'ONLINE':'OFFLINE'}</td><td>${x.busy?'YES':'NO'}</td></tr>`
    ).join('')||'<tr><td colspan="3">No registered workers</td></tr>';
    for(const [id,key] of [['w1','worker-1'],['w2','worker-2']])
      metric(id,w[key]?(w[key].alive?'ONLINE':'OFFLINE'):'NOT REGISTERED',!!(w[key]&&w[key].alive));
    el('updated').textContent='Last refresh '+new Date().toLocaleTimeString();
  }catch(e){
    metric('mgr','DISCONNECTED',false);
    el('workers').innerHTML='<tr><td colspan="3">'+esc(e.message)+'</td></tr>';
    metric('w1','Unavailable',false); metric('w2','Unavailable',false);
    el('updated').textContent='Bridge error: '+e.message;
  }
}
async function submitJob(){
  const k=el('kind').value; let payload={};
  if(k==='sum'){
    payload={start:Number(el('start').value),end:Number(el('end').value)};
    if(!Number.isInteger(payload.start)||!Number.isInteger(payload.end)||payload.start>payload.end)
      return alert('Enter valid start/end integers');
  }else if(k==='factorial'){
    payload={n:Number(el('n').value)};
    if(!Number.isInteger(payload.n)||payload.n<0||payload.n>500)return alert('n must be 0 to 500');
  }else{
    payload={values:el('values').value.split(',').map(x=>Number(x.trim()))};
    if(!payload.values.length||payload.values.some(x=>!Number.isFinite(x)))return alert('Enter valid numbers');
  }
  el('send').disabled=true;
  try{
    const d=await api('/api/submit',{type:'submit_job',job_type:k,payload});
    submittedJobs.unshift({id:d.job_id,type:k,status:d.status||'QUEUED'});
    el('msg').textContent='Submitted job '+d.job_id; drawJobs(); pollJobs();
  }catch(e){el('msg').textContent='Submit failed: '+e.message;}
  finally{el('send').disabled=false;}
}
async function pollJobs(){
  for(const j of submittedJobs){
    try{
      const d=await api('/api/status?id='+encodeURIComponent(j.id));
      Object.assign(j,d);
    }catch(e){j.error=e.message;}
  }
  drawJobs();
}
function drawJobs(){
  el('jobs').innerHTML=submittedJobs.length?submittedJobs.map(j=>
    `<div class="job"><b>${esc(j.job_type||j.type)}</b> · ${esc(j.status||'UNKNOWN')}<br>
    <span class="small">ID: ${esc(j.job_id||j.id)} · Worker: ${esc(j.worker_id||'—')}</span><br>
    Result: <b>${j.result===null||j.result===undefined?'—':esc(j.result)}</b>
    ${j.error?'<br>'+esc(j.error):''}</div>`
  ).join(''):'No jobs submitted yet.';
}
fields(); refresh(); setInterval(refresh,3000); setInterval(pollJobs,2000);
</script></body></html>"""

def request_manager(message):
    raw=(json.dumps(message,separators=(",",":"))+"\n").encode()
    with socket.create_connection((HOST,PORT),timeout=5) as s:
        s.settimeout(5);s.sendall(raw);line=s.makefile("r",encoding="utf-8").readline()
        if not line: raise ConnectionError("Bridge closed without manager response")
        return json.loads(line)

class Handler(BaseHTTPRequestHandler):
    def reply(self,obj,status=200):
        b=json.dumps(obj).encode();self.send_response(status);self.send_header("Content-Type","application/json");self.send_header("Content-Length",str(len(b)));self.send_header("Cache-Control","no-store");self.end_headers();self.wfile.write(b)
    def do_GET(self):
        u=urllib.parse.urlparse(self.path)
        if u.path=="/":
            b=HTML.encode();self.send_response(200);self.send_header("Content-Type","text/html; charset=utf-8");self.send_header("Content-Length",str(len(b)));self.end_headers();self.wfile.write(b);return
        try:
            if u.path=="/api/workers": self.reply(request_manager({"type":"list_workers"}))
            elif u.path=="/api/status":
                q=urllib.parse.parse_qs(u.query);jid=q.get("id",[""])[0]
                self.reply(request_manager({"type":"get_status","job_id":jid}))
            else:self.reply({"error":"Not found"},404)
        except Exception as e:self.reply({"error":str(e)},502)
    def do_POST(self):
        try:
            n=int(self.headers.get("Content-Length","0"));m=json.loads(self.rfile.read(n))
            self.reply(request_manager(m))
        except Exception as e:self.reply({"error":str(e)},502)
    def log_message(self,fmt,*args): print("[HTTP]",fmt%args)

if __name__=="__main__":
    print("Live dashboard: http://localhost:8080")
    print("Manager bridge: 127.0.0.1:5001")
    print("Keep this terminal open; Ctrl+C stops dashboard only.")
    ThreadingHTTPServer(("0.0.0.0",8080),Handler).serve_forever()
