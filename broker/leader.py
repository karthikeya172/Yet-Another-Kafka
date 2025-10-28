# broker/leader.py
import os, time, threading, json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
from broker.common import (
    r, LEADER_KEY, LEADER_LEASE_KEY, HWM_KEY,
    set_hwm, get_hwm, force_set_leader, renew_leader, get_leader
)

app = FastAPI()
MY_ADDR = os.environ.get("MY_ADDR", "127.0.0.1:5000")
FOLLOWER_ADDR = os.environ.get("FOLLOWER_ADDR", "127.0.0.1:5001")
LOG_FILE = os.environ.get("LOG_FILE", "leader.log")
LEASE_MS = int(os.environ.get("LEASE_MS", "5000"))
RENEW_INTERVAL = float(os.environ.get("RENEW_INTERVAL", "2.0"))

class Record(BaseModel):
    value: str

# simple append-only log structure: one message per line with offset
def append_log(value: str) -> int:
    # offset = line index starting from 0
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    with open(LOG_FILE, "a+") as f:
        f.seek(0)
        lines = f.readlines()
        offset = len(lines)
        entry = json.dumps({"offset": offset, "value": value})
        f.write(entry + "\n")
    return offset

def replicate_to_follower(offset: int, value: str) -> bool:
    url = f"http://{FOLLOWER_ADDR}/internal/replicate"
    payload = {"offset": offset, "value": value}
    try:
        resp = requests.post(url, json=payload, timeout=5)
        return resp.status_code == 200 and resp.json().get("status") == "ok"
    except Exception as e:
        print("Replication error:", e)
        return False

def lease_renewer():
    # on start, force-set ourselves as leader
    force_set_leader(MY_ADDR, lease_ms=LEASE_MS)
    print("Lease set as leader:", MY_ADDR)
    while True:
        ok = renew_leader(MY_ADDR, lease_ms=LEASE_MS)
        if not ok:
            print("Lease renewal failed; someone else may have claimed leadership.")
            # we can either try to re-acquire or exit. For now, exit to avoid split-brain.
            print("Exiting leader to avoid split-brain.")
            os._exit(1)
        time.sleep(RENEW_INTERVAL)

@app.on_event("startup")
def startup():
    t = threading.Thread(target=lease_renewer, daemon=True)
    t.start()

@app.post("/produce")
def produce(rec: Record):
    # Only leader should accept writes; validate from redis that we are leader
    cur = get_leader()
    if cur != MY_ADDR:
        raise HTTPException(status_code=403, detail="Not the leader")
    # 1. append to local log
    offset = append_log(rec.value)
    # 2. replicate to follower
    replicated = replicate_to_follower(offset, rec.value)
    if not replicated:
        # roll back? For now we keep local log but do not advance HWM.
        return {"status": "error", "reason": "replication_failed"}
    # 3. update HWM in Redis (committed)
    set_hwm(offset)
    return {"status": "ok", "offset": offset}

@app.get("/metadata/leader")
def metadata_leader():
    cur = get_leader()
    return {"leader": cur}

# add this endpoint in both leader.py and follower.py

@app.get("/log")
def get_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            return {"lines": f.readlines()}
    return {"lines": []}

@app.get("/hwm")
def get_hwm_endpoint():
    from broker.common import get_hwm
    return {"hwm": get_hwm()}
