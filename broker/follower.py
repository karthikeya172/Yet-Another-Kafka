# broker/follower.py
import os, time, threading, json
from fastapi import FastAPI, Request
from broker.common import r, set_leader_atomic, get_leader, force_set_leader, set_hwm, get_hwm
from pydantic import BaseModel

app = FastAPI()
MY_ADDR = os.environ.get("MY_ADDR", "127.0.0.1:5001")
LOG_FILE = os.environ.get("LOG_FILE", "follower.log")
LEASE_MS = int(os.environ.get("LEASE_MS", "5000"))
CHECK_INTERVAL = float(os.environ.get("CHECK_INTERVAL", "2.0"))

class RepRec(BaseModel):
    offset: int
    value: str

def append_log(offset: int, value: str):
    # ensure offsets are appended in order; we will not strictly enforce gaps here
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    with open(LOG_FILE, "a+") as f:
        entry = json.dumps({"offset": offset, "value": value})
        f.write(entry + "\n")

def try_promote():
    # try to become leader with SETNX semantics using force_set_leader
    acquired = set_leader_atomic(MY_ADDR, lease_ms=LEASE_MS)
    if acquired:
        print(f"[{MY_ADDR}] Promoted to leader!")
        # As leader now, best to restart as leader process or flip behavior
        # For simplicity, we just force_set leader so other nodes see us
        force_set_leader(MY_ADDR, lease_ms=LEASE_MS)
        # In a real system you'd change behavior or restart process as leader
        # Here we will just print and continue accepting replicated messages.
        # You can choose to programmatically start leader-specific behavior.
    return acquired

def lease_watcher():
    while True:
        cur = get_leader()
        if cur is None:
            print("No leader found in redis; attempting to acquire lease...")
            if try_promote():
                # once promoted, break out or keep renewing depending on design
                print("Acquired leader via SETNX. Note: follower process remains. For demo it's enough.")
                # after promotion, continue to renew (not implemented) — in practical terms,
                # you might want to restart follower as leader process to run lease renovator.
                # For simplicity, we just set the leader key and continue.
        time.sleep(CHECK_INTERVAL)

@app.on_event("startup")
def startup():
    t = threading.Thread(target=lease_watcher, daemon=True)
    t.start()

@app.post("/internal/replicate")
async def internal_replicate(rec: RepRec):
    # write to local follower log then ACK
    append_log(rec.offset, rec.value)
    # Optionally, after writing, update HWM locally then let leader set HWM.
    return {"status": "ok"}

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
