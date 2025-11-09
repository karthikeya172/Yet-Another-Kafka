import os
import json
import time
import threading
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

from broker.common import (
    r, LEADER_KEY, HWM_KEY,
    set_leader_atomic, renew_leader, get_leader,
    release_leader, set_hwm, get_hwm
)

# ---------------- CONFIG ----------------
BROKER_HOST = os.getenv("BROKER_HOST", "0.0.0.0")
BROKER_PORT = int(os.getenv("BROKER_PORT", "5001"))
MY_ADDR = os.getenv("MY_ADDR", "10.242.218.198:5001")

PEER_URL = os.getenv("PEER_URL", "http://10.242.175.21:5000")
REPLICATION_ENDPOINT = "/internal/replicate"

LEASE_TTL_SECONDS = int(os.getenv("LEASE_TTL_SECONDS", "3"))
LEASE_MS = LEASE_TTL_SECONDS * 1000
CHECK_INTERVAL = max(0.5, LEASE_TTL_SECONDS / 3.0)

LOG_DIR = os.getenv("LOG_DIR", "./data/logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "follower.log")

ROLE = "follower"

role_lock = threading.Lock()
log_lock = threading.Lock()

# ---------------- MODELS ----------------
class Record(BaseModel):
    value: str

class ReplicationData(BaseModel):
    offset: int
    value: str

# ---------------- LOGGING ----------------
def ensure_log():
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()

def read_all_lines():
    ensure_log()
    with open(LOG_FILE, "r") as f:
        return f.readlines()

def append_local(value: str) -> int:
    """Append a local entry as leader, assign sequential offset."""
    ensure_log()
    with log_lock:
        lines = read_all_lines()
        next_offset = len(lines)
        with open(LOG_FILE, "a") as f:
            entry = json.dumps({"offset": next_offset, "value": value})
            f.write(entry + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
    return next_offset

def append_replica(offset: int, value: str):
    """Follower replica append, preserving leader offset."""
    ensure_log()
    with log_lock:
        existing_offsets = {int(json.loads(l)["offset"]) for l in read_all_lines() if l.strip()}
        if offset in existing_offsets:
            print(f"[Follower] 🔁 Skipping duplicate offset={offset}")
            return
        with open(LOG_FILE, "a") as f:
            entry = json.dumps({"offset": offset, "value": value})
            f.write(entry + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        print(f"[Follower] ✅ Replicated from leader: offset={offset}, value={value}")

# ---------------- REPLICATION ----------------
def replicate_to_peer(offset: int, value: str, retries=3, backoff=0.5) -> bool:
    """Replicate to peer with correct offset."""
    url = f"{PEER_URL.rstrip('/')}{REPLICATION_ENDPOINT}"
    payload = {"offset": offset, "value": value}
    for attempt in range(retries):
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                return True
        except Exception:
            pass
        time.sleep(backoff * (attempt + 1))
    return False

# ---------------- LEASE MGMT ----------------
def set_role(new_role: str):
    global ROLE
    with role_lock:
        ROLE = new_role

def lease_renewer():
    print(f"[Leader] 🔄 Lease renewer active for {MY_ADDR}")
    while True:
        with role_lock:
            if ROLE != "leader":
                return
        ok = renew_leader(MY_ADDR, LEASE_MS)
        cur = get_leader()
        if not ok or cur != MY_ADDR:
            print("[Leader] ❌ Lost leadership — demoting to follower.")
            release_leader(MY_ADDR)
            set_role("follower")
            threading.Thread(target=follower_watcher, daemon=True).start()
            return
        time.sleep(CHECK_INTERVAL)

def follower_watcher():
    print("[Follower] 👀 Watching for leader...")
    while True:
        with role_lock:
            if ROLE != "follower":
                return
        cur = get_leader()
        if cur is None:
            print("[Follower] 🚨 No leader found — attempting to acquire lease...")
            if set_leader_atomic(MY_ADDR, LEASE_MS):
                print("[Follower] 🏆 Promoted to leader!")
                set_role("leader")
                with log_lock:
                    lines = read_all_lines()
                    current_offset = len(lines) - 1 if lines else -1
                    set_hwm(current_offset)
                    print(f"[Leader] 📊 Starting with HWM={current_offset}")
                threading.Thread(target=lease_renewer, daemon=True).start()
                return
        else:
            pull_and_apply_missing_from_leader(cur)
        time.sleep(CHECK_INTERVAL)

# ---------------- RESYNC ----------------
def pull_and_apply_missing_from_leader(active_leader_addr: str):
    try:
        ensure_log()
        my_lines = read_all_lines()
        existing_offsets = set()
        for l in my_lines:
            try:
                existing_offsets.add(int(json.loads(l)["offset"]))
            except Exception:
                continue
        my_next = (max(existing_offsets) + 1) if existing_offsets else 0
        url = f"http://{active_leader_addr}/log?from_offset={my_next}"
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return False
        lines = r.json().get("lines", [])
        for line in lines:
            try:
                entry = json.loads(line)
                off = int(entry["offset"])
                val = entry.get("value")
            except Exception:
                continue
            if off not in existing_offsets:
                append_replica(off, val)
        return True
    except Exception as e:
        print(f"[Resync] ⚠  Error: {e}")
        return False

# ---------------- FASTAPI ----------------
app = FastAPI(title="YAK Follower (Safe Leader)")

@app.on_event("startup")
def startup():
    global ROLE
    print(f"\n{'='*70}")
    print(f"[Broker] 🚀 Starting on {MY_ADDR}")
    print(f"{'='*70}\n")

    if os.path.exists(LOG_FILE):
        with log_lock:
            lines = read_all_lines()
            print(f"[Broker] Found existing log with {len(lines)} entries.")

    cur = get_leader()
    if cur and cur != MY_ADDR:
        set_role("follower")
        print(f"[Broker] Detected leader {cur} — joining as follower.")
        pull_and_apply_missing_from_leader(cur)
        threading.Thread(target=follower_watcher, daemon=True).start()
        return

    if set_leader_atomic(MY_ADDR, LEASE_MS):
        set_role("leader")
        print(f"[Broker] 🏆 I AM THE LEADER: {MY_ADDR}")
        with log_lock:
            lines = read_all_lines()
            current_offset = len(lines) - 1 if lines else -1
            set_hwm(current_offset)
            print(f"[Leader] 📊 Starting with HWM={current_offset}")
        threading.Thread(target=lease_renewer, daemon=True).start()
    else:
        set_role("follower")
        cur = get_leader()
        print(f"[Broker] Starting as follower. Leader={cur}")
        pull_and_apply_missing_from_leader(cur)
        threading.Thread(target=follower_watcher, daemon=True).start()

@app.on_event("shutdown")
def shutdown_event():
    if ROLE == "leader":
        release_leader(MY_ADDR)
        print(f"[Broker] Released leadership on shutdown ({MY_ADDR})")

@app.post("/produce")
def produce(rec: Record):
    with role_lock:
        if ROLE != "leader":
            raise HTTPException(status_code=403, detail="Not the leader")

    offset = append_local(rec.value)
    replicated = replicate_to_peer(offset, rec.value)

    # ✅ FIX: Only advance HWM if replication succeeded
    if replicated:
        set_hwm(offset)
        print(f"[Leader] ✅ Produced & replicated: offset={offset}, value={rec.value}")
    else:
        print(f"[Leader] ❌ Replication failed, not updating HWM. offset={offset}")

    return {"status": "ok", "offset": offset, "replicated": replicated}

@app.post("/internal/replicate")
def replicate_from_leader(data: ReplicationData):
    append_replica(data.offset, data.value)
    return {"status": "ok"}

@app.get("/log")
def log(from_offset: int = 0):
    ensure_log()
    lines = read_all_lines()
    if from_offset <= 0:
        return {"lines": lines}
    filtered = []
    for l in lines:
        try:
            e = json.loads(l)
            if int(e["offset"]) >= from_offset:
                filtered.append(l)
        except Exception:
            continue
    return {"lines": filtered}

@app.get("/metadata/leader")
def metadata():
    return {"leader": get_leader()}

@app.get("/health")
def health():
    return {"status": "ok", "role": ROLE}

@app.get("/hwm")
def get_high_water_mark():
    try:
        return {"hwm": get_hwm()}
    except Exception:
        raise HTTPException(status_code=500, detail="Unable to fetch HWM")

@app.get("/debug/status")
def debug_status():
    cur_leader = get_leader()
    with log_lock:
        lines = read_all_lines()
        local_offset = len(lines) - 1 if lines else -1
    with role_lock:
        return {
            "my_addr": MY_ADDR,
            "role": ROLE,
            "current_leader_in_redis": cur_leader,
            "local_log_entries": len(lines),
            "local_log_offset": local_offset,
            "hwm": get_hwm()
        }
