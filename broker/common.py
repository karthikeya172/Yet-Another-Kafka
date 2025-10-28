# broker/common.py
import os
import redis
import time
import threading
from typing import Optional

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Redis keys
LEADER_KEY = "yak:leader"       # value: "host:port"
LEADER_LEASE_KEY = "yak:leader_lease"  # optional duplicate lease
HWM_KEY = "yak:hwm"             # integer offset (last replicated offset)

def get_leader() -> Optional[str]:
    return r.get(LEADER_KEY)

def set_leader_atomic(candidate: str, lease_ms: int = 5000) -> bool:
    # Attempt to claim leader key only if not exists (SET NX)
    # Then set an expiry for lease semantics (PX)
    return r.set(LEADER_KEY, candidate, nx=True, px=lease_ms)

def renew_leader(candidate: str, lease_ms: int = 5000) -> bool:
    # Only renew if current leader equals candidate (simple check)
    cur = r.get(LEADER_KEY)
    if cur == candidate:
        # reset expiration using PEXPIRE
        r.pexpire(LEADER_KEY, lease_ms)
        return True
    return False

def force_set_leader(candidate: str, lease_ms: int = 5000):
    r.set(LEADER_KEY, candidate, px=lease_ms)

def get_hwm() -> int:
    val = r.get(HWM_KEY)
    return int(val) if val is not None else -1

def set_hwm(offset: int):
    r.set(HWM_KEY, int(offset))
