# broker/common.py
import os
import redis
from typing import Optional

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Redis keys
LEADER_KEY = os.environ.get("LEADER_KEY", "yak:leader")       # value: "host:port"
LEADER_LEASE_KEY = os.environ.get("LEADER_LEASE_KEY", "yak:leader_lease")  # optional duplicate lease
HWM_KEY = os.environ.get("HWM_KEY", "yak:hwm")             # integer offset (last replicated offset)

# Lua script to safely renew a lease: only pexpire when the stored value matches
# the candidate node id. Returns 1 when expiry set, 0 otherwise.
_LUA_RENEW = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
else
  return 0
end
"""

# Lua script to conditionally delete the leader key only if owned by candidate
_LUA_RELEASE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""

def get_leader() -> Optional[str]:
    """Get current leader from Redis."""
    return r.get(LEADER_KEY)

def set_leader_atomic(candidate: str, lease_ms: int = 5000) -> bool:
    """
    Attempt to claim leader key only if not exists (SET NX).
    Then set an expiry for lease semantics (PX).
    Returns True if successfully claimed, False otherwise.
    """
    result = r.set(LEADER_KEY, candidate, nx=True, px=lease_ms)
    return bool(result)

def renew_leader(candidate: str, lease_ms: int = 5000) -> bool:
    """
    Safely renew only if the candidate still owns the lease using an
    atomic Lua compare-and-pexpire. This avoids the small race window of
    doing a GET followed by PEXPIRE in two separate calls.
    Returns True if successfully renewed, False otherwise.
    """
    try:
        res = r.eval(_LUA_RENEW, 1, LEADER_KEY, candidate, lease_ms)
        return bool(res)
    except Exception as e:
        print(f"[Redis] Error renewing leader: {e}")
        return False

def release_leader(candidate: str) -> bool:
    """
    Release leadership only if the current value matches candidate.
    Returns True if the key was deleted, False otherwise.
    """
    try:
        res = r.eval(_LUA_RELEASE, 1, LEADER_KEY, candidate)
        return bool(res)
    except Exception as e:
        print(f"[Redis] Error releasing leader: {e}")
        return False

def force_set_leader(candidate: str, lease_ms: int = 5000):
    """
    Forcefully set leader (overwrites existing).
    Use with caution - breaks lease semantics.
    """
    r.set(LEADER_KEY, candidate, px=lease_ms)

def get_hwm() -> int:
    """Get High Water Mark (last committed offset)."""
    val = r.get(HWM_KEY)
    return int(val) if val is not None else -1

def set_hwm(offset: int):
    """Set High Water Mark."""
    r.set(HWM_KEY, int(offset))
