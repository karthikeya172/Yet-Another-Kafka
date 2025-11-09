import os
import time
import json
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

BROKERS = os.environ.get("BROKERS", "10.242.175.21:5000,10.242.218.198:5001").split(",")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2"))
CONNECT_TIMEOUT = int(os.environ.get("CONNECT_TIMEOUT", "3"))
READ_TIMEOUT = int(os.environ.get("READ_TIMEOUT", "5"))

offset = 0
current_leader = None
max_seen_offset = -1
waiting_counter = 0
seen_values = set()   # ✅ deduplication store

def log_info(msg): print(f"✅ {msg}")
def log_warn(msg): print(f"⚠  {msg}")

def discover_leader():
    for b in BROKERS:
        try:
            r = requests.get(f"http://{b}/metadata/leader", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if r.status_code == 200:
                leader = r.json().get("leader")
                if leader:
                    return leader
        except RequestException:
            log_warn(f"Broker {b} unreachable")
    return None

def get_hwm(leader):
    try:
        r = requests.get(f"http://{leader}/hwm", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if r.status_code == 200:
            return int(r.json().get("hwm", -1))
    except RequestException:
        pass
    return -1

def read_log_from_offset(leader, from_offset):
    try:
        r = requests.get(f"http://{leader}/log", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if r.status_code != 200:
            return []
        lines = r.json().get("lines", [])
        msgs = []
        for line in lines:
            try:
                entry = json.loads(line)
                off, val = entry.get("offset"), entry.get("value")
                if off is not None and val is not None and off >= from_offset:
                    msgs.append((off, val))
            except Exception:
                continue
        return msgs
    except (Timeout, ConnectionError):
        log_warn(f"Leader {leader} temporarily unreachable")
        return []
    except Exception as e:
        log_warn(f"Error reading log: {e}")
        return []

def consume_loop():
    global offset, current_leader, max_seen_offset, waiting_counter

    log_info("Consumer started... polling for new messages.")
    print(f"📡 Brokers: {BROKERS}")
    print(f"Starting from offset: {offset}\n")

    while True:
        try:
            leader = discover_leader()
            if not leader:
                log_warn("No leader available. Retrying...")
                time.sleep(POLL_INTERVAL)
                continue

            if leader != current_leader:
                log_info(f"Leader changed: {current_leader} → {leader}")
                current_leader = leader
                waiting_counter = 0

            hwm = get_hwm(leader)
            if hwm < 0:
                log_warn("Could not fetch HWM. Retrying...")
                time.sleep(POLL_INTERVAL)
                continue

            if max_seen_offset >= 0 and hwm < max_seen_offset:
                waiting_counter += 1
                if waiting_counter == 1 or waiting_counter % 5 == 0:
                    log_warn(f"Leader lagging (HWM={hwm}, last_seen={max_seen_offset}). Waiting for catch-up...")
                time.sleep(POLL_INTERVAL)
                continue

            if waiting_counter > 0:
                log_info(f"Leader caught up! Resuming from offset={offset}")
                waiting_counter = 0

            if hwm < offset:
                time.sleep(POLL_INTERVAL)
                continue

            msgs = read_log_from_offset(leader, offset)
            if not msgs:
                time.sleep(POLL_INTERVAL)
                continue

            msgs.sort()
            for off, val in msgs:
                if off >= offset:
                    if val in seen_values:  # ✅ skip duplicate content
                        log_warn(f"[skip-dup] offset={off}: duplicate of earlier message")
                        offset = off + 1
                        continue

                    log_info(f"[consume] offset={off}: {val}")
                    seen_values.add(val)
                    offset = off + 1
                    max_seen_offset = max(max_seen_offset, off)

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\n👋 Consumer shutting down gracefully...")
            break
        except Exception as e:
            log_warn(f"Temporary error: {e}")
            time.sleep(POLL_INTERVAL)
            continue

if __name__ == "__main__":
    consume_loop()