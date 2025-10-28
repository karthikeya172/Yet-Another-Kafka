# clients/consumer.py
import os, requests, time
BROKERS = os.environ.get("BROKERS", "127.0.0.1:5000,127.0.0.1:5001").split(",")

def get_leader():
    for b in BROKERS:
        try:
            r = requests.get(f"http://{b}/metadata/leader", timeout=2)
            leader = r.json().get("leader")
            if leader:
                return leader
        except:
            pass
    return None

def get_hwm():
    # read from redis directly would be nicer; but we will get it via leader
    leader = get_leader()
    if not leader:
        print("No leader found")
        return -1
    try:
        resp = requests.get(f"http://{leader}/hwm", timeout=2)
        return int(resp.json().get("hwm", -1))
    except:
        return -1

def fetch_log_from(addr):
    try:
        r = requests.get(f"http://{addr}/log", timeout=3)
        lines = r.json().get("lines", [])
        return lines
    except:
        return []

if __name__ == "__main__":
    # simple poller
    while True:
        leader = get_leader()
        if not leader:
            print("No leader; sleeping")
            time.sleep(2)
            continue
        hwm = get_hwm()
        print("Leader:", leader, "HWM:", hwm)
        lines = fetch_log_from(leader)
        for ln in lines:
            print("LOG:", ln.strip())
        time.sleep(5)
