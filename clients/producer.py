# clients/producer.py
import requests, time, sys, os
BROKERS = os.environ.get("BROKERS", "127.0.0.1:5000,127.0.0.1:5001").split(",")
TIMEOUT = 3

def get_leader_from_any():
    for b in BROKERS:
        try:
            resp = requests.get(f"http://{b}/metadata/leader", timeout=TIMEOUT)
            leader = resp.json().get("leader")
            if leader:
                return leader
        except:
            pass
    return None

def produce(value: str):
    leader = get_leader_from_any()
    if not leader:
        print("No leader discovered.")
        return False
    try:
        r = requests.post(f"http://{leader}/produce", json={"value": value}, timeout=5)
        if r.status_code == 200:
            print("Produced:", r.json())
            return True
        elif r.status_code == 403:
            # not leader — retry discovery
            print("Broker said not leader; rediscovering...")
            return produce(value)
        else:
            print("Produce failed:", r.text)
            return False
    except Exception as e:
        print("Produce error:", e)
        # attempt to rediscover and retry a few times
        time.sleep(1)
        return produce(value)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python producer.py \"your message\"")
        sys.exit(1)
    produce(" ".join(sys.argv[1:]))
