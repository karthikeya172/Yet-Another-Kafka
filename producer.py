# clients/producer.py - Complete Implementation with Unique Messages
"""
Producer Client - Intelligent Message Producer with Automatic Failover
Meets Project Requirements:
- Leader Discovery via /metadata/leader
- Automatic Failover on connection errors or 403 responses
- Retry Mechanism without data duplication
- Message Production with ACK confirmation
- Each message gets a unique ID to avoid duplicate content
"""

import os
import time
import requests
import sys
import uuid

# Configuration
BROKERS = os.environ.get("BROKERS", "10.242.175.21:5000,10.242.218.198:5001").split(",")
TOTAL_MESSAGES = int(os.environ.get("TOTAL_MESSAGES", "20"))
RETRY_DELAY = float(os.environ.get("RETRY_DELAY", "2"))
MAX_RETRIES_PER_MESSAGE = int(os.environ.get("MAX_RETRIES_PER_MESSAGE", "10"))
STOP_ON_FAILURE = os.environ.get("STOP_ON_FAILURE", "false").lower() == "true"

def discover_leader(brokers):
    """
    Query all brokers to find the current leader.
    
    Returns:
        str: Leader address (host:port) or None if no leader found
    """
    for broker in brokers:
        try:
            url = f"http://{broker}/metadata/leader"
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                leader = response.json().get("leader")
                if leader:
                    print(f"[discover] ✅ Leader found: {leader}")
                    return leader
        except Exception as e:
            # Continue to next broker
            continue
    
    print("[discover] ❌ No leader available")
    return None

def send_message_with_retry(brokers, message):
    """
    Send a single message with automatic retry and leader rediscovery.
    
    Args:
        brokers: List of broker addresses
        message: Message value to send
    
    Returns:
        bool: True if message successfully sent, False otherwise
    """
    retries = 0
    
    while retries < MAX_RETRIES_PER_MESSAGE:
        # Step 1: Discover current leader
        leader = discover_leader(brokers)
        if not leader:
            print(f"[produce] ⚠  No leader available. Retry {retries+1}/{MAX_RETRIES_PER_MESSAGE} in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
            retries += 1
            continue
        
        # Step 2: Try to send message to leader
        try:
            url = f"http://{leader}/produce"
            response = requests.post(url, json={"value": message}, timeout=5)
            
            if response.status_code == 200:
                # SUCCESS - Message acknowledged
                result = response.json()
                print(f"[produce] ✅ Message delivered successfully")
                print(f"[produce] Leader: {leader}, Offset: {result.get('offset')}")
                return True  # Exit and move to next message
                
            elif response.status_code == 403:
                # Leader changed - need to rediscover
                print(f"[failover] ⚠  Leader {leader} rejected request (403 Not Leader)")
                print(f"[failover] Rediscovering leader...")
                time.sleep(RETRY_DELAY)
                retries += 1
                continue  # Retry with new leader
                
            else:
                # Other HTTP error
                print(f"[produce] ⚠  Unexpected status {response.status_code}: {response.text}")
                time.sleep(RETRY_DELAY)
                retries += 1
                continue
                
        except requests.exceptions.ConnectionError:
            # Leader crashed or unreachable
            print(f"[produce] ❌ Connection error to leader {leader}")
            print(f"[failover] Leader unreachable, rediscovering...")
            time.sleep(RETRY_DELAY)
            retries += 1
            continue
            
        except requests.exceptions.Timeout:
            # Request timed out
            print(f"[produce] ⏱  Timeout connecting to leader {leader}")
            print(f"[failover] Retrying...")
            time.sleep(RETRY_DELAY)
            retries += 1
            continue
            
        except Exception as e:
            # Unexpected error
            print(f"[produce] ❌ Unexpected error: {e}")
            time.sleep(RETRY_DELAY)
            retries += 1
            continue
    
    # Exceeded max retries
    print(f"[produce] ❌ Failed to send message after {MAX_RETRIES_PER_MESSAGE} retries")
    return False

def main():
    """
    Main function: Sends messages sequentially with automatic failover.
    Demonstrates zero data loss during leader failure.
    Each message has a unique ID to avoid duplicate content.
    """
    print(f"\n{'='*70}")
    print(f"🚀 YET-ANOTHER-KAFKA PRODUCER")
    print(f"{'='*70}")
    print(f"📊 Configuration:")
    print(f"   Target Messages: {TOTAL_MESSAGES}")
    print(f"   Brokers: {BROKERS}")
    print(f"   Retry Delay: {RETRY_DELAY}s")
    print(f"   Max Retries: {MAX_RETRIES_PER_MESSAGE}")
    print(f"   Stop on Failure: {STOP_ON_FAILURE}")
    print(f"{'='*70}\n")
    
    sent_count = 0
    failed_messages = []
    
    # Send messages sequentially
    for i in range(1, TOTAL_MESSAGES + 1):
        # Create unique message with timestamp and UUID to avoid duplicates
        unique_id = str(uuid.uuid4())[:8]
        timestamp = int(time.time() * 1000)
        message = f"msg-{i}-{timestamp}-{unique_id}"
        
        print(f"\n{'─'*70}")
        print(f"📤 Message {i}/{TOTAL_MESSAGES}: '{message}'")
        print(f"{'─'*70}")
        
        # Attempt to send with automatic retry and failover
        success = send_message_with_retry(BROKERS, message)
        
        if success:
            sent_count += 1
            print(f"✅ SUCCESS - Message '{message}' delivered and acknowledged")
            print(f"📊 Progress: {sent_count}/{TOTAL_MESSAGES} messages sent ({(sent_count/TOTAL_MESSAGES)*100:.1f}%)")
        else:
            # Failed after all retries
            failed_messages.append(message)
            print(f"❌ FAILED - Message '{message}' could not be delivered")
            
            if STOP_ON_FAILURE:
                print("\n⛔ STOP_ON_FAILURE enabled - Stopping producer")
                break
            else:
                print("⏩ Continuing to next message...")
    
    # Final Summary
    print(f"\n{'='*70}")
    print(f"📊 FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"   Total Sent: {sent_count}/{TOTAL_MESSAGES}")
    print(f"   Success Rate: {(sent_count/TOTAL_MESSAGES)*100:.1f}%")
    
    if failed_messages:
        print(f"   ⚠  Failed Messages: {', '.join(failed_messages)}")
    else:
        print(f"   ✅ All messages delivered successfully!")
    
    print(f"{'='*70}\n")
    
    return sent_count == TOTAL_MESSAGES

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠  Producer interrupted by user (Ctrl+C)")
        print("Exiting...\n")
        sys.exit(130)
