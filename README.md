# Yet-Another-Kafka

This project challenges you to build a high-performance, resilient message broker inspired by the core mechanics of platforms like Apache Kafka. 

Project Overview
You will engineer a complete, fault-tolerant message broker that can withstand a catastrophic leader failure with zero data loss. This system focuses directly on the hardest parts of distributed systems: ensuring only one leader exists (consensus), guaranteeing all data is safely replicated, and building intelligent clients that can seamlessly failover when things go wrong.

Your system will consist of:

One Leader Broker: The single authority for all data writes.
One Follower Broker: A hot-standby, ready to take over instantly.
One Metadata Store (Redis): The "source of truth" for who the leader is.
Intelligent Clients: A Producer and Consumer that know how to find the leader, even after a failure.
You will build a system where data is not "committed" until it is safely replicated, and you will prove it by simulating a leader crash and watching your system heal itself.

Objective
Design and implement a distributed message broker focusing on:

Atomic Leader Election using a Redis-based lease mechanism.
Synchronous Data Replication to guarantee that any acknowledged message is durable.
Strict Role Enforcement where only the Leader can accept writes.
High Water Mark (HWM) enforcement to prevent consumers from reading un-replicated data.
Seamless Client Failover where clients automatically discover and switch to the new leader.
