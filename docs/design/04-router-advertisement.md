How a Router discovers and targets Ingester nodes

The mechanism is intentionally configuration-light so that you can add or remove ingesters without touching client-facing endpoints.
It is made up of two cooperating subsystems:

Sub-system	Purpose	Implementation details
Catalog-backed shard map	Stores authoritative metadata:  • list of live ingester IDs and their gRPC Flight addresses  • which “shard” (hash-range) each ingester owns + replica factor	A Postgres (or Cloud-SQL) database called Catalog.  Every ingester INSERTs a row into ingesters and keeps it fresh with a heartbeat; routers run a read-only watcher that caches the tables in-memory and refreshes every few seconds.  The Helm chart shows the DSN the Router uses to talk to the Catalog  ￼
Gossip membership channel	Provides fast liveness signal and peer addresses without hammering Postgres	Each Router embeds a small member-list (Hashicorp-style) daemon.  Ingester pods join the same gossip ring and broadcast “I’m ingester #42, here’s my Flight URL”.  The Router log filter router::gossip=info that ships in the default values file is a hint of this internal module  ￼



⸻

End-to-end resolution flow
	1.	Startup
	•	Ingester registers in the Catalog and starts sending heartbeats (last_seen column).
	•	It also joins the gossip ring and announces its Flight endpoint.
	2.	Router bootstrap
	•	On launch, the Router performs a full table scan of ingesters, shards, and shard_owners to build its in-memory shard map.
	•	It concurrently subscribes to:
	•	a LISTEN/NOTIFY channel (or periodic polling) on the Catalog tables, and
	•	the gossip stream for low-latency updates.
	3.	Write arrives
	•	Router hashes (org_id, bucket, table) → shard_id.
	•	Looks up shard_id in its local map → list [ingester-A, ingester-B] (replication = 2).
	•	Sends the Arrow batch via gRPC-Flight DoPut to both targets.
	4.	Failure handling
	•	If an ingester stops heart-beating, the Catalog marks it stopped_at; routers evict it on the next refresh.
	•	Gossip dropouts (< 5 s) are treated as soft failures—Router retries the next replica before surfacing back-pressure.
	•	A background shard re-assignment job writes new rows in shard_owners; routers pick them up automatically.

⸻

Why this design scales
	•	Zero client impact – routers, not clients, track the fleet.
	•	Fast switchover – gossip gives sub-second detection; Catalog guarantees eventual consistency.
	•	No thundering herd – only routers query the Catalog; ingesters write once per heartbeat.
	•	Pod-level elasticity – adding an ingester just means: start pod → register → get shards; routers begin routing to it within seconds.

You can further tune discovery by:

Knob	Effect
heartbeat_interval (ingester)	Controls how quickly routers declare a node dead.
router.cache_ttl	Minimum age before a router re-reads the Catalog (0 = each write batch).
replication_factor	Number of ingesters each shard is written to; increases durability at the cost of extra traffic.

In short, the Router learns about ingesters by combining authoritative metadata from the Catalog with real-time gossip liveness, giving both reliability and quick convergence when the ingest tier scales up, rolls, or encounters failures.
