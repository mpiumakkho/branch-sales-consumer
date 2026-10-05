# Scale measurement

How many branches one consumer instance can hold, measured instead of estimated. Everything runs in Docker on one machine with `bench.sh`; the stack is separate from the demo (`infra/`) and is removed with `bench.sh down`.

```bash
cd bench
./bench.sh                       # BROKERS=10 PER_BRANCH=200 LEVELS="100 300 1000" are the defaults
./bench.sh down
```

## What it measures

| Phase | Setup | Numbers |
|---|---|---|
| Load | one branch per broker, each gets `PER_BRANCH` records with different sale dates in one go (a branch's producer after a long outage) | records per second through parse, validation, upsert, receipt and ack; receipts written; rows in `dead_letter` |
| Client | more registry rows pointing at the same brokers until `LEVELS` branches are connected | per level: time for the registry refresh to connect them, heap after a forced GC, threads, TCP connections, resident memory, idle CPU |

The client phase measures only what a connected branch costs in the consumer JVM. The extra branches join the same consumer group on a broker that already has a member, so they hold a consumer, a producer, a thread and connections but receive nothing. That is the idle cost of a branch; a branch with records to read costs that plus the load-phase figure.

## Setup

- Brokers: `apache/kafka:4.3.1`, single node, PLAINTEXT, `-Xmx192m`, generated into `brokers.compose.yml` (git-ignored).
- Consumer: the jar from this repo in the `build` stage of the Dockerfile (a JDK, so `jcmd` can force a GC and read the heap), `BRANCH_KAFKA_SECURITY_PROTOCOL=PLAINTEXT`, registry refresh every 5 s.
- Database: `postgres:18.6-alpine`, migrated by the consumer.

## Differences from production

- No TLS and no SCRAM: a real branch connection adds a TLS handshake per connection and re-authentication every 10 minutes, and `SSLEngine` buffers per connection (tens of KB). The numbers here are a lower bound for memory and connect time.
- Brokers share one machine with the consumer, so throughput includes broker time; a WAN adds latency per record (one receipt send waits for the branch's ack before the next record).
- Several registry rows point at the same broker. In production every branch has its own broker and every connected branch receives records.

## Results (2026-10-05)

Windows 11, Docker Desktop with 8 CPUs and 9.6 GB; consumer JVM OpenJDK 25 `-Xmx2g`; 10 brokers; defaults.

Load phase: 10 branches, 200 records waiting at each.

| Connected in | All 2,000 stored + 2,000 receipts in | Records/s | dead_letter |
|---|---|---|---|
| 10 s (one refresh) | 28 s | 71 over the 28 s, about 110 for the 18 s after connecting; about 11 per branch thread | 0 |

One record costs about 90 ms on a branch's thread: parse, validation, one database transaction, one receipt send and wait for the branch broker's ack. Branches run in parallel, so total throughput grows with the number of branches that have records.

Client phase: branches connected, nothing to read.

| Branches | Connected in | Heap after GC | Threads | TCP connections | RSS | CPU idle |
|---|---|---|---|---|---|---|
| 10 | 10 s | 23 MB | 71 | 60 | 334 MB | 1.5% |
| 100 | 3 s | 45 MB | 342 | 245 | 420 MB | 27% |
| 300 | 4 s | 126 MB | 942 | 649 | 642 MB | 41% |
| 1,000 | 32 s | 387 MB (276 MB a few minutes later) | 3,041 | 2,083 | 1,431 MB | 320% right after connecting, 140–185% once the groups are stable |

Per connected branch: 3 threads (listener, consumer heartbeat, receipt producer sender), 2 TCP connections, about 0.3 MB heap, about 1.1 MB RSS, and about 0.15% of a core while idle. The idle CPU is the fetch long-poll of every consumer (`fetch.max.wait.ms` 500 ms by default), plus the brokers' side of it (the 10 brokers together used about 85% of a core for 1,000 idle consumers). Connecting is sequential inside one registry refresh: 1,000 new rows took 32 s.

What this means for the sizing in requirements §10: one instance holds 1,000 branches in 1.5 GB and 1.5 cores idle, so 2,000 branches would fit in one large instance, but 300–500 branches per instance (`CONSUMER_SHARD`) keeps thread counts, rebalance time and the blast radius of a restart reasonable. A day's records of 2,000 branches (one each) take well under a minute at the measured rate even on one instance.
