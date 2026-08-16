# distributed-kv-store

A distributed key-value store written from scratch in Go — a "mini Redis/etcd."
Single-node storage is an LSM-style engine (skip-list memtable, write-ahead
log, sorted-string-table segments, bloom filters, background compaction);
multiple nodes form a cluster via consistent hashing with configurable
replication. It's exposed over both a JSON REST API and a small Redis-like
TCP text protocol.

This project exists to demonstrate the core building blocks of a real
distributed storage system end to end:

| Concept                          | Where it lives                                  |
|-----------------------------------|--------------------------------------------------|
| Hash maps / sorted in-memory index | [`internal/storage/skiplist.go`](internal/storage/skiplist.go) |
| Write-ahead log (crash recovery)  | [`internal/storage/wal.go`](internal/storage/wal.go) |
| LSM-tree segments (SSTables)      | [`internal/storage/sstable.go`](internal/storage/sstable.go) |
| Bloom filters                     | [`internal/storage/bloom.go`](internal/storage/bloom.go) |
| Compaction                        | [`internal/storage/engine.go`](internal/storage/engine.go) |
| Read-write locks / concurrency    | `sync.RWMutex` in [`Engine`](internal/storage/engine.go) |
| Consistent hashing (sharding)     | [`internal/cluster/hashring.go`](internal/cluster/hashring.go) |
| Replication                       | [`internal/cluster/replication.go`](internal/cluster/replication.go), [`internal/api/http.go`](internal/api/http.go) |
| REST API                          | [`internal/api/http.go`](internal/api/http.go) |
| TCP protocol (RESP-like)          | [`internal/api/tcp.go`](internal/api/tcp.go) |
| Serialization                     | JSON over HTTP; a compact binary record format on disk (WAL/SSTable) |
| Benchmarking                      | [`benchmark/engine_bench_test.go`](benchmark/engine_bench_test.go) |
| Unit + integration tests          | `*_test.go` throughout, [`test/integration`](test/integration) |

## Architecture

### Single-node storage engine

Each node embeds an LSM-style engine (`internal/storage`):

```
        writes                              reads
          │                                   │
          ▼                                   ▼
   ┌─────────────┐                    ┌───────────────┐
   │     WAL     │  (durability)      │   memtable     │  checked first
   │ (fsync'd)   │───────────┐        │  (skip list)   │
   └─────────────┘           ▼        └───────┬────────┘
                       ┌─────────────┐         │ miss
                       │  memtable   │         ▼
                       │ (skip list) │  ┌───────────────┐
                       └──────┬──────┘  │ SSTable N      │  bloom filter
                          flush at      │ (newest)       │  → sparse index
                       size threshold   ├───────────────┤  → binary search
                              ▼         │ SSTable N-1    │
                       ┌─────────────┐  ├───────────────┤
                       │  SSTable    │  │      ...       │
                       │ (sorted,    │  └───────────────┘
                       │  immutable) │
                       └─────────────┘
```

- **Writes** are appended to the WAL (fsync'd for durability) and then applied
  to an in-memory skip-list memtable. A `sync.RWMutex` serializes writers
  while letting reads proceed concurrently.
- When the memtable exceeds `-flush-mb` (default 4MB), it's flushed to an
  immutable, sorted **SSTable** on disk, along with a **bloom filter** (to
  make "key definitely not here" checks cheap) and a sparse index (a
  checkpoint every 32 keys, binary-searched, then a short linear scan) so a
  point lookup does one bloom check + one seek instead of scanning the file.
- **Reads** check the memtable, then SSTables newest → oldest, stopping at
  the first hit (or tombstone).
- **Compaction**: once the number of SSTables crosses a trigger (default 6),
  they're merged into a single segment, keeping only the newest value per key
  and dropping tombstones — bounding how many segments a read has to check
  and reclaiming space from deleted/overwritten keys.
- **Crash recovery**: on startup, the engine loads existing SSTables and
  replays the WAL to rebuit the memtable, so any write that was fsync'd
  before a crash is not lost.

### Cluster layer

Nodes are told about each other via a static peer list at startup (no
gossip protocol — kept simple on purpose) and build a **consistent hash
ring** with 150 virtual nodes per physical node (`internal/cluster/hashring.go`).
This means adding/removing a node only reshuffles roughly `1/N` of the
keyspace instead of nearly all of it, unlike naive `hash(key) % N` sharding.

For a given key, `ring.GetN(key, replicationFactor)` returns the ordered set
of nodes that should hold a copy: the first is the **primary/coordinator**,
the rest are **replicas**.

```
                    consistent hash ring
                 ┌─────────────────────────┐
                 │      node-A (●●●…)       │
      key ───────┼──► owner = first node    │
                 │      clockwise from       │
                 │      hash(key)            │
                 │   node-C          node-B  │
                 └─────────────────────────┘

  write path:
    client ──► any node
                  │
                  ├─ am I an owner for this key? ──yes──► write locally,
                  │                                        async-replicate
                  │                                        to other owners
                  └─ no ──► proxy PUT/DELETE to the primary owner
```

- A **write** to a non-owning node is proxied to the primary owner over
  HTTP; the primary applies it locally and asynchronously pushes it to the
  other replica owners via an internal `/internal/replicate/{key}` endpoint.
- A **read** to a non-owning node is proxied to the primary owner.
- This gives you tunable replication (`-replicas=N`) without needing a
  separate consensus protocol — good enough to demonstrate the mechanics;
  see [Limitations](#limitations--things-a-production-system-would-add) for
  what's intentionally left out.

### APIs

**REST (JSON over HTTP)**

| Method | Path                  | Description                          |
|--------|------------------------|---------------------------------------|
| `PUT`    | `/kv/{key}`           | Set key's value (body = raw bytes)   |
| `GET`    | `/kv/{key}`           | Get key's value                       |
| `DELETE` | `/kv/{key}`           | Delete key                            |
| `GET`    | `/kv?prefix=foo`      | Scan all live keys with a prefix      |
| `GET`    | `/cluster/status`     | Node ID, known nodes, replication factor |
| `GET`    | `/health`              | Liveness check                        |

**TCP (mini-Redis text protocol)**, default port `6380`:

```
SET key value   -> +OK
GET key         -> $value   (or $-1 if missing)
DEL key         -> +OK
PING            -> +PONG
```

## Getting started

Requires Go 1.23+.

```bash
go build ./... 
go test ./... -race
```

### Run a single node

```bash
go run ./cmd/server -http-addr=127.0.0.1:8080 -tcp-addr=127.0.0.1:6380 -data-dir=data
```

```bash
go run ./cmd/kvctl -addr 127.0.0.1:8080 put name "hello world"
go run ./cmd/kvctl -addr 127.0.0.1:8080 get name
go run ./cmd/kvctl -addr 127.0.0.1:8080 scan name
go run ./cmd/kvctl -addr 127.0.0.1:8080 status
```

Or over the TCP protocol:

```bash
printf 'SET foo bar\r\nGET foo\r\n' | nc 127.0.0.1 6380
```

### Run a 3-node cluster locally

```bash
./scripts/run_cluster.sh
```

This starts three nodes on `127.0.0.1:9001/9002/9003` with replication
factor 2. Write to any node and read it back from any other:

```bash
go run ./cmd/kvctl -addr 127.0.0.1:9001 put user:1 alice
go run ./cmd/kvctl -addr 127.0.0.1:9003 get user:1   # -> alice, proxied/replicated
```

### Run with Docker Compose

```bash
docker compose up --build
```

Brings up a 3-node cluster reachable at `localhost:8081/8082/8083`.

## Testing & benchmarks

```bash
make test    # unit + integration tests, race detector
make bench   # storage engine throughput benchmarks
```

Test coverage includes: skip-list correctness, WAL append/replay/crash
recovery, bloom filter false-positive rate, SSTable read/write and reload
from disk, engine flush/compaction/persistence, consistent-hash-ring
distribution and minimal-movement-on-node-removal, and a multi-node
integration suite that spins up real HTTP servers to verify cross-node
proxying and replication.

## Project layout

```
cmd/
  server/       # node entrypoint: wires storage + cluster + API together
  kvctl/        # CLI client for the REST API
internal/
  storage/      # skip list, WAL, bloom filter, SSTable, engine, compaction
  cluster/      # consistent hash ring, membership, replication
  api/          # REST (HTTP) and TCP protocol servers
test/
  integration/  # multi-node cluster tests
benchmark/      # engine throughput benchmarks
scripts/
  run_cluster.sh
```

## Limitations / things a production system would add

This is a learning/portfolio project, not a production database. Notable
simplifications, kept intentionally simple rather than half-implemented:

- **No consensus / leaderless conflict resolution.** Replication is
  primary-coordinated and best-effort (fire-and-forget to replicas); there's
  no Raft/Paxos, no quorum reads/writes, and no vector clocks or
  last-write-wins conflict resolution across concurrent writes to the same
  key from different coordinators.
- **No gossip-based failure detection.** Membership is a static list passed
  at startup; a dead node isn't automatically evicted from the hash ring.
  Failed replication attempts are logged but not retried.
- **No compression or protobuf.** On-disk records use a simple length-prefixed
  binary format; over the wire, JSON. Swapping in protobuf/flatbuffers or
  block compression (e.g. Snappy per SSTable block) is a natural follow-up.
- **No streaming range scans.** `Scan(prefix)` materializes and merges all
  segments into memory, fine for admin/debug use but not for huge datasets.

## License

MIT — see [LICENSE](LICENSE).
