# distributed-kv-store

[![Go](https://img.shields.io/badge/go-1.23%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](go.mod)

A distributed key-value store written from scratch in Go — a "mini Redis/etcd"
with **zero third-party dependencies**: everything below is standard library.

- **Storage**: an LSM-tree engine — skip-list memtable, fsync'd write-ahead log,
  immutable sorted-string tables, per-segment bloom filters, sparse indexes,
  background compaction.
- **Cluster**: consistent hashing with 150 virtual nodes per physical node,
  configurable replication factor, transparent request proxying.
- **Interfaces**: a JSON REST API, a Redis-like TCP text protocol, and a CLI.

## Contents

- [Quick start](#quick-start)
- [Architecture](#architecture)
  - [Storage engine](#storage-engine)
  - [On-disk formats](#on-disk-formats)
  - [Cluster layer](#cluster-layer)
- [API reference](#api-reference)
- [Configuration](#configuration)
- [Testing and benchmarks](#testing-and-benchmarks)
- [Project layout](#project-layout)
- [Concept map](#concept-map)
- [Limitations](#limitations)
- [License](#license)

## Quick start

Requires Go 1.23+. Nothing else — no database, no broker, no vendored modules.

```bash
git clone https://github.com/sricharanraj/distributed-kv-store.git
cd distributed-kv-store
make test
```

Start a 3-node cluster on localhost (ports 9001/9002/9003, replication factor 2):

```bash
./scripts/run_cluster.sh
```

Write to one node, read it back from a different one — the cluster figures out
who owns the key:

```bash
go run ./cmd/kvctl -addr 127.0.0.1:9001 put user:1 alice
go run ./cmd/kvctl -addr 127.0.0.1:9003 get user:1
go run ./cmd/kvctl -addr 127.0.0.1:9002 status
```

Prefer Docker? `docker compose up --build` brings up the same 3-node cluster on
`localhost:8081/8082/8083`.

Single node instead:

```bash
go run ./cmd/server -http-addr=127.0.0.1:8080 -tcp-addr=127.0.0.1:6380 -data-dir=data
printf 'SET foo bar\r\nGET foo\r\n' | nc 127.0.0.1 6380
```

## Architecture

### Storage engine

Each node embeds an LSM-style engine (`internal/storage`) optimised for
write-heavy workloads: writes never seek, they only append.

```
        writes                              reads
          │                                   │
          ▼                                   ▼
   ┌─────────────┐                    ┌───────────────┐
   │     WAL     │  (durability)      │   memtable    │  checked first
   │  (fsync'd)  │───────────┐        │  (skip list)  │
   └─────────────┘           ▼        └───────┬───────┘
                       ┌─────────────┐        │ miss
                       │  memtable   │        ▼
                       │ (skip list) │  ┌───────────────┐
                       └──────┬──────┘  │ SSTable N     │  bloom filter
                          flush at      │ (newest)      │  → sparse index
                       size threshold   ├───────────────┤  → binary search
                              ▼         │ SSTable N-1   │
                       ┌─────────────┐  ├───────────────┤
                       │   SSTable   │  │      ...      │
                       │  (sorted,   │  └───────────────┘
                       │  immutable) │
                       └─────────────┘
```

**Write path.** A mutation is appended to the WAL and fsync'd *before* it is
applied to the in-memory skip-list memtable, so any acknowledged write survives
a crash. Deletes write a tombstone rather than mutating anything in place. A
single `sync.RWMutex` guards the memtable and segment list — writers serialise,
readers run concurrently.

**Flush.** When the memtable exceeds `-flush-mb` (default 4 MB) it is sorted and
written out as an immutable **SSTable**, together with a bloom filter and a
sparse index. The WAL is then truncated.

**Read path**, and what each step costs:

| Step | Cost |
|---|---|
| 1. Probe the memtable | O(log n) skip-list search, no I/O |
| 2. For each SSTable, newest → oldest: bloom filter check | O(k) bit tests, no I/O; ~1% false-positive rate |
| 3. On a bloom hit: binary-search the sparse index | O(log n) in memory |
| 4. Scan from the nearest checkpoint | ≤ 32 records read from disk |

Reads stop at the first hit — or at the first tombstone, which means "deleted"
rather than "keep looking in older segments".

**Compaction.** Once the segment count crosses `CompactionTrigger` (default 6),
segments are k-way merged into one, keeping only the newest value per key and
dropping tombstones. This bounds how many bloom checks a miss costs and reclaims
space from overwritten and deleted keys.

**Recovery.** On startup the engine loads existing SSTables from the data
directory and replays the WAL to rebuild the memtable, restoring the exact
pre-crash state.

### On-disk formats

Both formats are compact, length-prefixed, little-endian binary — no JSON, no
reflection, no schema library.

```
WAL record       [1B opcode][4B keyLen][key bytes][4B valLen][value bytes]
                    opcode: 1 = put, 2 = delete

SSTable record   [1B tombstone][4B keyLen][key bytes][4B valLen][value bytes]
                    records sorted by key; sparse index checkpoints every 32
```

Bloom filters are sized from the segment's entry count for a 1% target
false-positive rate, using double hashing (Kirsch–Mitzenmacher) over two
independent FNV hashes to synthesise *k* hash functions from two.

### Cluster layer

Nodes learn about each other from a static peer list at startup — deliberately
no gossip — and build a **consistent hash ring** with 150 virtual nodes per
physical node. Adding or removing a node reshuffles roughly `1/N` of the
keyspace instead of nearly all of it, which is what naive `hash(key) % N`
sharding would do.

For a key, `ring.GetN(key, replicationFactor)` returns the ordered owners: the
first is the **primary/coordinator**, the rest are **replicas**.

```
                    consistent hash ring
                 ┌─────────────────────────┐
                 │     node-A (●●●…)       │
      key ───────┼──► owner = first node   │
                 │      clockwise from     │
                 │      hash(key)          │
                 │   node-C        node-B  │
                 └─────────────────────────┘

  write path:
    client ──► any node
                  │
                  ├─ am I an owner for this key? ──yes──► write locally,
                  │                                       async-replicate
                  │                                       to other owners
                  └─ no ──► proxy PUT/DELETE to the primary owner
```

Any node accepts any request. A write to a non-owner is proxied to the primary
owner, which applies it locally and asynchronously pushes it to the remaining
replica owners via an internal `/internal/replicate/{key}` endpoint. Reads to a
non-owner are proxied the same way. The result is tunable replication
(`-replicas=N`) without a consensus protocol — see [Limitations](#limitations)
for exactly what that trade buys and costs.

## API reference

### REST (JSON over HTTP)

| Method | Path | Description |
|---|---|---|
| `PUT` | `/kv/{key}` | Set key's value (request body = raw bytes) |
| `GET` | `/kv/{key}` | Get key's value |
| `DELETE` | `/kv/{key}` | Delete key |
| `GET` | `/kv?prefix=foo` | Scan all live keys with a prefix |
| `GET` | `/cluster/status` | Node ID, known nodes, replication factor |
| `GET` | `/health` | Liveness check |

```bash
curl -X PUT  --data-binary 'alice' http://127.0.0.1:8080/kv/user:1
curl         http://127.0.0.1:8080/kv/user:1
curl         'http://127.0.0.1:8080/kv?prefix=user:'
curl -X DELETE http://127.0.0.1:8080/kv/user:1
```

### TCP (mini-Redis text protocol)

Default port `6380`, CRLF-terminated commands:

```
SET key value   ->  +OK
GET key         ->  $value        ($-1 if missing)
DEL key         ->  +OK
PING            ->  +PONG
```

### CLI

```bash
kvctl [-addr host:port] put <key> <value>
kvctl [-addr host:port] get <key>
kvctl [-addr host:port] del <key>
kvctl [-addr host:port] scan [prefix]
kvctl [-addr host:port] status
```

## Configuration

Server flags (`go run ./cmd/server -h`):

| Flag | Default | Description |
|---|---|---|
| `-http-addr` | `127.0.0.1:8080` | Address for the HTTP REST API |
| `-tcp-addr` | `127.0.0.1:6380` | Address for the TCP text protocol |
| `-node-id` | value of `-http-addr` | This node's ID as peers know it |
| `-peers` | *(empty)* | Comma-separated peer node IDs |
| `-data-dir` | `data` | Directory for WAL + SSTable files |
| `-replicas` | `1` | Replication factor, clamped to the cluster size |
| `-flush-mb` | `4` | Memtable flush threshold, in MB |

## Testing and benchmarks

```bash
make test    # unit + integration tests, with -race
make bench   # storage engine throughput benchmarks
make vet     # go vet ./...
```

Coverage spans skip-list correctness, WAL append/replay/crash recovery, bloom
filter false-positive rate, SSTable write/read and reload from disk, engine
flush/compaction/persistence, hash-ring distribution *and* the
minimal-movement property when a node is removed, plus a multi-node integration
suite that starts real HTTP servers to exercise cross-node proxying and
replication.

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

## Concept map

Where to look if you're here for a specific systems concept:

| Concept | Where it lives |
|---|---|
| Sorted in-memory index | [`internal/storage/skiplist.go`](internal/storage/skiplist.go) |
| Write-ahead log (crash recovery) | [`internal/storage/wal.go`](internal/storage/wal.go) |
| LSM-tree segments (SSTables) | [`internal/storage/sstable.go`](internal/storage/sstable.go) |
| Bloom filters | [`internal/storage/bloom.go`](internal/storage/bloom.go) |
| Compaction | [`internal/storage/engine.go`](internal/storage/engine.go) |
| Read-write locks / concurrency | `sync.RWMutex` in [`Engine`](internal/storage/engine.go) |
| Consistent hashing (sharding) | [`internal/cluster/hashring.go`](internal/cluster/hashring.go) |
| Replication | [`internal/cluster/replication.go`](internal/cluster/replication.go), [`internal/api/http.go`](internal/api/http.go) |
| REST API | [`internal/api/http.go`](internal/api/http.go) |
| TCP protocol (RESP-like) | [`internal/api/tcp.go`](internal/api/tcp.go) |
| Serialization | JSON over HTTP; length-prefixed binary on disk |
| Benchmarking | [`benchmark/engine_bench_test.go`](benchmark/engine_bench_test.go) |
| Unit + integration tests | `*_test.go` throughout, [`test/integration`](test/integration) |

## Limitations

This is a learning and portfolio project, not a production database. The
simplifications below are deliberate — left clean rather than half-implemented:

- **No consensus, no quorum.** Replication is primary-coordinated and
  best-effort (fire-and-forget to replicas). There is no Raft/Paxos, no quorum
  reads or writes, and no vector clocks or last-write-wins reconciliation for
  concurrent writes to the same key via different coordinators. Concurrent
  conflicting writes can therefore diverge across replicas.
- **No failure detection.** Membership is the static list passed at startup; a
  dead node is not evicted from the ring, and failed replication attempts are
  logged but never retried.
- **No compression or protobuf.** Records on disk use the simple
  length-prefixed format above; the wire format is JSON. Swapping in
  protobuf/flatbuffers, or per-block Snappy compression inside SSTables, is a
  natural next step.
- **No streaming range scans.** `Scan(prefix)` materialises and merges all
  segments in memory — fine for admin and debugging, wrong for large datasets.
- **Single-level compaction.** All segments merge into one rather than into a
  tiered or levelled hierarchy, so compaction cost grows with total data size.

## License

MIT — see [LICENSE](LICENSE).
