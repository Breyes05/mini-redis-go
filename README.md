# mini-redis-go

[![CI](https://github.com/Breyes05/mini-redis-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Breyes05/mini-redis-go/actions/workflows/ci.yml)
[![Go Reference](https://img.shields.io/badge/go-1.25-blue)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A Redis-compatible in-memory key-value store, written from scratch in Go —
no `net/rpc`, no existing Redis library, just a TCP server that implements
the real [RESP wire protocol](https://redis.io/docs/latest/develop/reference/protocol-spec/)
so any Redis client can talk to it unmodified.

Built as a systems-programming project to go deep on concurrency, network
protocol parsing, and the storage-engine tradeoffs a real in-memory
database has to make.

```
$ redis-cli -p 6380
127.0.0.1:6380> SET foo bar EX 60
OK
127.0.0.1:6380> GET foo
"bar"
127.0.0.1:6380> TTL foo
(integer) 57
127.0.0.1:6380> DEL foo
(integer) 1
```

## Why this exists

Most "CRUD API" portfolio projects don't say much about how someone thinks
about systems. This one is scoped specifically to surface that: a real
binary protocol parser, a concurrency model that has to actually reason
about lock contention, and a background process (expiry sweeping) running
alongside request handling. The [design notes](docs/DESIGN.md) walk through
the tradeoffs behind each of those in more depth than this README does, and
[engineering notes](docs/ENGINEERING_NOTES.md) collects decisions, bugs
found, and tradeoffs in one skimmable reference.

## Features

- **Real RESP protocol** — inline commands over plain text also work, so you
  can poke at it with `nc localhost 6380` if you don't have `redis-cli`
- **Sharded, thread-safe storage** — 16-way sharded keyspace so unrelated
  keys don't contend on the same lock ([details](docs/DESIGN.md#concurrency-sharded-locks-over-a-single-global-mutex))
- **Key expiration** — both lazy (checked on read) and active (background
  sweep), matching how real Redis reclaims memory ([details](docs/DESIGN.md#expiration-lazy--active-not-just-one))
- **Crash recovery via an append-only file (AOF)** — every mutating command
  is logged to disk (in the same RESP format used on the wire) before the
  client is told it succeeded, and replayed on startup to rebuild state
  ([details](docs/DESIGN.md#persistence-append-only-file))
- **Leader-follower replication** — a follower full-syncs on connect, then
  streams live writes; both sides report a byte-for-byte comparable
  replication offset, the same way real Redis's `FULLRESYNC` handshake
  works ([details](docs/DESIGN.md#replication-leader-follower))
- **Commands implemented:** `PING`, `ECHO`, `SET` (with `EX` seconds),
  `GET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `REPLOFFSET`

## Architecture

```
┌────────────┐   RESP over TCP    ┌────────────────────────────────────┐
│ redis-cli /│ ──────────────────▶│ Leader (1 goroutine/connection)     │
│ any client │◀────────────────── │        │                            │
└────────────┘                    │        ▼                            │
                                   │  Command dispatcher                 │
                                   │        │                            │
                                   │        ▼                            │
                                   │  Sharded store (16 shards,          │
                                   │  RWMutex per shard) ◀───┐           │
                                   │        │  ▲              │           │
                                   │        │  │        Active expiry    │
                                   │        │  └──────  sweep (100ms     │
                                   │        │            background      │
                                   │        │            goroutine)      │
                                   │        ▼                            │
                                   │  propagate: AOF + replication hub   │
                                   │        │              │             │
                                   │        ▼              ▼             │
                                   │  appendonly.aof   Hub.Broadcast     │
                                   │  (disk)           to every          │
                                   │                   connected         │
                                   │                   replica           │
                                   └────────────────────────┬───────────┘
                                                             │ SYNC:
                                                             │ full sync,
                                                             │ then a live
                                                             │ command
                                                             │ stream
                                                             ▼
                              ┌───────────────────────────────────────┐
                              │ Follower — replication.RunFollower     │
                              │ applies each command via the same      │
                              │ dispatch path (read-only to clients)   │
                              └───────────────────────────────────────┘
```

## Getting started

Requires Go 1.25+.

```bash
git clone https://github.com/Breyes05/mini-redis-go.git
cd mini-redis-go
make run          # builds and starts the server on :6380
```

By default the server logs writes to `appendonly.aof` in the working
directory and replays it on startup, so state survives a restart:

```bash
./bin/mini-redis-go -addr :6380 -aof appendonly.aof -fsync everysec
```

- `-aof ""` disables persistence entirely (pure in-memory, like milestone 1)
- `-fsync always` syncs to disk after every write (durable, slower);
  `-fsync everysec` (default) batches syncs once a second, matching Redis's
  own default — see [the tradeoff writeup](docs/DESIGN.md#persistence-append-only-file)

Talk to it with `redis-cli` if you have it installed:

```bash
redis-cli -p 6380 SET foo bar
redis-cli -p 6380 GET foo
```

...or with nothing but `nc`, since inline (plain-text) commands are also
supported:

```bash
nc localhost 6380
PING
+PONG
```

### Running a leader + follower

In one terminal, start a leader as usual. In another, point a second
instance at it with `-replicaof`:

```bash
./bin/mini-redis-go -addr :6380                          # leader
./bin/mini-redis-go -addr :6381 -aof follower.aof -replicaof 127.0.0.1:6380
```

The follower full-syncs immediately, then stays caught up with every write
made against the leader. It still serves reads to ordinary clients, but
rejects writes:

```bash
redis-cli -p 6380 SET foo bar     # OK — on the leader
redis-cli -p 6381 GET foo         # "bar" — replicated
redis-cli -p 6381 SET foo baz     # (error) READONLY You can't write against a read only replica.
redis-cli -p 6380 REPLOFFSET      # bytes of writes broadcast so far
redis-cli -p 6381 REPLOFFSET      # matches the leader's once caught up
```

Run the test suite (includes an end-to-end test that opens a real TCP
connection and exchanges raw RESP bytes):

```bash
make test    # go test ./...
make race    # same, with the race detector
```

## Project layout

```
cmd/server/            entry point (flag parsing, wiring, replay-then-serve)
internal/resp/         RESP protocol reader + writer
internal/store/        sharded in-memory keyspace with TTL support
internal/server/       TCP server + command dispatch
internal/persistence/  append-only file: log writer + startup replay
internal/replication/  leader-side Hub (fan-out) + follower-side sync client
docs/DESIGN.md         deeper design rationale and tradeoffs
```

## Supported commands

| Command | Example | Notes |
|---|---|---|
| `PING` | `PING` | Replies `PONG` |
| `ECHO` | `ECHO hello` | Replies with the given string |
| `SET` | `SET key value [EX seconds]` | Optional TTL |
| `GET` | `GET key` | Nil bulk string if missing/expired |
| `DEL` | `DEL key [key ...]` | Returns count deleted |
| `EXISTS` | `EXISTS key` | `1` or `0` |
| `EXPIRE` | `EXPIRE key seconds` | Sets TTL on an existing key |
| `TTL` | `TTL key` | Seconds left, `-1` if no TTL, `-2` if missing |
| `REPLOFFSET` | `REPLOFFSET` | Bytes of replicated writes processed so far (not a real Redis command) |

`SYNC` is also handled, but as an internal replica handshake rather than a
client command — see [Running a leader + follower](#running-a-leader--follower).

## Roadmap

This was scoped as a multi-weekend project; commands/storage above are
milestone 1. Next up, in order:

- [x] **Persistence** — append-only file (AOF) writer + replay on startup
- [x] **Replication** — single leader, N followers, full sync + streamed
      writes, with a reported replication offset
- [ ] **Benchmarks** — throughput/latency numbers via `redis-benchmark`,
      published in this README once measured
- [ ] **LRU eviction** — approximate LRU under a configured max-memory limit

See [docs/DESIGN.md](docs/DESIGN.md) for the reasoning behind what's built
so far and what each of these will involve.

## License

[MIT](LICENSE)
