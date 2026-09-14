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
the tradeoffs behind each of those in more depth than this README does.

## Features

- **Real RESP protocol** — inline commands over plain text also work, so you
  can poke at it with `nc localhost 6380` if you don't have `redis-cli`
- **Sharded, thread-safe storage** — 16-way sharded keyspace so unrelated
  keys don't contend on the same lock ([details](docs/DESIGN.md#concurrency-sharded-locks-over-a-single-global-mutex))
- **Key expiration** — both lazy (checked on read) and active (background
  sweep), matching how real Redis reclaims memory ([details](docs/DESIGN.md#expiration-lazy--active-not-just-one))
- **Commands implemented:** `PING`, `ECHO`, `SET` (with `EX` seconds),
  `GET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`

## Architecture

```
┌────────────┐   RESP over TCP    ┌───────────────────────────────┐
│ redis-cli /│ ──────────────────▶│ Server (1 goroutine/connection)│
│ any client │◀────────────────── │        │                       │
└────────────┘                    │        ▼                       │
                                   │  Command dispatcher            │
                                   │        │                       │
                                   │        ▼                       │
                                   │  Sharded store (16 shards,     │
                                   │  RWMutex per shard) ◀───┐      │
                                   │        ▲                │      │
                                   │        │                │      │
                                   │  Active expiry sweep ───┘      │
                                   │  (background goroutine,        │
                                   │   ticks every 100ms)           │
                                   └───────────────────────────────┘
```

## Getting started

Requires Go 1.25+.

```bash
git clone https://github.com/Breyes05/mini-redis-go.git
cd mini-redis-go
make run          # builds and starts the server on :6380
```

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

Run the test suite (includes an end-to-end test that opens a real TCP
connection and exchanges raw RESP bytes):

```bash
make test    # go test ./...
make race    # same, with the race detector
```

## Project layout

```
cmd/server/          entry point (flag parsing, wiring)
internal/resp/        RESP protocol reader + writer
internal/store/       sharded in-memory keyspace with TTL support
internal/server/      TCP server + command dispatch
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

## Roadmap

This was scoped as a multi-weekend project; commands/storage above are
milestone 1. Next up, in order:

- [ ] **Persistence** — append-only file (AOF) writer + replay on startup
- [ ] **Replication** — single leader, N followers, full sync + streamed
      writes, with a reported replication offset
- [ ] **Benchmarks** — throughput/latency numbers via `redis-benchmark`,
      published in this README once measured
- [ ] **LRU eviction** — approximate LRU under a configured max-memory limit

See [docs/DESIGN.md](docs/DESIGN.md) for the reasoning behind what's built
so far and what each of these will involve.

## License

[MIT](LICENSE)
