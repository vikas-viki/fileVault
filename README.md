# FileVault

A distributed, replicated file storage system built with NestJS, gRPC and Redis.

Files are streamed in, split into fixed-size chunks, content-addressed by
SHA-256, and replicated across multiple storage nodes so the loss of any single
node doesn't lose data.

## Architecture

There are two deployable apps in this monorepo:

| App           | Path                | Role                                                        |
| ------------- | ------------------- | ----------------------------------------------------------- |
| `coordinator` | `apps/coordinator`  | Tracks node liveness/capacity, picks replicas for an upload |
| `node`        | `apps/node`         | Stores file chunks on disk and replicates them to peers     |

Shared code (proto definitions, generated gRPC interfaces, constants, the
chunk-sizer stream) lives in `libs/shared`.

### Coordinator

- **`GET /upload-request?fileSize=<bytes>`** — picks `REPLICATION_COUNT` storage
  nodes for a new upload. It round-robins across alive nodes (via a Redis `INCR`
  counter) and filters by capacity
  (`spaceAvailable - allocatedSpace > fileSize + buffer`), then optimistically
  reserves the space per node in Redis. Returns the ordered replica list.
- **`GET /health`** — liveness probe.
- **gRPC `HeartbeatService.Heartbeat`** — every node calls this every 5s,
  reporting free disk space and how much it has allocated since the last beat.
  Liveness is tracked in a Redis sorted set keyed by expiry timestamp; capacity
  is reconciled against the optimistic reservation made at `upload-request`.

Each node is registered in Redis under its **dialable gRPC address**
(`host:port`), so that address can be used directly as a client target when
building the replica list.

### Node

- **`POST /node/stream`** (multipart, parsed with busboy) — the client uploads a
  file to the first ("entry") node in the replica list returned by the
  coordinator.
- **gRPC `NodeService.StreamChunk`** (client-streaming) — how the entry node
  ships chunks to the other replicas.

## Replication: fan-out

The entry node is the single write path. For each 1 MB chunk it:

1. hashes the chunk (SHA-256),
2. **in parallel** writes its own copy to disk *and* streams the chunk to every
   other replica over gRPC,
3. only pulls the next chunk once all copies for the current one have landed.

The other replicas are **leaves** — they store what they receive and never
forward further.

```
             ┌──────────► node2 (leaf: store)
client ──►  node1 (store) │
             └──────────► node3 (leaf: store)
```

With replication factor 3, the entry node fans out to at most 2 peers, so its
egress is bounded at 2× the file size. Upload latency tracks the *slowest*
replica rather than the *sum* of a relay chain. The upload only succeeds once
**all** replicas have stored and acked; if any replica fails, the whole upload
is aborted.

### Backpressure

Both the inbound upload loop and the outbound replica writes are driven by a
single `for await` over the chunk stream, and every disk write and gRPC
`write()` is awaited before the next chunk is pulled. Because the raw grpc-js
client stream honours flow control, a slow disk or a slow replica propagates
backpressure all the way back to the uploading client's socket — no unbounded
in-memory buffering.

## Running locally

Needs Node.js, pnpm and a running Redis. Put `REDIS_HOST`, `REDIS_PORT` and
`COORDINATOR_PORT` in a root `.env`, then:

```bash
pnpm install && pnpm build

# coordinator — HTTP :3000, gRPC :3001
node dist/apps/coordinator/main.js

# a storage node — unique NODE_ID + ports per node
NODE_ID=node1 GRPC_PORT=4001 port=4000 node dist/apps/node/main.js
```

Start three nodes (distinct `NODE_ID`/`GRPC_PORT`/`port`) to exercise
replication. Chunks land under `~/Documents/fileVault/<NODE_ID>/`. Proto types
are regenerated with `pnpm gen:proto` after editing `libs/shared/src/protos`.

Node config: `NODE_ID` (identity + storage subdir), `GRPC_PORT` (StreamChunk
port, default `4001`), `port` (HTTP upload port, default `4000`). Core tunables
live in `libs/shared/src/helpers/constants.ts` — `REPLICATION_COUNT` (3),
`STREAM_CHUNK_SIZE` (1 MB), `HEARTBEAT_TIMEOUT_SECONDS` (15).

## Roadmap

- [ ] Authentication on the upload/coordinator APIs
- [ ] A metadata database (file → chunk/replica mapping, ownership)
- [ ] Admin dashboard to generate load and observe the cluster at scale
- [ ] Download / read path
- [ ] Topology-aware replica placement (avoid co-locating replicas in one
      failure domain)
