# CLAUDE.md

Guidance for working in this repo. See `README.md` for the architecture.

## Coding conventions

- **Always handle errors** with `try`/`catch` around anything that can throw (I/O, DB, gRPC, network, streams). Fail loudly with a clear message; never swallow silently.
- **Clean code**: small, single-purpose functions; intention-revealing names; no dead code; keep layering intact (see below).
- **Comments**: at most a **single line** directly above the code, and only when it genuinely helps understand non-obvious logic. Default to none. Architecture/rationale goes in `README.md`, not inline.
- **Edge cases**: if a change has edge cases (partial failure, races, empty/oversized input, unavailability of a dependency), call them out explicitly when presenting the work.

## Architectural invariants

Do not violate these without an explicit decision:

- **Nodes are dumb storage** — they never access Postgres. A node stores bytes and *reports* metadata to the coordinator (the commit step). Never add DB access to a node.
- **Coordinator stays out of the file-byte path** — it does metadata/orchestration only. File bytes flow client↔node directly, never through the coordinator.
- **Redis vs Postgres**: Redis holds ephemeral, high-frequency state (node liveness, capacity, counters); Postgres holds durable metadata (users, files). Don't store one's data in the other.
- **Node selection is atomic** — the check-and-reserve of node capacity runs as a single Redis Lua script. Keep it atomic.

## DB access layering

`module → controller → service → repository → model`. Services hold business logic and depend on **repositories**; only the repository imports/queries the Sequelize model (`@InjectModel`) and translates ORM concerns (e.g. return `null` on a unique-violation instead of leaking `UniqueConstraintError`). Services must not touch models directly.

## Verification

Build/typecheck passing is **not** verification — a stale bundle can typecheck while running old code. Before calling something done, exercise the actual runtime path (a real request/upload), not just `tsc`/`build`.

## Config

Required env vars: `JWT_SECRET`, `POSTGRES_*`, `REDIS_*`, and per-node `NODE_ID` / `GRPC_PORT` / `port`. See the config table in `README.md`.

## Protos

After editing `libs/shared/src/protos/*.proto`, run `pnpm gen:proto` to regenerate the TypeScript interfaces.

## Commits

Short subject line + optional brief body. No `Co-Authored-By` trailer. Commit only when asked.

## Building

`nest build` alone only builds the default (coordinator) project. Use `npm run build`, which builds shared + coordinator + node and copies the `.proto` assets. Rebuild the specific app you changed before running it.

## Running the stack

Requires Redis and Postgres running, and a `filevault` database. Start the **coordinator first**, then the nodes (a node crashes if the coordinator is unavailable at startup). Each node needs a unique `NODE_ID` / `GRPC_PORT` / `port`.

**Whenever a run starts any processes, print a table of what is running, where, with ports:**

| Role | PID | HTTP | gRPC | Log |
|------|-----|------|------|-----|
| coordinator | … | 3000 | 3001 | … |
| node1 | … | 4010 | 4001 | … |
| node2 | … | 4020 | 4002 | … |
| node3 | … | 4030 | 4003 | … |

`lsof` shows every process as `node` on macOS, so identify by **port** (3000/3001 = coordinator, 40xx = nodes) or cross-reference the PID with the printed table.

## Known follow-ups

- **Node resilience**: a node crashes (unhandled gRPC rejection) if the coordinator is unavailable — a coordinator restart kills the fleet. It should retry instead.
- **Multipart upload wiring**: `/node/stream` reads `fileId` / `nodesToStream` / `fileSize` via `@Body`, which does not populate for `multipart/form-data`. The real client→node upload needs these passed as parsed form fields (or headers).
