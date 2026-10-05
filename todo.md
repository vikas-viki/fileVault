# FileVault — System-Wide Master Development Checklist

## 1. Shared Infrastructure & Database Layer
- [ ] **Database Schema & AWS RDS Proxy** — Design Postgres tables for objects, bin files, and chunk offsets, connecting through AWS RDS Proxy for connection pooling[cite: 1].
- [D] **Redis State & Caching** — Set up Redis for session management, node availability heartbeats, and storage node capacity tracking[cite: 1].
- [ ] **Observability Pipeline** — Configure Grafana dashboards to monitor node disk I/O, network stream throughput, and worker queue metrics[cite: 1].

## 2. API Gateway & Pre-signed URL Service
- [D] **Pre-signed Upload URL Generator** — Implement Gateway endpoints to issue pre-signed upload URLs routing direct client streams to assigned storage nodes[cite: 1].
- [D] **Pre-signed Download URL Generator** — Implement Gateway endpoints to generate pre-signed download URLs for direct node reads[cite: 1].
- [D] **File Metadata Ingestion** — Expose REST/gRPC routes on the Gateway to register object creation and track session lifecycle state[cite: 1].

## 3. Web Client Capabilities
- [ ] **Client-Side File Chunking** — Build browser-side chunking logic for large files before transmission[cite: 1].
- [ ] **Intelligent Retry Mechanism** — Implement exponential backoff and payload retry logic for failed chunk transmissions[cite: 1].
- [ ] **Multi-part File Metadata Tracking** — Attach headers and metadata to correlate streaming chunks with their parent file ID[cite: 1].

## 4. Primary Storage Node Ingestion & Bin Engine
- [D] **Stream Normalization** — Pipe raw client streams through `StreamChunkSizerService` to yield uniform 64 KB network buffers.
- [D] **Pre-Mutation Validation** — Check file byte bounds (`remainingBytes - chunkLength + BUFFER_STREAM_SIZE >= 0`) prior to mutating state.
- [D] **5 MB Bin Boundary Splitting** — Detect container limits (`length + chunkLength > STORAGE_CHUNK_SIZE`), slice buffers, and write head/tail segments across bin files.
- [D] **Zero-Copy Disk Allocation** — Write chunks directly to allocated 5 MB container bin files on local disk using `Buffer.subarray` and offset handles.

## 5. gRPC Replication & Quorum Protocol (W=2 / N=3)
- [D] **Non-Blocking Channel Setup** — Connect to replica nodes via `Promise.allSettled` and run an initial quorum availability check ($W \ge 2$).
- [D] **Parallel Fanout Streaming** — Dispatch chunks concurrently to active replica streams using `Promise.allSettled`.
- [D] **Dead Node Filtering** — Maintain `failedStreamNodes` to exclude failed sockets from subsequent `r.write()` calls mid-stream.
- [D] **Mid-Stream Quorum Enforcement** — Instantly abort uploads if `failedStreamNodes.size > REPLICATION_COUNT - MIN_FILE_REPLICATION`.
- [D] **Safe Stream Teardown** — Safely close healthy gRPC streams using `Promise.allSettled(relays.map(r => r.end()))`.

## 6. Replica Node Processing (`processNodeStream`)
- [D] **Zero-Copy Stream Ingestion** — Process raw gRPC buffers using zero-copy `Buffer.from(_chunk.buffer, _chunk.byteOffset, _chunk.byteLength)`.
- [D] **Defensive Boundary Splitting** — Handle boundary crossings (`length + chunkLength > STORAGE_CHUNK_SIZE`) defensively to prevent file overrun if upstream boundaries shift.
- [D] **Replica Metadata Tracking** — Write chunk replica locations and offsets (`writeChunkReplicaToDb`) to local database models.

## 7. Read & Download Engine
- [ ] **Pre-Signed Token Authentication** — Validate pre-signed download request tokens directly on storage nodes[cite: 1] (to be done with a proxy that does this).
- [D] **Parallel Range Assembly** — Resolve chunk offsets from DB and read byte ranges from 5 MB bin files into an outgoing HTTP stream.
- [NOT_DOING_FE_TO_DO_UPLOAD_WITH_NEW_OFFSET_ON_FAIL] **Read Failover** — Fail over to a healthy replica node seamlessly if a chunk read fails on the primary node.

## 8. Async Background Workers (AWS EventBridge + SQS + Cron Worker)
- [ ] **AWS EventBridge Scheduler** — Configure cron triggers in EventBridge to publish periodic task events to SQS[cite: 1].
- [ ] **SQS Worker Ingestion** — Implement SQS message consumers in the Cron Worker with visibility timeout and DLQ policies[cite: 1].
- [NOT_NEEDED] **Rebalance Worker** — Calculate cluster disk imbalance and migrate bin files from over-utilized nodes to low-capacity nodes over gRPC[cite: 1].
we are NOT implementing an active background cluster rebalancer.Here is why that remains out of scope:Append-Only Write Path: New uploads are routed strictly to active nodes ($< 98\%$ full). Once full, nodes seamlessly freeze to READ_ONLY.Zero-Network GC: Local bin compaction reclaims disk space on individual nodes without moving a single byte over the cluster network.Reactive-Only Healing: Data movement across nodes only happens reactively via the Replica Healer when a node physically dies, or via the Bit-Rot Worker when a chunk corrupts.Eliminates Thrashing: You avoid network saturation, cross-node locking bugs, and ping-pong data movement entirely.

- [ ] **Bit-Rot Recovery Worker** — Scan bin files on disk, verify SHA-256 checksums, and repair corrupted chunks using healthy replica nodes[cite: 1].

- [ ] **Garbage Collection (GC) Worker** — Sweep storage nodes to purge unindexed bin fragments, soft-deleted files, and expired upload sessions[cite: 1].
Pick Target: SELECT id FROM bin_files WHERE status = 'SEALED' AND active_bytes <= 322MB (30%).Lock: Set DB status = 'COMPACTING' + acquire Redis lock lock:compaction:<bin_id>.Map Active Chunks: Fetch chunk_replicas WHERE is_deleted = false for offset/size map.Local Append: Read surviving byte blocks $\rightarrow$ append to current local ACTIVE bin (zero network I/O).DB Commit (Transaction): Re-point chunk_replicas to new bin_file_id + offset, increment target bin's active_bytes, remove old bin_files record.OS Delete: fs.unlink() old 1 GB bin file to instantly reclaim disk space.Unfreeze Node: If node storage usage drops below 95%, flip Redis state from READ_ONLY back to READ_WRITE.



TODO: once a node is filled up to 98% (excluding buffer storage space of 5gb, it should send available space 0 to coordinator so that it doesnt
pick it for new uploads and when the space reduces to <= 95% it will send the it availaiblity again)
it will come to <- 95% when gc cleares deleted files


TODO, create migrations folder to run migrations

TODO: delete the file related metadata as soon as user deletes a file (object, chunks, chunkReplica), 
and decrease the filesize from the binFile, curcial for garbage collection 