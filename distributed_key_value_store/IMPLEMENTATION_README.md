# Distributed KV Store Implementation Readme

## How to Build and Run

```bash
npm install
npm run build
npm start
```

For a 3-node cluster, run three processes with distinct environment values:

```bash
NODE_ID=node-1 PUBLIC_PORT=8081 GRPC_PORT=50051 DATA_DIR=./data/node1 PEERS=node-2@localhost:50052:8082,node-3@localhost:50053:8083 npm start
NODE_ID=node-2 PUBLIC_PORT=8082 GRPC_PORT=50052 DATA_DIR=./data/node2 PEERS=node-1@localhost:50051:8081,node-3@localhost:50053:8083 npm start
NODE_ID=node-3 PUBLIC_PORT=8083 GRPC_PORT=50053 DATA_DIR=./data/node3 PEERS=node-1@localhost:50051:8081,node-2@localhost:50052:8082 npm start
```

Verify each node:

```bash
curl http://localhost:8081/health
curl http://localhost:8082/health
curl http://localhost:8083/health
```

---

## Consistency Model

> Strong consistency via single-leader Raft replication with majority commit. Writes are accepted by the current leader and replicated to a majority of nodes before applying to the state machine.

---

## Replication Strategy

> The leader appends client commands to a local WAL-backed log and sends AppendEntries to followers via gRPC. A command is considered committed when a majority of nodes have acknowledged it. Reads and writes are served by the leader; non-leaders proxy requests to the leader.

---

## Architecture

See `ARCHITECTURE.md` for full design details.

Client → HTTP edge → Raft leader/follower logic → gRPC internal replication → WAL on disk

---

## Known Limitations

- Non-leader proxying is implemented, but leadership handoff and stale-leader edge cases still need hardening.
- No log snapshotting or compaction; the WAL is replayed fully at startup.
- Read freshness depends on a recent leader heartbeat rather than a fresh ReadIndex round-trip.
- Recovery is basic: followers catch up using AppendEntries retry, but there is no dedicated fast sync path.
- Docker Compose is included, but there is no chaos agent yet.

---

## Build Files

- `Dockerfile`
- `docker-compose.yml`
- `tsconfig.json`
- `src/index.ts`
- `src/protos/cluster.proto`
- `IMPLEMENTATION.md`
