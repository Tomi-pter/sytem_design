# Distributed KV Store — Architecture Summary

**Model**: Raft-lite consensus, single-leader replication, majority quorum (W=2 of 3), CP under partition.
**Stack**: TypeScript, gRPC/protobuf for internal node-to-node RPC, HTTP/JSON for the client-facing API.
**Deployment**: Docker Compose, 3 node containers + chaos agent sidecar.

---

## 1. Core Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Replication model | Leader-based (Raft) | Simple conflict resolution — writes ordered by arrival at leader, no CRDTs/vector clocks needed |
| Consistency | Strong, majority-quorum (W=2, R via ReadIndex) | Linearizable without requiring *all* nodes — tolerates 1 node down, matches the "survive a node crash" milestone |
| CAP trade-off | CP | On partition, minority side refuses writes/reads rather than serving stale data |
| Internal transport | gRPC + protobuf | Deadlines map cleanly onto election/heartbeat timeouts; schema doubles as WAL record format |
| Client transport | HTTP/JSON | Matches the hackathon's fixed API contract (`PUT/GET/DELETE /store/{key}`, `/health`) |
| Non-leader request handling | **Proxy (Option 2)** | Non-leader forwards internally to current leader and relays its response verbatim — guarantees every client request gets a contract-compliant `200/404/500`, regardless of which node it hits |
| Leader election | Real Raft: randomized election timeout, term numbers, majority vote, log-recency check | Prevents split-brain in asymmetric partitions; reelection is the same code path as bootstrap, no special-casing |
| Reads | Leader-only, ReadIndex-confirmed | Leader confirms majority heartbeat-ack before answering — prevents a partitioned/stale "leader" from serving stale reads |
| Snapshotting | Omitted (explicitly, in README) | Log stays small (~1000s of entries) at hackathon scale — full replay on boot is fast enough |

---

## 2. System Layout

```
                     Client (tester / curl)
                           │  HTTP/JSON  :8080
                           ▼
        ┌─────────────────────────────────────┐
        │         HTTP Edge Layer               │
        │  - checks local Raft role             │
        │  - if not leader: proxy request to    │
        │    current leader, relay response      │
        │  - /health always answered locally    │
        └──────────────────┬────────────────────┘
                           │
        ┌──────────────────▼────────────────────┐
        │            Raft Engine                  │
        │  role: FOLLOWER / CANDIDATE / LEADER    │
        │  currentTerm, votedFor, log[]           │
        │  commitIndex, lastApplied               │
        │  (leader only) nextIndex[], matchIndex[] │
        └──────────────────┬────────────────────┘
                           │  gRPC/protobuf   :50051
                           ▼
                  Peer nodes (RequestVote,
                     AppendEntries)

  Disk (per node):
    /data/wal.log        — length-prefixed protobuf LogEntry records
    /data/metadata.json  — { currentTerm, votedFor } — fsynced on every change
```

---

## 3. Write Path

1. Client `PUT /store/{key}` hits any node.
2. If not leader → proxy internally to current leader (max 1 hop), relay leader's response.
3. Leader appends `LogEntry` to local log + WAL (batched fsync).
4. Leader sends `AppendEntries` to both followers in parallel (gRPC, with deadline).
5. On majority ack (2 of 3, including self) → advance `commitIndex` → apply to state machine → `200 OK` to client.
6. No majority within timeout → `503`/`500` (do not falsely confirm).

## 4. Read Path (ReadIndex)

1. Client `GET /store/{key}` hits any node → proxy to leader if needed.
2. Leader confirms current leadership: majority heartbeat-ack within the last election-timeout window (cheap version — no extra round-trip if a recent ack already exists).
3. Confirmed → serve from local committed state (`200`/`404`).
4. Not confirmed (this node is stale/partitioned) → `503`, never serve a possibly-stale read.

## 5. Election / Reelection

1. All nodes boot as FOLLOWER, randomized election timeout (150–300ms).
2. Timeout with no heartbeat → become CANDIDATE, increment term, vote for self, `RequestVote` to peers in parallel.
3. Peer grants vote iff: candidate's term ≥ own, not already voted this term, candidate's log is at least as up-to-date.
4. Majority votes (2 of 3) → become LEADER, immediately heartbeat to establish authority.
5. Any node seeing a higher/equal term from a legitimate leader steps down to follower.
6. Reelection = same process, triggered whenever heartbeats stop. No bootstrap-specific code path.

## 6. Recovery (Stage 4 — anti-entropy)

Leader tracks `nextIndex[peer]` / `matchIndex[peer]`. For a recovering/lagging follower:

1. Leader sends `AppendEntries` at its optimistic `nextIndex` → follower's log doesn't match at `prevLogIndex/prevLogTerm` → `success=false`, includes `conflictTerm` + `firstIndexOfConflictTerm`.
2. Leader jumps `nextIndex` back to the start of the conflicting term (not decrementing 1-by-1) → retries.
3. Once agreement is found, leader streams forward from that point — same `AppendEntries` RPC as normal replication, no separate catch-up protocol.

## 7. Failure Handling Reference

| Situation | Behavior |
|---|---|
| Non-leader has no known leader (mid-election) | `503`, short retry hint |
| Forward-to-leader times out | `503` — do not apply locally |
| "Leader" can't reach majority (partitioned) | Write times out → `500`/`503`; read fails ReadIndex check → `503` |
| Forwarded request lands on another non-leader (stale view) | Cap at 1 hop, return `503` rather than chaining |
| Node crash (SIGKILL) | Unflushed WAL entries since last batch fsync may be lost — acceptable, only *acknowledged* writes must survive |
| Node restart | Replay `wal.log` from disk → rebuild state + log array → rejoin as follower → leader's nextIndex/matchIndex probing handles catch-up |

## 8. Explicitly Cut Corners (state these in the design review)

- No full Raft log compaction/snapshotting — fine at this data volume.
- ReadIndex uses "recent heartbeat ack within timeout window" rather than a fresh confirmation round every read — small correctness window near leadership transitions, traded for simplicity.
- Election uses standard Raft randomized timeout, not a more advanced pre-vote extension — slightly more vulnerable to unnecessary elections on flaky networks, acceptable at 3-node scale.
