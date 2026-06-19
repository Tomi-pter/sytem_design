# 🔥 Build It, Break It, Fix It

### A First-Principles Distributed Systems Hackathon

> _"Anyone can build a system that works when everything goes right. The real engineers build systems that work when everything goes wrong."_

---

## TL;DR for Participants

| Detail           | Info                                                                                                                        |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **Format**       | Solo — you build it alone, you defend it alone                                                                              |
| **Duration**     | ~5.5 hours (+ 30 min kickoff talk)                                                                                          |
| **Challenge**    | Build a distributed key-value store that survives chaos                                                                     |
| **Language**     | Your choice — Go, Rust, Java, Python, C++, whatever you want                                                                |
| **Starter code** | None. Blank slate. You architect everything.                                                                                |
| **Deployment**   | Cloud deployment (3 nodes) via Docker Compose, Kubernetes, or bare VMs — see [Deployment Guide](#deployment-infrastructure) |
| **Submission**   | Push to your GitHub repo before the deadline                                                                                |
| **AI tools**     | Fully allowed. But read the [caveat](#on-ai-tools).                                                                         |
| **Prize**        | Knowledge that money can't buy. Also, lunch. 🍕                                                                             |

---

## Philosophy

This hackathon is **not** about:

- ❌ Using Kafka, Redis Cluster, or any managed distributed service as a crutch
- ❌ Stringing together buzzword architectures from blog posts
- ❌ Building the fanciest UI or the most features
- ❌ Finishing fastest

This hackathon **is** about:

- ✅ Understanding **why** distributed systems are hard — from first principles
- ✅ Designing for **failure as the norm**, not the exception
- ✅ Making **deliberate trade-off decisions** (CAP, latency vs consistency, etc.)
- ✅ Proving your system works **under chaos**, not just under demo conditions

You are building the distributed logic from scratch — replication, consistency, failure handling, recovery. You decide the communication protocol, the replication strategy, the failure detection — everything. **The architecture decisions are half the challenge.**

> [!NOTE]
> **Docker and Kubernetes are allowed for deployment/packaging.** You can use Docker Compose to spin up your 3 nodes, or even deploy on Kubernetes. What you **cannot** do is use orchestration features (K8s health checks, restart policies, service mesh) to replace the distributed logic you're supposed to build. See [Rules](#rules--guidelines) for the exact boundary.

---

## On AI Tools

AI tools (Copilot, ChatGPT, Claude, Cursor, etc.) are **fully allowed**.

But here's the honest truth from someone who's built distributed systems for 4 years:

> AI is phenomenal at scaffolding CRUD endpoints, writing serialization code, and generating boilerplate. It will speed you up on the **easy parts**.
>
> But AI consistently struggles with:
>
> - Reasoning about network partition scenarios
> - Designing correct quorum logic
> - Handling split-brain without data loss
> - Race conditions under concurrent load
> - Recovery protocols that actually converge
>
> These are the parts where **your judgment** is what matters. The participants who think deeply about failure modes and make deliberate design choices will outperform those who prompt their way through.

Use AI for velocity. Use your brain for correctness.

---

## The Challenge: Distributed Key-Value Store

Build a **distributed key-value store** that runs across **3 nodes** (separate containers, pods, or processes in the cloud) and can:

1. **Store and retrieve** key-value pairs via a simple API (`PUT`, `GET`, `DELETE`)
2. **Replicate data** across nodes so that no single node is a single point of failure
3. **Survive node failures** — at least 1 node can die without data loss or total outage
4. **Handle concurrent traffic** — multiple clients reading and writing simultaneously
5. **Recover** — when a failed node comes back, it catches up on missed data

### Why a KV Store?

It's the "hello world" of distributed systems, but doing it **correctly** touches every hard problem:

```
                    ┌─────────────────────────────┐
                    │   Your KV Store Must Handle │
                    ├─────────────────────────────┤
                    │  • Data replication         │
                    │  • Consistency guarantees   │
                    │  • Leader election (maybe)  │
                    │  • Failure detection        │
                    │  • Conflict resolution      │
                    │  • Network partition handling │
                    │  • State recovery           │
                    │  • Concurrent access        │
                    └─────────────────────────────┘
```

### API Contract

Your system must expose an HTTP API (on each node) with at minimum:

```
PUT  /store/{key}         Body: {"value": "..."}    → 200 OK / 500 Error
GET  /store/{key}                                    → 200 {"value": "..."} / 404 Not Found
DELETE /store/{key}                                  → 200 OK / 404 Not Found
GET  /health                                         → 200 {"status": "healthy", "nodes": 3, "role": "leader|follower|..."}
```

> [!NOTE]
> **Alternative challenges** (if you'd prefer — announce this to the group and let them vote):
>
> - **Distributed Task Queue**: Producer-consumer with guaranteed at-least-once delivery across 3 worker nodes
> - **Distributed Rate Limiter**: Global rate limiting (e.g., 1000 req/min) enforced across 3 nodes with eventual consistency
> - **Distributed Lock Service**: Mutual exclusion with fencing tokens across 3 nodes
>
> All of these hit the same fundamental problems. The KV store is recommended because it has the clearest correctness criteria for automated testing.

---

## Progressive Stages

The hackathon is structured in **4 stages**. Each builds on the previous. You're scored cumulatively — you don't need to finish all stages to learn or to place well.

---

### 🟢 Stage 1 — "Hello, Distributed World" (90 min)

**Goal**: Get a basic multi-node system communicating.

| Task                                   | Think About                                                                 |
| -------------------------------------- | --------------------------------------------------------------------------- |
| Design your node architecture          | What does each node process look like? What threads/goroutines does it run? |
| Implement node-to-node communication   | Raw TCP? gRPC? HTTP? What are the trade-offs?                               |
| Basic `PUT` and `GET` on a single node | Data structure choice: hash map? B-tree? Write-ahead log?                   |
| Health check / heartbeat mechanism     | How do nodes know who's alive? How often do they check? What's the timeout? |
| Service discovery                      | How does node A find node B and C? Hardcoded? Config file? DNS?             |

**First Principles You'll Confront**:

- Synchronous vs asynchronous communication — and why it matters when a node is slow
- Serialization formats — JSON is easy, protobuf is fast, what do you pick and why?
- The bootstrapping problem — how does a cluster form from nothing?

**Milestone**: You can `PUT` a key on node A and `GET` it back from node A. All 3 nodes show heartbeat logs confirming they see each other.

---

### 🟡 Stage 2 — "Replication & Consistency" (90 min)

**Goal**: Data must exist on more than one node. Reads must return correct(ish) data.

| Task                                     | Think About                                            |
| ---------------------------------------- | ------------------------------------------------------ |
| Replicate writes to 2+ nodes             | Leader-based? Leaderless? Quorum? Chain replication?   |
| Define your consistency model explicitly | Strong? Eventual? Read-your-writes? Causal?            |
| Handle concurrent writes to the same key | Last-writer-wins? Vector clocks? CRDTs?                |
| Implement a read path                    | Read from leader? Read from any replica? Quorum reads? |

**First Principles You'll Confront**:

- **CAP theorem** — you WILL have to make a trade-off. Name it. Own it.
- **Consistency models** — linearizability is expensive. Eventual is cheap but surprising. What do your users actually need?
- **Quorum math** — if `W + R > N`, you get strong consistency. If not, you get speed. Choose.
- **Conflict resolution** — two clients write the same key at the same time. What happens? If you can't answer this clearly, your system has a bug.

**Milestone**: `PUT` a key on node A → `GET` it from node B → get the correct value. You can explain in one sentence what consistency guarantee your system provides.

> [!TIP]
> **Stop and think before coding this stage.** Grab paper. Draw your replication flow. Write one sentence: _"My system provides [X] consistency because [Y]."_ If you can't write that sentence, you're not ready to code.

---

### 🔴 Stage 3 — "Chaos Hour" 💀 (60 min)

**Goal**: Your system is about to get wrecked. Survive.

> [!CAUTION]
> **The organizer will inject failures into your running cloud deployment during this stage.** You don't know exactly when. You don't know exactly what. Design for it in advance.

| Failure Type                    | What Happens                                        | What We're Testing                                             |
| ------------------------------- | --------------------------------------------------- | -------------------------------------------------------------- |
| **Node crash**                  | One of your VMs gets stopped/killed                 | Failover: do remaining nodes keep serving?                     |
| **Network partition**           | Two nodes can't talk to each other                  | Split-brain: does your system handle this or corrupt data?     |
| **Slow node**                   | One node gets 2-5s artificial latency               | Timeout handling: does your system hang or degrade gracefully? |
| **Asymmetric partition** (hard) | A can talk to B, B can talk to C, A can't talk to C | Partial failure: the hardest real-world scenario               |

**What You Must Do**:

1. **Detect** the failure — how fast? What mechanism?
2. **Continue serving** requests — possibly degraded, but not dead
3. **Don't lose data** that was already acknowledged to clients
4. **Don't serve stale reads silently** — if you serve eventual data, your `/health` should reflect degraded state

**First Principles You'll Confront**:

- **Failure detection** — is a node dead or just slow? The answer matters enormously.
- **Split-brain** — two nodes both think they're the leader. Congratulations, you now have data corruption.
- **The impossibility of exactly-once delivery** — you can have at-least-once or at-most-once. Pick one. Design for it.
- **Idempotency** — if a client retries a write, does it get applied twice?

**Milestone**: Organizer kills one of your nodes. Your system continues serving reads and writes on the remaining nodes. No data loss.

---

### 🟣 Stage 4 — "Recovery & Load" (30 min, compressed)

**Goal**: Failed nodes come back. Traffic ramps up. Stay alive.

| Task                       | Think About                                                                             |
| -------------------------- | --------------------------------------------------------------------------------------- |
| Node recovery & state sync | When the killed node restarts, how does it catch up? Full copy? Delta sync? Log replay? |
| Anti-entropy mechanism     | How does a recovering node know _what_ it missed?                                       |
| Survive the load test      | The traffic simulator hits your system for 60 seconds. Stay up.                         |

**First Principles You'll Confront**:

- **State reconciliation** — the recovering node has stale data. How do you bring it up to speed without blocking the live nodes?
- **Thundering herd** — if 3 nodes come back at once and all request full state transfers...
- **Back-pressure** — when traffic exceeds your capacity, do you crash or do you shed load gracefully?

**Milestone**: Kill a node → write 1000 keys → bring node back → all 1000 keys are readable from the recovered node. Survive 60 seconds of load test without >5% error rate.

> [!NOTE]
> This stage is intentionally compressed to 30 minutes. Teams that designed well in Stage 2 (with recovery in mind) will breeze through this. Teams that didn't — that's the lesson. **Distributed systems reward upfront design.**

---

## Timeline

```
 ┌──────────────────────────────────────────────────────────────────────────┐
 │  TIME           PHASE                              DURATION              │
 ├──────────────────────────────────────────────────────────────────────────┤
 │  0:00 – 0:30    Kickoff & Primer Talk               30 min               │
 │  0:30 – 2:00    Stage 1: Communication & Basics     90 min               │
 │  2:00 – 2:15    Break + Quick Show-of-Hands         15 min               │
 │  2:15 – 3:45    Stage 2: Replication & Consistency  90 min               │
 │  3:45 – 4:00    Break + Status Check                15 min               │
 │  4:00 – 5:00    Stage 3: Chaos Hour                 60 min               │
 │  5:00 – 5:15    Quick Breather                      15 min               │
 │  5:15 – 5:45    Stage 4: Recovery & Load Test       30 min               │
 │  5:45 – 6:30    Demos, Judging & Discussion         45 min               │
 └──────────────────────────────────────────────────────────────────────────┘
```

### Break Activities

The breaks aren't just for coffee — use them strategically:

- **2:00 Break**: Quick show of hands — "Who has 3 nodes talking? Who chose leader-based replication? Leaderless?" This sparks ideas and cross-pollination.
- **3:45 Break**: "Who's confident their system survives a node kill?" — Then smile ominously. Build tension for Chaos Hour.

---

## Kickoff Talk (30 min)

Set the stage so everyone starts with the same mental model, regardless of background.

### 1. Why Distributed Systems? (3 min)

- Single machine limits: CPU plateau, memory ceiling, disk bandwidth
- Availability: one server = one point of failure
- Geography: your users are global, speed of light is a bottleneck

### 2. The 8 Fallacies of Distributed Computing (5 min)

Walk through each one. Kill the assumptions:

1. The network is reliable → **No. Packets get dropped. Cables get cut.**
2. Latency is zero → **No. Cross-AZ is ~1ms. Cross-region is ~50-200ms.**
3. Bandwidth is infinite → **No. And it matters when you're syncing state.**
4. The network is secure → **Not today's focus, but never assume.**
5. Topology doesn't change → **Nodes join and leave. Deal with it.**
6. There is one administrator → **In the cloud, failure domains overlap.**
7. Transport cost is zero → **Data transfer costs money. Design for it.**
8. The network is homogeneous → **Different nodes, different speeds.**

### 3. Communication 101 (5 min)

- Request/Response vs Message Passing
- Sync vs Async — and the deadly timeout question
- **The killer demo**: "You send an HTTP POST. The server writes the data to disk. Then the server crashes before sending the response. Did the write happen? The client doesn't know. What does it do?"

### 4. Consistency & Replication (7 min)

- Why replicate? Durability + Availability
- CAP theorem — the **real** version, not the Venn diagram
  - "You can have Consistency + Availability... until there's a Partition. During a partition, you choose: serve stale data (AP) or refuse to serve (CP)."
- Strong vs Eventual: the bank account example
  - "You have $100. You withdraw $80 on node A and $80 on node B simultaneously. Under eventual consistency, you just gave away $160."
- Quorum intuition: W + R > N → guaranteed overlap → strong reads

### 5. Failure is the Steady State (5 min)

- At Google/AWS scale, something is **always** broken
- Types: crash failures, omission failures, Byzantine failures
- Failure detection is **guessing** — you can never be sure a node is dead vs slow
- The split-brain horror story: two leaders, two truths, one corrupted database

### 6. The Challenge Briefing (5 min)

- Walk through the 4 stages
- Show the judging rubric
- **"Design before you code. The participants who open their editor first will finish last."**

---

## Judging Rubric (100 points)

### Automated Scoring (60 points)

The organizer runs the traffic simulator + chaos injector against each participant's cloud deployment after the deadline.

| Category                                | Points | How It's Measured                                                                    |
| --------------------------------------- | ------ | ------------------------------------------------------------------------------------ |
| **Correctness under normal conditions** | 10     | Write 500 keys → read all 500 → all values match                                     |
| **Replication correctness**             | 10     | Write on node A → read from node B and C → values match                              |
| **Availability during node crash**      | 15     | Kill 1 node → send 200 requests → measure success rate                               |
| **Data durability after crash**         | 10     | Kill 1 node → verify all previously-written data still readable from surviving nodes |
| **Recovery correctness**                | 10     | Kill node → write 200 new keys → restart node → verify recovered node has all data   |
| **Load test survival**                  | 5      | 60-second traffic simulation → error rate < 5%                                       |

### Design Review (40 points)

Each participant gets **3-5 minutes** to present + Q&A:

| Category                           | Points | What We're Looking For                                                                              |
| ---------------------------------- | ------ | --------------------------------------------------------------------------------------------------- |
| **Architecture clarity**           | 10     | Can you draw your system on a whiteboard? Do you know what each component does?                     |
| **Consistency model articulation** | 10     | Can you name your consistency guarantee in one sentence? Do you know the trade-off you made?        |
| **Failure handling rationale**     | 10     | When asked "what happens when node B dies mid-write?", do you have a clear answer?                  |
| **Honest self-assessment**         | 10     | What would you do differently? What's the weakest part of your system? Self-awareness > perfection. |

> [!IMPORTANT]
> **Honest self-assessment matters.** A participant who built a flawed system but can precisely explain _why_ it's flawed and _how_ they'd fix it scores higher than someone who built something that works but can't explain why. We're testing understanding, not just output.

---

## Deployment Infrastructure

You need to run **3 nodes** of your KV store. How you deploy them is part of the challenge. Choose your deployment tier:

### Tier 1: Docker Compose (Recommended) 🟢

**Best for**: Most participants. Simple, fast, gives you isolated nodes with a real network.

You run all 3 nodes as Docker containers on a **single cloud VM** using Docker Compose. Each container is a separate "node" with its own IP on a Docker bridge network.

```yaml
# Example docker-compose.yml — adapt to your system
version: "3.8"

services:
  node1:
    build: .
    container_name: kvstore-node1
    environment:
      - NODE_ID=node1
      - PEERS=node2:8080,node3:8080
    ports:
      - "8081:8080" # Expose to outside world
    networks:
      - kvnet

  node2:
    build: .
    container_name: kvstore-node2
    environment:
      - NODE_ID=node2
      - PEERS=node1:8080,node3:8080
    ports:
      - "8082:8080"
    networks:
      - kvnet

  node3:
    build: .
    container_name: kvstore-node3
    environment:
      - NODE_ID=node3
      - PEERS=node1:8080,node2:8080
    ports:
      - "8083:8080"
    networks:
      - kvnet

  # MANDATORY — the chaos agent (provided by organizer)
  chaos-agent:
    image: ghcr.io/arihant/chaos-agent:latest # pre-built by organizer
    container_name: chaos-agent
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock # gives it control over containers
    ports:
      - "9090:9090" # chaos control endpoint
    networks:
      - kvnet

networks:
  kvnet:
    driver: bridge
```

**Why Docker Compose works great**:

- `docker kill kvstore-node2` → instant node crash
- `docker pause kvstore-node2` → simulates a frozen/slow node
- `docker network disconnect kvnet kvstore-node2` → network partition
- `docker start kvstore-node2` → node recovery
- All via the Docker socket — the chaos agent can do this programmatically

**Cloud cost**: 1 VM instead of 3 → even cheaper. A single `e2-medium` on GCP is ~$0.03/hr.

---

### Tier 2: Kubernetes (Advanced) 🟡

**Best for**: Participants who already know K8s and want a production-like environment.

Deploy your 3 nodes as **Pods** (or a StatefulSet with 3 replicas) in a Kubernetes cluster. The organizer gets `kubectl` access to your namespace to inject chaos.

```yaml
# Example: 3 pods in a StatefulSet
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kvstore
spec:
  serviceName: kvstore
  replicas: 3
  selector:
    matchLabels:
      app: kvstore
  template:
    metadata:
      labels:
        app: kvstore
    spec:
      containers:
        - name: kvstore
          image: your-registry/kvstore:latest
          ports:
            - containerPort: 8080
          env:
            - name: NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          # IMPORTANT: Do NOT add livenessProbe or readinessProbe
          # that auto-restarts your pods. That's K8s doing the
          # distributed logic for you. YOU handle failure detection.
```

> [!WARNING]
> **If you use Kubernetes, these features are OFF LIMITS:**
>
> - ❌ `livenessProbe` / `readinessProbe` with restart policies (K8s would auto-heal your pods — that's cheating)
> - ❌ Service mesh (Istio/Linkerd) for retry logic or circuit breaking
> - ❌ K8s `NetworkPolicy` for your own traffic management (the organizer uses these for chaos)
> - ❌ Persistent Volume Claims with automatic reattachment
>
> You can use K8s for: deploying pods, DNS-based service discovery (`kvstore-0.kvstore`, etc.), and exposing endpoints via LoadBalancer/NodePort. That's it.

**Chaos on K8s** (what the organizer does):

- `kubectl delete pod kvstore-1 --grace-period=0 --force` → node crash
- Apply a `NetworkPolicy` to block traffic between specific pods → partition
- `kubectl exec kvstore-2 -- tc qdisc add dev eth0 root netem delay 3000ms` → slow node

**Cloud cost**: A small GKE Autopilot or EKS cluster with 3 pods is ~$0.10-0.30/hr. Or use a free-tier cluster (GKE gives 1 free zonal cluster management fee).

---

### Tier 3: Bare VMs / Processes (Minimal) 🔵

**Best for**: Participants who want maximum control or can't use Docker.

3 separate VMs (or 3 processes on one VM). SSH in, build, run. Old school.

**Chaos**: Organizer SSHs in and runs `kill -9`, `tc netem`, `iptables` directly.

---

### Deployment Comparison

|                            | Docker Compose                           | Kubernetes                  | Bare VMs                        |
| -------------------------- | ---------------------------------------- | --------------------------- | ------------------------------- |
| **Setup time**             | ~10 min                                  | ~30 min                     | ~20 min                         |
| **Cloud cost (6 hrs)**     | ~$0.02 (1 VM)                            | ~$0.50-1.50                 | ~$0.00-0.10 (3 micro VMs)       |
| **Chaos injection**        | Docker commands / chaos agent            | kubectl + NetworkPolicy     | SSH + kill/iptables             |
| **Realism**                | Good (separate containers, real network) | Best (production-like)      | Good (actual separate machines) |
| **Complexity**             | Low                                      | High                        | Low                             |
| **Organizer access model** | Chaos agent via Docker socket            | kubectl access to namespace | SSH access                      |

---

## Chaos Agent (Mandatory Sidecar)

Regardless of your deployment tier, you **must** run the **chaos agent** alongside your nodes. This is a small container/process provided by the organizer that gives standardized chaos control.

### What It Does:

The chaos agent exposes an HTTP API on port `9090` that **only the organizer** calls:

```
POST /chaos/kill    {"target": "node2"}           → kills the target node's container/process
POST /chaos/pause   {"target": "node2"}           → freezes the target (SIGSTOP / docker pause)
POST /chaos/slow    {"target": "node2", "ms": 3000} → adds network latency to target
POST /chaos/partition {"a": "node1", "b": "node3"} → drops traffic between two nodes
POST /chaos/heal                                   → removes ALL injected failures
POST /chaos/resume  {"target": "node2"}           → restarts a killed/paused node
GET  /chaos/status                                 → shows current chaos state
```

### Why a Chaos Agent?

1. **Standardized access**: The organizer doesn't need SSH keys, K8s credentials, or cloud IAM for 25-40 different setups. One HTTP endpoint per participant.
2. **Fair chaos**: Everyone gets the exact same failure injection mechanism.
3. **Scriptable**: The organizer can run automated chaos scenarios against all participants simultaneously.
4. **Safe**: The agent can only affect containers/processes in your deployment, nothing else.

### How to Deploy It:

**Docker Compose** (easiest — see the compose file above):

- Mount the Docker socket → agent can `docker kill`, `docker pause`, `docker network disconnect` your containers

**Kubernetes**:

- Deploy as a sidecar or separate pod in your namespace with appropriate RBAC to delete/patch pods

**Bare VMs**:

- Run the agent binary on the VM, configured with PIDs or process names of your nodes

> [!IMPORTANT]
> The chaos agent will be **pre-built and distributed by the organizer** before the hackathon. You just include it in your deployment. It's ~20 lines to add to your docker-compose.yml.

---

## Cloud Provider Guide

> [!IMPORTANT]
> The cost of running this hackathon's infrastructure is effectively **$0.00 – $0.50** on any major cloud provider with student credits or free tier. Don't let cost anxiety stop you from deploying.

### Option 1: Google Cloud Platform (Recommended for Students)

**Free credits**: $300 for new accounts (no `.edu` needed)

| Deployment     | What to Spin Up                      | Cost (6 hrs) |
| -------------- | ------------------------------------ | ------------ |
| Docker Compose | 1 x `e2-medium` VM (2 vCPU, 4GB RAM) | ~$0.03       |
| Kubernetes     | GKE Autopilot cluster (3 pods)       | ~$0.30       |
| Bare VMs       | 3 x `e2-micro` (free tier eligible)  | $0.00        |

### Option 2: AWS

**Free tier**: 750 hours/month of t3.micro (for 12 months after signup)

| Deployment     | What to Spin Up            | Cost (6 hrs)                           |
| -------------- | -------------------------- | -------------------------------------- |
| Docker Compose | 1 x `t3.small`             | ~$0.02                                 |
| Kubernetes     | EKS + 2 `t3.micro` workers | ~$0.60 (EKS control plane is $0.10/hr) |
| Bare VMs       | 3 x `t3.micro` (free tier) | $0.00                                  |

### Option 3: Azure

**Student credits**: $100 via Azure for Students (`.edu` email required)

| Deployment     | What to Spin Up                          | Cost (6 hrs) |
| -------------- | ---------------------------------------- | ------------ |
| Docker Compose | 1 x `B2s` VM                             | ~$0.05       |
| Kubernetes     | AKS (free control plane) + 2 `B2s` nodes | ~$0.10       |
| Bare VMs       | 3 x `B1s`                                | ~$0.09       |

### Cloud Setup Checklist

- [ ] Cloud account created with credits/free tier active
- [ ] Docker installed on your VM(s)
- [ ] Firewall/security group allows:
  - Your KV store ports (e.g., 8081-8083) from organizer's IP
  - Chaos agent port (9090) from organizer's IP
  - SSH from your IP
- [ ] Your system starts with `docker compose up` or equivalent
- [ ] Chaos agent is running and accessible on port 9090
- [ ] You've tested: can you hit `http://<your-vm-ip>:8081/health` from your laptop?

> [!TIP]
> **Simplest path**: 1 VM → install Docker → `git clone` → `docker compose up`. You're live in 5 minutes. Don't overthink deployment — save your brainpower for the distributed logic.

---

## Submission & Evaluation Workflow

### What the Organizer Needs from You

```
1. GitHub repo URL (public, or invite @arihant as collaborator)
2. Cloud endpoint: VM IP address (or K8s LoadBalancer IP)
3. Node ports (e.g., 8081, 8082, 8083)
4. Chaos agent port (default: 9090)
5. Deployment tier: Docker Compose / Kubernetes / Bare VMs
6. A README.md with:
   - How to build and run your system
   - Your consistency model (1 sentence)
   - Your replication strategy (1 paragraph)
   - Known limitations
```

### Evaluation Flow

```
  PARTICIPANT                                    ORGANIZER
  ───────────                                    ─────────

  Code on laptop ──→ Push to GitHub ──────→  Pull & review code
       │                                         │
       ▼                                         │
  Deploy to cloud ──→ Share IP + ports ───→  Point traffic simulator
       │              + chaos agent port     at participant's nodes
       │                                         │
       │                                         ▼
       │                                    Hit chaos agent API:
       │                                    POST /chaos/kill {node2}
       │                                    POST /chaos/partition {1↔3}
       │                                    POST /chaos/slow {node1, 3s}
       │                                         │
       │                                         ▼
       │                                    Score automated tests
       │                                    (60 points)
       │                                         │
       ▼                                         ▼
  Present design ◄────────────────────→   Score design review
  (3-5 min talk)                          (40 points)
```

### Evaluation Order:

1. **After deadline**: Organizer runs automated tests against each participant's cloud deployment via the chaos agent
2. **If cloud deploy failed**: Organizer pulls from GitHub and runs locally with `docker compose up` (reduced score — max 80/100)
3. **Demo round**: Each participant presents their design (3-5 min + Q&A)
4. **Scores announced** over lunch 🍕

---

## Pre-Hackathon Prep (Send 1 Week Before)

### What Participants Should Do Before Showing Up:

- [ ] **Set up a cloud account** on GCP/AWS/Azure — don't waste hackathon time on signup
- [ ] **Spin up and SSH into a VM** at least once — make sure you can do it
- [ ] **Choose your language** — make sure you know how to do HTTP servers and TCP sockets in it
- [ ] **Read at least 1 of the resources below**

### Must-Know Concepts:

- Basic networking (TCP/IP, HTTP, what a port is)
- How to run multiple processes (on your laptop or on VMs)
- Basic concurrency (threads, goroutines, async — in your chosen language)
- Git (for submission)

### Recommended Reading (Pick 1-2, seriously):

| Resource                                                                                                                                    | Type      | Time   | Why                                                                            |
| ------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ------ | ------------------------------------------------------------------------------ |
| [Notes on Distributed Systems for Young Bloods](https://www.somethingsimilar.com/2013/01/14/notes-on-distributed-systems-for-young-bloods/) | Blog post | 10 min | Best 10-minute intro. Read this one at minimum.                                |
| [The CAP FAQ](https://www.the-paper-trail.org/page/cap-faq/)                                                                                | Blog post | 15 min | Clears up the most common CAP misconceptions                                   |
| [DDIA](https://dataintensive.net/) Chapters 5 & 8                                                                                           | Book      | 2 hrs  | Replication (Ch5) and "The Trouble with Distributed Systems" (Ch8). The bible. |
| [Martin Kleppmann's lectures 1-3](https://www.youtube.com/playlist?list=PLeKd45zvjcDFUEv_ohr_HdUFe97RItdiB)                                 | Video     | 3 hrs  | If you prefer video. Lectures 1-3 cover the essentials.                        |

### Language-Specific Tips:

| Language    | Good For                                                                      | Watch Out For                                                                                                   |
| ----------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **Go**      | Goroutines make concurrency trivial. Excellent stdlib for HTTP and TCP. Fast. | Error handling verbosity.                                                                                       |
| **Rust**    | Ownership model prevents data races at compile time. Fast.                    | Steep learning curve. Don't pick this unless you already know Rust.                                             |
| **Java**    | Robust threading. Excellent libraries.                                        | Boilerplate. JVM startup time on micro VMs.                                                                     |
| **Python**  | Fast to prototype. Easy HTTP servers (Flask/FastAPI).                         | GIL limits true parallelism. `asyncio` or `multiprocessing` needed. Not ideal for high-throughput load testing. |
| **Node.js** | Event loop handles many connections.                                          | Single-threaded. CPU-bound work blocks everything.                                                              |
| **C++**     | Maximum performance.                                                          | Development speed. Debugging distributed C++ in 6 hours is pain.                                                |

---

## Organizer's Toolkit (Pre-Built Before Hackathon)

> [!NOTE]
> All three tools below will be built before the hackathon day. Specs documented here for completeness.

### 1. Chaos Agent (Go — deployed by participants)

The sidecar container that participants deploy alongside their nodes. See [Chaos Agent section](#chaos-agent-mandatory-sidecar) above for full API spec.

Implementation approach:

- Docker SDK for Go to control containers via Docker socket
- `tc netem` for latency injection (exec into container)
- `iptables` rules for network partitions
- Simple HTTP server on port 9090
- State tracking (what chaos is currently active)

### 2. Traffic Simulator (Go)

- Configurable traffic profile (ramp up → steady → spike → cooldown)
- Mixed read/write workload with configurable ratio
- **Consistency verification**: write a key with a unique value, read it back from a different node, compare
- Latency histogram output (p50, p95, p99)
- Error rate tracking per phase
- JSON report output for automated scoring
- Accepts target endpoints as CLI args → run against any participant

### 3. Orchestrator / Scoring Script (Go)

The master script the organizer runs per participant:

```
orchestrator --target <participant-ip> \
             --kv-ports 8081,8082,8083 \
             --chaos-port 9090 \
             --scenario standard \
             --output scores/<participant-name>.json
```

It runs a predefined chaos scenario while the traffic simulator is hammering the system, then generates a score card:

```json
{
  "participant": "alice",
  "scores": {
    "correctness_normal": 10,
    "replication_correctness": 8,
    "availability_during_crash": 12,
    "data_durability": 10,
    "recovery_correctness": 7,
    "load_test_survival": 4
  },
  "total_automated": 51,
  "details": {
    "requests_sent": 2400,
    "requests_succeeded": 2280,
    "consistency_violations": 3,
    "recovery_time_ms": 4500,
    "p99_latency_ms": 230
  }
}
```

---

## Rules & Guidelines

### Hard Rules:

1. **Solo work only** — no pair programming, no sharing code between participants during the hackathon
2. **All code must be in your GitHub repo** by the deadline — no post-deadline commits (we check timestamps)
3. **No managed distributed databases** — no DynamoDB, no Cosmos DB, no CockroachDB, no Redis Cluster. You're building the distributed system, not using one.
4. **Docker / Kubernetes: allowed for DEPLOYMENT, not for DISTRIBUTED LOGIC.** Specifically:

| ✅ Allowed                                                | ❌ Not Allowed                                            |
| --------------------------------------------------------- | --------------------------------------------------------- |
| `docker compose up` to start 3 containers                 | K8s `livenessProbe` auto-restarting crashed pods          |
| K8s StatefulSet to deploy 3 pods                          | K8s `readinessProbe` for traffic routing                  |
| Docker networking between containers                      | Service mesh (Istio/Linkerd) for retries/circuit breaking |
| K8s DNS for service discovery (e.g., `kvstore-0.kvstore`) | K8s `PodDisruptionBudget` for availability                |
| Docker volumes for persistence                            | K8s auto-scaling based on load                            |

**Rule of thumb**: If you can explain what the Docker/K8s feature does and it's just "runs my binary" or "gives it a network address" → fine. If it's doing health checking, failover, load balancing, or restart logic → that's your job to implement.

5. **HTTP API must match the contract** — the automated tester needs to hit your endpoints
6. **Chaos agent must be deployed** — the organizer needs port 9090 accessible on your VM

### Soft Guidelines:

- Design before you code. Seriously. 20 minutes of design saves 2 hours of debugging.
- Start with 2 nodes if 3 feels overwhelming. Get replication working, then add the third.
- Simple and correct beats complex and buggy. A system with leader-based replication that actually works scores higher than a "Raft implementation" that doesn't.
- Write a README as you go. You'll thank yourself during the design review.
- Commit frequently. If your system breaks at 4:30, you want a working commit from 3:45 to fall back to.
- **Test chaos locally** before the hackathon: `docker kill kvstore-node2` and see what your system does. If you haven't tested this, Chaos Hour will be brutal.

---

## What Makes This Hackathon Different

| Typical Hackathon        | This Hackathon                                                        |
| ------------------------ | --------------------------------------------------------------------- |
| Rewards features shipped | Rewards **resilience proven**                                         |
| Judges working demos     | Judges **working demos under fire**                                   |
| "Does it work?"          | **"Does it work when node 2 is dead and node 3 is slow?"**            |
| AI can carry you         | AI scaffolds the easy parts; **your judgment handles the hard parts** |
| Build something new      | Understand something **fundamental**                                  |

The participants who do well here won't just know how to _use_ Kafka or Redis — they'll understand **why** those tools exist and what problems they solve at a fundamental level.

That's the difference between a developer who deploys distributed systems and an engineer who **builds** them.

---

## Quick Reference Card (Print & Distribute)

```
╔══════════════════════════════════════════════════════════════╗
║                    SURVIVAL CHEAT SHEET                      ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  CAP THEOREM                                                 ║
║  ───────────                                                 ║
║  Partition happens → Choose: Consistency OR Availability     ║
║  CP: Refuse requests if unsure about consistency             ║
║  AP: Serve requests but data might be stale                  ║
║                                                              ║
║  QUORUM MATH                                                 ║
║  ────────────                                                ║
║  N = total nodes    W = write ack needed   R = read nodes    ║
║  W + R > N  →  strong consistency                            ║
║  Example: N=3, W=2, R=2  →  always see latest write          ║
║                                                              ║
║  REPLICATION STRATEGIES                                      ║
║  ───────────────────────                                     ║
║  Leader-based: one node handles all writes, replicates out   ║
║  Leaderless: any node takes writes, quorum resolves conflict ║
║  Chain: write→node1→node2→node3→ack                          ║
║                                                              ║
║  FAILURE DETECTION                                           ║
║  ─────────────────                                           ║
║  Heartbeat interval: how often nodes ping each other         ║
║  Timeout threshold: how long before declaring a node dead    ║
║  Too short = false positives  │  Too long = slow failover    ║
║                                                              ║
║  WHEN IN DOUBT                                               ║
║  ─────────────                                               ║
║  1. Draw it on paper first                                   ║
║  2. What happens if a node dies mid-operation?               ║
║  3. What happens if two clients write the same key?          ║
║  4. Can you explain your design in one sentence?             ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
```

---

_Hackathon organized by Arihant_  
_Draft v3 — June 19, 2025_
