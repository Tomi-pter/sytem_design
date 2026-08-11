import express from "express";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import fs from "fs";
import path from "path";
import http from "http";

const NODE_ID = process.env.NODE_ID || "node-1";
const PUBLIC_PORT = parseInt(process.env.PUBLIC_PORT || "8081", 10);
const GRPC_PORT = parseInt(process.env.GRPC_PORT || "50051", 10);
const DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, "../data");
const PEERS_RAW = process.env.PEERS || "";

interface LogEntry {
  index: number;
  term: number;
  op: "PUT" | "DELETE";
  key: string;
  value: string;
}

interface PeerConfig {
  id: string;
  grpcAddress: string;
  httpAddress: string;
}

type VoteRequestPayload = {
  term: number;
  candidateId: string;
  lastLogIndex: number;
  lastLogTerm: number;
};

type VoteResponsePayload = {
  term: number;
  voteGranted: boolean;
};

type AppendEntriesRequestPayload = {
  term: number;
  leaderId: string;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
};

type AppendEntriesResponsePayload = {
  term: number;
  success: boolean;
  matchIndex: number;
  conflictTerm: number;
  conflictIndex: number;
};

const app = express();
app.use(express.json());

const PEERS = parsePeersEnv(PEERS_RAW);
const peerMap = new Map(PEERS.map((peer) => [peer.id, peer]));

let currentTerm = 0;
let votedFor: string | null = null;
let role: "FOLLOWER" | "CANDIDATE" | "LEADER" = "FOLLOWER";
let leaderId: string | null = null;
let logEntries: LogEntry[] = [];
let commitIndex = 0;
let lastApplied = 0;
let lastHeartbeat = Date.now();
let lastLeaderAck = 0;
let electionTimeoutMs = randomElectionTimeout();
const heartbeatIntervalMs = 100;
const stateMachine = new Map<string, string>();

const nextIndex: Record<string, number> = {};
const matchIndex: Record<string, number> = {};

const PROTO_PATH = path.resolve(__dirname, "./protos/cluster.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
});
const raftProto = grpc.loadPackageDefinition(packageDefinition) as any;

function parsePeersEnv(value: string): PeerConfig[] {
  if (!value.trim()) return [];
  return value.split(",").map((token) => {
    const [id, address] = token.split("@");
    if (!id || !address) {
      throw new Error(
        `Invalid PEERS token: ${token}. Expected nodeId@host:grpcPort:httpPort`,
      );
    }
    const parts = address.split(":");
    if (parts.length !== 3) {
      throw new Error(
        `Invalid peer address: ${address}. Expected host:grpcPort:httpPort`,
      );
    }
    const [host, grpcPort, httpPort] = parts;
    return {
      id,
      grpcAddress: `${host}:${grpcPort}`,
      httpAddress: `http://${host}:${httpPort}`,
    };
  });
}

function randomElectionTimeout() {
  return 150 + Math.floor(Math.random() * 150);
}

function safeDir(dir: string) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function loadMetadata() {
  const metadataPath = path.resolve(DATA_DIR, "metadata.json");
  if (!fs.existsSync(metadataPath)) return;
  try {
    const raw = fs.readFileSync(metadataPath, "utf-8");
    const parsed = JSON.parse(raw);
    currentTerm = parsed.currentTerm ?? 0;
    votedFor = parsed.votedFor ?? null;
  } catch (err) {
    console.warn("Failed to load metadata", err);
  }
}

function saveMetadata() {
  const metadataPath = path.resolve(DATA_DIR, "metadata.json");
  fs.writeFileSync(metadataPath, JSON.stringify({ currentTerm, votedFor }));
}

function appendWal(entry: LogEntry) {
  const walPath = path.resolve(DATA_DIR, "wal.log");
  const record = JSON.stringify(entry) + "\n";
  fs.appendFileSync(walPath, record);
}

function loadWal() {
  const walPath = path.resolve(DATA_DIR, "wal.log");
  if (!fs.existsSync(walPath)) return;
  const content = fs.readFileSync(walPath, "utf-8").trim();
  if (!content) return;
  const lines = content.split("\n");
  for (const line of lines) {
    try {
      const entry = JSON.parse(line) as LogEntry;
      logEntries.push(entry);
    } catch {
      console.warn("Skipping invalid WAL entry");
    }
  }
}

function applyLogToStateMachine() {
  while (lastApplied < commitIndex) {
    const entry = logEntries[lastApplied];
    if (!entry) break;
    if (entry.op === "PUT") {
      stateMachine.set(entry.key, entry.value);
    } else {
      stateMachine.delete(entry.key);
    }
    lastApplied += 1;
  }
}

function getLastLogIndex() {
  return logEntries.length;
}

function getLastLogTerm() {
  return logEntries.length === 0 ? 0 : logEntries[logEntries.length - 1].term;
}

function clusterSize() {
  return PEERS.length + 1;
}

function quorumSize() {
  return Math.floor(clusterSize() / 2) + 1;
}

function isLeader() {
  return role === "LEADER";
}

function isLeaderHealthy() {
  return isLeader() && Date.now() - lastLeaderAck < electionTimeoutMs;
}

function getLeaderPeer() {
  if (!leaderId) return null;
  return peerMap.get(leaderId) ?? null;
}

function createRaftClient(address: string) {
  return new raftProto.raftkv.RaftInternal(
    address,
    grpc.credentials.createInsecure(),
  );
}

function proxyToLeader(req: express.Request, res: express.Response) {
  const leaderPeer = getLeaderPeer();
  if (!leaderPeer) {
    return res.status(503).json({ error: "No leader known" });
  }

  const url = new URL(req.originalUrl, leaderPeer.httpAddress);
  const method = req.method;
  const body =
    req.body && Object.keys(req.body).length
      ? JSON.stringify(req.body)
      : undefined;
  const headers: http.OutgoingHttpHeaders = {
    ...req.headers,
    host: url.host,
  };
  if (body) {
    headers["content-type"] = "application/json";
    headers["content-length"] = Buffer.byteLength(body);
  }

  const options: http.RequestOptions = {
    method,
    hostname: url.hostname,
    port: url.port,
    path: `${url.pathname}${url.search}`,
    headers,
    timeout: 2000,
  };

  const proxyReq = http.request(options, (proxyRes) => {
    res.writeHead(proxyRes.statusCode ?? 502, proxyRes.headers);
    proxyRes.pipe(res);
  });

  proxyReq.on("error", (err) => {
    res.status(502).json({ error: "Leader proxy error", detail: err.message });
  });

  if (body) {
    proxyReq.write(body);
  }
  proxyReq.end();
}

app.get("/store/:key", (req, res) => {
  if (!isLeaderHealthy()) {
    if (!isLeader()) {
      return proxyToLeader(req, res);
    }
    return res.status(503).json({ error: "Leader cannot confirm majority" });
  }

  const key = req.params.key;
  if (stateMachine.has(key)) {
    return res.status(200).json({ [key]: stateMachine.get(key) });
  }
  return res.status(404).json({ error: "Not Found" });
});

app.put("/store/:key", async (req, res) => {
  if (!isLeaderHealthy()) {
    if (!isLeader()) {
      return proxyToLeader(req, res);
    }
    return res.status(503).json({ error: "Leader cannot confirm majority" });
  }

  const key = req.params.key;
  const value = req.body?.[key];

  if (typeof value !== "string") {
    return res
      .status(400)
      .json({ error: `Body must contain a string value for key "${key}"` });
  }

  const entry: LogEntry = {
    index: getLastLogIndex() + 1,
    term: currentTerm,
    op: "PUT",
    key,
    value,
  };
  appendWal(entry);
  logEntries.push(entry);

  const success = await replicateLog(entry.index);
  if (!success) {
    return res.status(503).json({ error: "Failed to replicate to majority" });
  }

  commitIndex = Math.max(commitIndex, entry.index);
  applyLogToStateMachine();
  return res.status(200).json({ status: "OK" });
});

app.delete("/store/:key", async (req, res) => {
  if (!isLeaderHealthy()) {
    if (!isLeader()) {
      return proxyToLeader(req, res);
    }
    return res.status(503).json({ error: "Leader cannot confirm majority" });
  }

  const key = req.params.key;
  const entry: LogEntry = {
    index: getLastLogIndex() + 1,
    term: currentTerm,
    op: "DELETE",
    key,
    value: "",
  };
  appendWal(entry);
  logEntries.push(entry);

  const success = await replicateLog(entry.index);
  if (!success) {
    return res.status(503).json({ error: "Failed to replicate to majority" });
  }

  commitIndex = Math.max(commitIndex, entry.index);
  applyLogToStateMachine();
  return res.status(200).json({ status: "OK" });
});

app.get("/health", (req, res) => {
  return res.status(200).json({
    status: isLeaderHealthy() ? "healthy" : "degraded",
    nodes: clusterSize(),
    role: role.toLowerCase(),
    leader: leaderId,
    term: currentTerm,
    commitIndex,
    lastApplied,
  });
});

function requestVote(call: any, callback: any) {
  const req = call.request as VoteRequestPayload;
  if (req.term < currentTerm) {
    return callback(null, { term: currentTerm, voteGranted: false });
  }

  if (req.term > currentTerm) {
    currentTerm = req.term;
    role = "FOLLOWER";
    votedFor = null;
    leaderId = null;
    saveMetadata();
  }

  const candidateUpToDate =
    req.lastLogTerm > getLastLogTerm() ||
    (req.lastLogTerm === getLastLogTerm() &&
      req.lastLogIndex >= getLastLogIndex());

  if (
    (votedFor === null || votedFor === req.candidateId) &&
    candidateUpToDate
  ) {
    votedFor = req.candidateId;
    lastHeartbeat = Date.now();
    saveMetadata();
    return callback(null, { term: currentTerm, voteGranted: true });
  }

  return callback(null, { term: currentTerm, voteGranted: false });
}

function appendEntries(call: any, callback: any) {
  const req = call.request as AppendEntriesRequestPayload;
  if (req.term < currentTerm) {
    return callback(null, {
      term: currentTerm,
      success: false,
      matchIndex: 0,
      conflictTerm: 0,
      conflictIndex: 0,
    });
  }

  if (req.term > currentTerm) {
    currentTerm = req.term;
    role = "FOLLOWER";
    votedFor = null;
    leaderId = req.leaderId;
    saveMetadata();
  } else {
    leaderId = req.leaderId;
  }

  lastHeartbeat = Date.now();

  if (req.prevLogIndex > 0) {
    const prevLogEntry = logEntries[req.prevLogIndex - 1];
    if (!prevLogEntry || prevLogEntry.term !== req.prevLogTerm) {
      const conflictTerm = prevLogEntry?.term ?? 0;
      const conflictIndex = prevLogEntry
        ? findFirstIndexOfTerm(conflictTerm)
        : req.prevLogIndex;
      return callback(null, {
        term: currentTerm,
        success: false,
        matchIndex: 0,
        conflictTerm,
        conflictIndex,
      });
    }
  }

  let index = req.prevLogIndex;
  for (const entry of req.entries) {
    index += 1;
    if (logEntries[index - 1]?.term !== entry.term) {
      logEntries = logEntries.slice(0, index - 1);
      logEntries.push(entry);
      appendWal(entry);
    }
  }

  if (req.leaderCommit > commitIndex) {
    commitIndex = Math.min(req.leaderCommit, getLastLogIndex());
    applyLogToStateMachine();
  }

  return callback(null, {
    term: currentTerm,
    success: true,
    matchIndex: getLastLogIndex(),
    conflictTerm: 0,
    conflictIndex: 0,
  });
}

function findFirstIndexOfTerm(term: number) {
  for (let i = 0; i < logEntries.length; i += 1) {
    if (logEntries[i].term === term) return i + 1;
  }
  return 1;
}

function startElection() {
  if (role === "LEADER") return;

  role = "CANDIDATE";
  currentTerm += 1;
  votedFor = NODE_ID;
  leaderId = null;
  saveMetadata();
  const votesNeeded = quorumSize();
  let votes = 1;
  lastHeartbeat = Date.now();

  PEERS.forEach((peer) => {
    const client = createRaftClient(peer.grpcAddress);
    const request: VoteRequestPayload = {
      term: currentTerm,
      candidateId: NODE_ID,
      lastLogIndex: getLastLogIndex(),
      lastLogTerm: getLastLogTerm(),
    };
    client.RequestVote(request, (err: any, response: VoteResponsePayload) => {
      if (role !== "CANDIDATE") return;
      if (err) return;
      if (response.term > currentTerm) {
        currentTerm = response.term;
        role = "FOLLOWER";
        votedFor = null;
        leaderId = null;
        saveMetadata();
        return;
      }
      if (response.voteGranted) {
        votes += 1;
        if (votes >= votesNeeded) {
          role = "LEADER";
          leaderId = NODE_ID;
          lastLeaderAck = Date.now();
          initializeLeaderState();
          broadcastHeartbeats();
        }
      }
    });
  });
}

function initializeLeaderState() {
  for (const peer of PEERS) {
    nextIndex[peer.id] = getLastLogIndex() + 1;
    matchIndex[peer.id] = 0;
  }
}

function sendAppendEntriesToPeer(peer: PeerConfig) {
  return new Promise<AppendEntriesResponsePayload>((resolve, reject) => {
    const next = Math.max(1, nextIndex[peer.id] ?? getLastLogIndex() + 1);
    const prevLogIndex = next - 1;
    const prevLogTerm =
      prevLogIndex === 0 ? 0 : (logEntries[prevLogIndex - 1]?.term ?? 0);
    const entries = logEntries.slice(prevLogIndex);
    const request: AppendEntriesRequestPayload = {
      term: currentTerm,
      leaderId: NODE_ID,
      prevLogIndex,
      prevLogTerm,
      entries,
      leaderCommit: commitIndex,
    };
    const client = createRaftClient(peer.grpcAddress);
    client.AppendEntries(
      request,
      (err: any, response: AppendEntriesResponsePayload) => {
        if (err) {
          return reject(err);
        }
        resolve(response);
      },
    );
  });
}

async function replicateLog(targetIndex: number) {
  let successCount = 1;
  const responses = await Promise.allSettled(
    PEERS.map((peer) => sendAppendEntriesToPeer(peer)),
  );

  responses.forEach((result, index) => {
    const peer = PEERS[index];
    if (result.status === "fulfilled") {
      const response = result.value;
      if (response.term > currentTerm) {
        currentTerm = response.term;
        role = "FOLLOWER";
        votedFor = null;
        leaderId = null;
        saveMetadata();
        return;
      }
      if (response.success) {
        successCount += 1;
        nextIndex[peer.id] = Math.max(
          nextIndex[peer.id] ?? 1,
          response.matchIndex + 1,
        );
        matchIndex[peer.id] = Math.max(
          matchIndex[peer.id] ?? 0,
          response.matchIndex,
        );
      } else {
        nextIndex[peer.id] = Math.max(1, response.conflictIndex);
      }
    }
  });

  if (successCount >= quorumSize()) {
    lastLeaderAck = Date.now();
    return true;
  }
  return false;
}

function broadcastHeartbeats() {
  if (role !== "LEADER") return;

  Promise.allSettled(PEERS.map((peer) => sendAppendEntriesToPeer(peer))).then(
    (results) => {
      let successCount = 1;
      for (let i = 0; i < results.length; i += 1) {
        const result = results[i];
        const peer = PEERS[i];
        if (result.status !== "fulfilled") continue;
        const response = result.value;
        if (response.term > currentTerm) {
          currentTerm = response.term;
          role = "FOLLOWER";
          votedFor = null;
          leaderId = null;
          saveMetadata();
          return;
        }
        if (response.success) {
          successCount += 1;
          nextIndex[peer.id] = Math.max(
            nextIndex[peer.id] ?? 1,
            response.matchIndex + 1,
          );
          matchIndex[peer.id] = Math.max(
            matchIndex[peer.id] ?? 0,
            response.matchIndex,
          );
        } else {
          nextIndex[peer.id] = Math.max(1, response.conflictIndex);
        }
      }
      if (successCount >= quorumSize()) {
        lastLeaderAck = Date.now();
      }
    },
  );
}

function beginElection() {
  if (role === "LEADER") return;
  if (Date.now() - lastHeartbeat <= electionTimeoutMs) return;
  electionTimeoutMs = randomElectionTimeout();
  startElection();
}

function startServer() {
  safeDir(DATA_DIR);
  loadMetadata();
  loadWal();
  commitIndex = getLastLogIndex();
  applyLogToStateMachine();

  const server = new grpc.Server();
  server.addService(raftProto.raftkv.RaftInternal.service, {
    RequestVote: requestVote,
    AppendEntries: appendEntries,
  });

  server.bindAsync(
    `0.0.0.0:${GRPC_PORT}`,
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error("gRPC bind failed", err);
        process.exit(1);
      }
      server.start();
      console.log(`gRPC listening on ${port}`);
    },
  );

  app.listen(PUBLIC_PORT, () => {
    console.log(`HTTP listening on ${PUBLIC_PORT}`);
  });

  setInterval(() => {
    if (role === "LEADER") {
      broadcastHeartbeats();
    }
    beginElection();
  }, heartbeatIntervalMs);
}

startServer();
