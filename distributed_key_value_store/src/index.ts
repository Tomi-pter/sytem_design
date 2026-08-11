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
