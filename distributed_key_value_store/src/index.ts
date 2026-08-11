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
