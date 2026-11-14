// server/index.ts
//
// The gRPC server entry point for Dead Reckoning. Boots the gRPC server on
// GRPC_PORT, registers the InferenceService (Generate + Ack) via the shared
// handlers in server/handler.ts, and wires up the token journal.
//
// This file is intentionally thin: all protocol logic (resume vs. fresh,
// journal replay, ACK trimming) lives in handler.ts and journal.ts so the
// browser bridge can reuse the exact same service implementation.
//
// Entry point dispatch: `npm run dev:server` runs this as the gRPC server;
// `npm run dev:bridge -- --bridge` runs server/bridge.ts instead (the browser
// bridge). Both share this workspace's dependencies.
//
// JOURNAL_DB: path to the SQLite journal (default `.journal.db` via .env; set
// to empty to run memory-only). JOURNAL_TTL_MS: idle-session TTL, i.e. how long
// a dropped stream stays resumable — the number the talk's "sizing the journal
// TTL" slide is about: it must exceed the longest plausible client outage, and
// it is bounded by the cost of holding tokens.

import * as dotenv from "dotenv";
import * as grpc from "@grpc/grpc-js";
import { fileURLToPath } from "node:url";
import { TokenJournal } from "./journal.js";
import { loadInferenceProto, makeServiceImplementation } from "./handler.js";

// Load .env from the repository root (two levels up from server/src/).
dotenv.config({ path: fileURLToPath(new URL("../../.env", import.meta.url)) });

/** Timestamp-prefixed stdout logger for server lifecycle events. */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [grpc-server] ${message}\n`);
}

/** Read the gRPC listen port from the environment. */
function loadPort(): number {
  const raw = process.env.GRPC_PORT;
  const port = raw ? Number.parseInt(raw, 10) : 50051;
  if (Number.isNaN(port) || port <= 0) {
    throw new Error(`Invalid GRPC_PORT: ${raw ?? "(unset)"}`);
  }
  return port;
}

/** Build and start the gRPC server. */
function startGrpcServer(): void {
  const port = loadPort();
  const ttlMs = Number.parseInt(process.env.JOURNAL_TTL_MS ?? "600000", 10);
  const journal = new TokenJournal(
    Number.isNaN(ttlMs) ? 600000 : ttlMs,
    process.env.JOURNAL_DB ?? "",
  );

  const { service } = loadInferenceProto();

  const server = new grpc.Server();
  server.addService(service, makeServiceImplementation(journal));

  server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (error, boundPort) => {
    if (error) {
      log(`failed to bind on :${port}: ${error.message}`);
      process.exit(1);
    }
    log(`gRPC server listening on 0.0.0.0:${boundPort}`);
  });

  const shutdown = (): void => {
    log("shutting down gRPC server");
    journal.dispose();
    server.tryShutdown(() => process.exit(0));
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

// Dispatch: `--bridge` runs the browser bridge instead of the gRPC server.
const wantsBridge = process.argv.includes("--bridge");
if (wantsBridge) {
  log("launching bridge mode");
  void import("./bridge.js").then(({ startBridge }) => startBridge());
} else {
  startGrpcServer();
}
