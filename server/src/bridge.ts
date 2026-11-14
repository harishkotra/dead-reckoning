// server/bridge.ts
//
// The browser bridge: an Express + WebSocket server that lets the browser
// drive the gRPC service. Browsers cannot speak gRPC natively, so this bridge
// translates between WebSocket JSON messages and gRPC calls.
//
// Message protocol (browser -> bridge):
//   { type: "generate", prompt: string, session_id: string }
//   { type: "resume", session_id: string, resume_from_token: number }
//   { type: "ack", session_id: string, acked_token_index: number }
//   { type: "cancel", session_id: string }
//
// Message protocol (bridge -> browser):
//   { type: "token", token, token_index, is_final, replayed, session_id }
//   { type: "journal_state", session_id, token_count }
//   { type: "ack_state", session_id, acked_token_index, journaled_token_count, first_retained_index }
//   { type: "stream_started" | "stream_resumed" | "stream_dropped", ... }
//   { type: "resync_required", session_id, first_retained_index }
//
// The bridge creates a gRPC client per incoming message and forwards every
// response token over the WebSocket. Browser `ack` messages are forwarded to
// the gRPC Ack RPC; the server's post-ack state is relayed back as ack_state.
// If the gRPC stream drops, the bridge logs it and keeps running — it never
// crashes on a disconnected stream, and the session stays resumable via the
// server's journal.
//
// A closed browser socket cancels only this bridge's gRPC CALL, never the
// server-side generation job: the bridge is a listener, so a browser drop means
// "nobody is listening right now", not "stop the model".

import * as dotenv from "dotenv";
import * as grpc from "@grpc/grpc-js";
import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import { fileURLToPath } from "node:url";
import { createInferenceClient, type InferenceClient } from "./handler.js";
import type { GenerateRequest, GenerateResponse, AckRequest, AckResponse } from "../generated/inference.js";
import type { ClientReadableStream } from "@grpc/grpc-js/build/src/call.js";

dotenv.config({ path: fileURLToPath(new URL("../../.env", import.meta.url)) });

/** Timestamp-prefixed stdout logger for bridge lifecycle events. */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [bridge] ${message}\n`);
}

/** A browser -> bridge message. Discriminated on `type`. */
type BridgeRequest =
  | { type: "generate"; prompt: string; session_id: string }
  | { type: "resume"; session_id: string; resume_from_token: number }
  | { type: "ack"; session_id: string; acked_token_index: number }
  | { type: "cancel"; session_id: string };

/** Create a gRPC client for the InferenceService (Generate + Ack). */
function createGrpcClient(address: string): InferenceClient {
  return createInferenceClient(address);
}

/**
 * Send a JSON message to the browser socket, but only if it is still open.
 * After the "Kill Stream" demo the browser has closed its socket while the
 * gRPC stream keeps producing tokens; an unguarded ws.send() would throw and
 * crash the bridge — exactly the failure this project is meant to survive.
 */
function sendToBrowser(socket: WebSocket, message: unknown): void {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}

/**
 * Drive one Generate call for a session and forward every response to the
 * browser as token + journal_state messages. Resolves when the stream ends
 * (including the is_final marker); never throws — a dropped gRPC stream is
 * logged and treated as a normal end of this forwarding loop.
 */
function forwardStream(
  client: InferenceClient,
  request: GenerateRequest,
  socket: WebSocket,
  sessionId: string,
  active: Set<ClientReadableStream<GenerateResponse>>,
): Promise<void> {
  return new Promise((resolve) => {
    const stream = client.Generate(request);
    let lastIndex = request.resumeFromToken;
    active.add(stream);

    stream.on("data", (response: GenerateResponse) => {
      lastIndex = response.tokenIndex;
      // Forward the token to the browser.
      sendToBrowser(socket, {
        type: "token",
        token: response.token,
        token_index: response.tokenIndex,
        is_final: response.isFinal,
        replayed: response.replayed,
        session_id: sessionId,
      });
      // Keep the browser's journal inspector in sync on every token.
      sendToBrowser(socket, {
        type: "journal_state",
        session_id: sessionId,
        token_count: response.tokenIndex + 1,
      });
    });

    stream.on("error", (error: grpc.ServiceError) => {
      active.delete(stream);
      // The gRPC stream dropped (or the upstream LLM failed, or the cursor fell
      // out of the journal window). Log it and move on — the journal on the
      // server already holds every token produced, so the session can be
      // resumed later.
      log(
        `gRPC stream for session "${sessionId}" dropped at token ${lastIndex} ` +
          `(code ${error.code ?? "?"}): ${error.message}`,
      );
      if (error.code === grpc.status.FAILED_PRECONDITION) {
        // Never let the browser accept a silent gap: it must resync.
        const first = error.metadata?.get("first-retained-index")?.[0];
        sendToBrowser(socket, {
          type: "resync_required",
          session_id: sessionId,
          first_retained_index: first ? Number(first) : -1,
        });
      } else {
        sendToBrowser(socket, {
          type: "stream_dropped",
          session_id: sessionId,
          last_token_index: lastIndex,
        });
      }
      resolve();
    });

    stream.on("end", () => {
      active.delete(stream);
      log(`gRPC stream for session "${sessionId}" ended cleanly at token ${lastIndex}`);
      resolve();
    });
  });
}

/** Forward a browser ACK to the gRPC Ack RPC and relay the post-ack state. */
async function handleAck(
  client: InferenceClient,
  socket: WebSocket,
  sessionId: string,
  ackedTokenIndex: number,
): Promise<void> {
  try {
    const response: AckResponse = await client.Ack({
      sessionId,
      ackedTokenIndex,
    } satisfies AckRequest);
    log(
      `ack session="${sessionId}" watermark=${response.ackedTokenIndex} ` +
        `retained=${response.journaledTokenCount} first=${response.firstRetainedIndex}`,
    );
    sendToBrowser(socket, {
      type: "ack_state",
      session_id: sessionId,
      acked_token_index: response.ackedTokenIndex,
      journaled_token_count: response.journaledTokenCount,
      first_retained_index: response.firstRetainedIndex,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    log(`ack for session "${sessionId}" failed: ${message}`);
  }
}

/** Handle one incoming browser message by starting the right gRPC call. */
async function handleMessage(
  client: InferenceClient,
  socket: WebSocket,
  raw: string,
  active: Set<ClientReadableStream<GenerateResponse>>,
): Promise<void> {
  let message: BridgeRequest;
  try {
    message = JSON.parse(raw) as BridgeRequest;
  } catch {
    log(`ignoring non-JSON message from browser: ${raw.slice(0, 80)}`);
    return;
  }

  if (message.type === "generate") {
    const { prompt, session_id: sessionId } = message;
    if (!prompt || !sessionId) {
      log(`ignoring generate message missing prompt/session_id`);
      return;
    }
    log(`browser requested fresh generation for session "${sessionId}"`);
    sendToBrowser(socket, {
      type: "stream_started",
      session_id: sessionId,
      resume_from_token: 0,
    });
    await forwardStream(
      client,
      { prompt, sessionId, resumeFromToken: 0 },
      socket,
      sessionId,
      active,
    );
  } else if (message.type === "resume") {
    const { session_id: sessionId, resume_from_token: resumeFromToken } = message;
    if (!sessionId || typeof resumeFromToken !== "number" || resumeFromToken < 0) {
      log(`ignoring resume message missing session_id/resume_from_token`);
      return;
    }
    log(`browser requested resume for session "${sessionId}" from token ${resumeFromToken}`);
    sendToBrowser(socket, {
      type: "stream_resumed",
      session_id: sessionId,
      resume_from_token: resumeFromToken,
    });
    await forwardStream(
      client,
      { prompt: "", sessionId, resumeFromToken },
      socket,
      sessionId,
      active,
    );
  } else if (message.type === "cancel") {
    const { session_id: sessionId } = message;
    if (!sessionId) {
      log(`ignoring cancel message missing session_id`);
      return;
    }
    try {
      const response = await client.Cancel({ sessionId });
      log(
        `cancel session="${sessionId}" at token ${response.cancelledAtToken} ` +
          `(was running: ${response.wasRunning})`,
      );
      sendToBrowser(socket, {
        type: "cancelled",
        session_id: sessionId,
        cancelled_at_token: response.cancelledAtToken,
        was_running: response.wasRunning,
      });
    } catch (error) {
      const message2 = error instanceof Error ? error.message : String(error);
      log(`cancel for session "${sessionId}" failed: ${message2}`);
    }
  } else if (message.type === "ack") {
    const { session_id: sessionId, acked_token_index: ackedTokenIndex } = message;
    if (!sessionId || typeof ackedTokenIndex !== "number" || ackedTokenIndex < 0) {
      log(`ignoring ack message missing session_id/acked_token_index`);
      return;
    }
    await handleAck(client, socket, sessionId, ackedTokenIndex);
  } else {
    log(`ignoring unknown message type: ${(message as { type?: string }).type ?? "(none)"}`);
  }
}

/** Start the Express + WebSocket bridge. */
export function startBridge(): void {
  const bridgePort = Number.parseInt(process.env.BRIDGE_PORT ?? "3001", 10);
  // The gRPC server address: SERVER_ADDRESS overrides, otherwise derive from
  // GRPC_PORT so a single .env change keeps server and bridge in sync.
  const grpcPort = process.env.GRPC_PORT ?? "50051";
  const serverAddress = process.env.SERVER_ADDRESS ?? `localhost:${grpcPort}`;

  const app = express();
  app.get("/health", (_req, res) => {
    res.json({ ok: true, service: "dead-reckoning-bridge" });
  });

  const httpServer = app.listen(bridgePort, () => {
    log(`bridge listening on http://localhost:${bridgePort} (gRPC -> ${serverAddress})`);
  });

  const wss = new WebSocketServer({ server: httpServer });
  const client = createGrpcClient(serverAddress);

  wss.on("connection", (socket: WebSocket) => {
    log("browser connected");
    // gRPC calls this socket is currently listening to. Closing the socket
    // cancels the CALLS only — the server keeps generating and journaling, so
    // the browser can reconnect and resume from the last token it saw. This is
    // the single most important line in the demo: a dead browser must not be a
    // cancelled inference.
    const active = new Set<ClientReadableStream<GenerateResponse>>();
    socket.on("message", (data) => {
      const raw = data.toString();
      void handleMessage(client, socket, raw, active);
    });
    socket.on("close", () => {
      log(`browser disconnected (cancelling ${active.size} call(s); generation continues)`);
      for (const stream of active) {
        stream.cancel();
      }
      active.clear();
    });
    socket.on("error", (error: Error) => {
      // A client abruptly closing its socket (the "Kill Stream" button) lands
      // here. Log it; the server is unaffected and the session stays resumable.
      log(`browser socket error: ${error.message}`);
    });
  });

  const shutdown = (): void => {
    log("shutting down bridge");
    wss.close();
    httpServer.close(() => process.exit(0));
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

// Allow running directly: `npm run dev:bridge` or `tsx src/bridge.ts`.
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  startBridge();
}
