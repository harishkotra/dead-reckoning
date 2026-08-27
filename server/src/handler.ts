// server/handler.ts
//
// The protocol handlers shared by the gRPC server (server/index.ts) and the
// browser bridge (server/bridge.ts). Both talk to the same InferenceService,
// so the Generate/Ack logic, the proto loading, and the service definition
// live here once.
//
// Generate implements the resumable-stream protocol:
//   - fresh request  -> journal + stream every token, is_final on the last
//   - resume request -> replay journaled tokens with ORIGINAL indices, then
//                       continue streaming fresh tokens from the LLM
// Ack implements the periodic-ACK channel: it records the client's confirmed
// watermark so the journal can trim tokens the client no longer needs.

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { TokenJournal } from "./journal.js";
import { loadLlmConfig, streamCompletion } from "./llm.js";
import type { GenerateRequest, GenerateResponse, AckRequest, AckResponse } from "../generated/inference.js";
import type { ServerWritableStream, handleUnaryCall } from "@grpc/grpc-js/build/src/server-call.js";
import type { ClientReadableStream } from "@grpc/grpc-js/build/src/call.js";

/** Path to the proto file, resolved from this module. */
const PROTO_PATH = join(dirname(fileURLToPath(import.meta.url)), "../../proto/inference.proto");

/** Timestamp-prefixed stdout logger for protocol lifecycle events. */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [grpc-server] ${message}\n`);
}

/** The gRPC client surface the bridge uses to talk to the server. */
export interface InferenceClient {
  Generate(request: GenerateRequest): ClientReadableStream<GenerateResponse>;
  Ack(request: AckRequest): Promise<AckResponse>;
}

/** A typed client constructor produced by proto-loader. */
type InferenceClientCtor = new (address: string, credentials: grpc.ChannelCredentials) => {
  Generate(request: GenerateRequest): ClientReadableStream<GenerateResponse>;
  Ack(request: AckRequest, callback: grpc.requestCallback<AckResponse>): grpc.ClientUnaryCall;
  close(): void;
};

/**
 * Create a gRPC client for the InferenceService and wrap the callback-based
 * Ack unary method into a promise, matching the InferenceClient surface.
 */
export function createInferenceClient(address: string): InferenceClient & { close(): void } {
  const { clientCtor } = loadInferenceProto();
  const raw = new clientCtor(address, grpc.credentials.createInsecure());
  return {
    Generate: (request) => raw.Generate(request),
    Ack: (request) =>
      new Promise<AckResponse>((resolve, reject) => {
        raw.Ack(request, (error, response) => {
          if (error) {
            reject(error);
          } else if (!response) {
            reject(new Error("Ack returned an empty response"));
          } else {
            resolve(response);
          }
        });
      }),
    close: () => raw.close(),
  };
}

/** Load the proto once and expose both the service definition and a client ctor. */
export function loadInferenceProto(): {
  service: grpc.ServiceDefinition;
  clientCtor: InferenceClientCtor;
} {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(packageDefinition) as unknown as {
    inference: { InferenceService: { service: grpc.ServiceDefinition } };
  };
  const service = proto.inference.InferenceService.service;
  // proto-loader does not emit a client class; grpc.makeGenericClientConstructor
  // builds one from the service definition, with the same method surface
  // (Generate streaming + Ack unary, both callback-based).
  const clientCtor = grpc.makeGenericClientConstructor(
    service,
    "inference.InferenceService",
  ) as unknown as InferenceClientCtor;
  return { service, clientCtor };
}

/**
 * Stream one GenerateResponse down the gRPC call. Resolves when the write is
 * flushed; rejects if the client has gone away (a dropped stream — exactly the
 * event this project is built to survive).
 */
function send(
  call: ServerWritableStream<GenerateRequest, GenerateResponse>,
  message: GenerateResponse,
): Promise<void> {
  return new Promise((resolve, reject) => {
    call.write(message, (error: Error | null | undefined) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}

/**
 * Send the final marker token and terminate the server-streaming call. Without
 * an explicit end(), the client never receives its stream "end" event and
 * hangs waiting for more tokens.
 */
async function finish(
  call: ServerWritableStream<GenerateRequest, GenerateResponse>,
  finalIndex: number,
): Promise<void> {
  await send(call, { token: "", tokenIndex: finalIndex, isFinal: true });
  call.end();
}

/**
 * Replay every journaled token from `fromIndex` onward with its original
 * token_index. If the requested index has already been ACK-trimmed away, the
 * journal returns an empty window and replay is a no-op (the client only ever
 * resumes at or after its own confirmed watermark, so this cannot lose data).
 */
async function replayJournal(
  journal: TokenJournal,
  call: ServerWritableStream<GenerateRequest, GenerateResponse>,
  sessionId: string,
  fromIndex: number,
): Promise<void> {
  const tokens = journal.read(sessionId, fromIndex);
  for (let i = 0; i < tokens.length; i += 1) {
    // Replay with the ORIGINAL index, so the client's dead-reckoning cursor
    // lands exactly where it was when the stream dropped.
    await send(call, {
      token: tokens[i],
      tokenIndex: fromIndex + i,
      isFinal: false,
    });
  }
}

/**
 * Run a fresh LLM inference, journaling and streaming every token. Resolves
 * with the index of the last token streamed (exclusive end of the journal).
 */
async function streamFresh(
  journal: TokenJournal,
  call: ServerWritableStream<GenerateRequest, GenerateResponse>,
  sessionId: string,
  prompt: string,
  startIndex: number,
): Promise<number> {
  const config = loadLlmConfig();
  let index = startIndex;
  await streamCompletion(config, prompt, async (token: string) => {
    journal.write(sessionId, index, token);
    await send(call, {
      token,
      tokenIndex: index,
      isFinal: false,
    });
    index += 1;
  });
  return index;
}

/**
 * The Generate RPC handler. Decides between "resume from journal" and
 * "fresh inference" per the protocol, and streams the response.
 *
 * Note on prompts: the journal stores only tokens, so the original prompt is
 * kept in a parallel in-memory registry. A resume request that carries no
 * prompt (e.g. from the browser bridge, whose resume message has no prompt
 * field) falls back to the session's stored prompt so fresh inference can
 * continue after the replay.
 */
function generateHandler(
  journal: TokenJournal,
  prompts: Map<string, string>,
  call: ServerWritableStream<GenerateRequest, GenerateResponse>,
): void {
  const { prompt, sessionId, resumeFromToken } = call.request;

  void (async () => {
    try {
      if (resumeFromToken > 0 && journal.has(sessionId)) {
        // The bridge's resume message has no prompt; use the stored one.
        const storedPrompt = prompts.get(sessionId);
        const effectivePrompt = prompt.length > 0 ? prompt : (storedPrompt ?? "");
        const journaled = journal.count(sessionId);
        log(
          `session "${sessionId}" RESUMING from token ${resumeFromToken} ` +
            `(journal retains ${journaled} tokens, last=${journal.lastIndex(sessionId)})`,
        );
        await replayJournal(journal, call, sessionId, resumeFromToken);
        const replayed = Math.max(0, journal.lastIndex(sessionId) - resumeFromToken + 1);
        log(`session "${sessionId}" replayed ${replayed} journaled tokens`);
        const endIndex = await streamFresh(journal, call, sessionId, effectivePrompt, journal.lastIndex(sessionId) + 1);
        await finish(call, endIndex);
        log(`session "${sessionId}" completed after resume at token ${endIndex}`);
      } else {
        const reason = resumeFromToken > 0 ? "session not in journal" : "fresh request";
        log(`session "${sessionId}" STARTING fresh inference (${reason})`);
        prompts.set(sessionId, prompt);
        const endIndex = await streamFresh(journal, call, sessionId, prompt, 0);
        await finish(call, endIndex);
        log(`session "${sessionId}" completed fresh at token ${endIndex}`);
      }
    } catch (error) {
      // A dropped client stream (the "Kill Stream" scenario) surfaces here as a
      // write error. The journal already has every token produced so far, so
      // the session can be resumed later — nothing is lost.
      const err = error instanceof Error ? error : new Error(String(error));
      log(`session "${sessionId}" stream ended with error: ${err.message}`);
      call.destroy(err);
    }
  })();
}

/**
 * The Ack RPC handler. Records the client's confirmed watermark, trims the
 * journal accordingly, and returns the post-ack state so the client (or a
 * crashed client recovering its cursor) can see exactly where it stands.
 */
function ackHandler(
  journal: TokenJournal,
  call: grpc.ServerUnaryCall<AckRequest, AckResponse>,
  callback: grpc.sendUnaryData<AckResponse>,
): void {
  const { sessionId, ackedTokenIndex } = call.request;
  const state = journal.ack(sessionId, ackedTokenIndex);
  if (!state) {
    callback({ code: grpc.status.NOT_FOUND, message: `session "${sessionId}" not in journal` });
    return;
  }
  callback(null, {
    sessionId,
    ackedTokenIndex: state.acked_index,
    journaledTokenCount: state.journaled_token_count,
    firstRetainedIndex: state.first_retained_index,
  });
}

/**
 * Build the full service implementation (Generate + Ack) bound to a journal
 * and prompt registry. The server registers this on its grpc.Server.
 */
export function makeServiceImplementation(
  journal: TokenJournal,
  prompts: Map<string, string>,
): {
  Generate: (call: ServerWritableStream<GenerateRequest, GenerateResponse>) => void;
  Ack: handleUnaryCall<AckRequest, AckResponse>;
} {
  return {
    Generate: (call) => generateHandler(journal, prompts, call),
    Ack: (call, callback) => ackHandler(journal, call, callback),
  };
}
