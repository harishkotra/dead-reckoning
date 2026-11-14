// server/handler.ts
//
// The protocol handlers shared by the gRPC server (server/index.ts) and the
// browser bridge (server/bridge.ts). Both talk to the same InferenceService,
// so the Generate/Ack/Cancel logic, the proto loading, and the service
// definition live here once.
//
// Generate implements the resumable-stream protocol as ATTACH-TO-A-JOB:
//   - session unknown          -> start a generation job, stream from token 0
//   - job still running        -> replay the journal from the cursor, then
//                                 follow the SAME run live (no second model call)
//   - cursor below the window  -> FAILED_PRECONDITION + metadata, never a silent gap
//   - run already completed    -> replay to the end and finish; no model call
//   - run unfinished, no job   -> the process that owned it died: continue from
//                                 the journal tail (non-deterministic by nature)
//
// A dropped call is a DETACH. Only Cancel ends generation.

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { TokenJournal } from "./journal.js";
import { GenerationJob } from "./job.js";
import type {
  GenerateRequest,
  GenerateResponse,
  AckRequest,
  AckResponse,
  CancelRequest,
  CancelResponse,
} from "../generated/inference.js";
import type { ServerWritableStream, handleUnaryCall } from "@grpc/grpc-js/build/src/server-call.js";
import type { ClientReadableStream } from "@grpc/grpc-js/build/src/call.js";

/** Path to the proto file, resolved from this module. */
const PROTO_PATH = join(dirname(fileURLToPath(import.meta.url)), "../../proto/inference.proto");

/** How much journal text a post-restart continuation is shown. */
const CONTINUATION_TAIL_CHARS = 4000;

/** Timestamp-prefixed stdout logger for protocol lifecycle events. */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [grpc-server] ${message}\n`);
}

/** The gRPC client surface the bridge uses to talk to the server. */
export interface InferenceClient {
  Generate(request: GenerateRequest): ClientReadableStream<GenerateResponse>;
  Ack(request: AckRequest): Promise<AckResponse>;
  Cancel(request: CancelRequest): Promise<CancelResponse>;
}

/** A typed client constructor produced by proto-loader. */
type InferenceClientCtor = new (address: string, credentials: grpc.ChannelCredentials) => {
  Generate(request: GenerateRequest): ClientReadableStream<GenerateResponse>;
  Ack(request: AckRequest, callback: grpc.requestCallback<AckResponse>): grpc.ClientUnaryCall;
  Cancel(request: CancelRequest, callback: grpc.requestCallback<CancelResponse>): grpc.ClientUnaryCall;
  close(): void;
};

/**
 * Create a gRPC client for the InferenceService and wrap the callback-based
 * unary methods into promises, matching the InferenceClient surface.
 */
export function createInferenceClient(address: string): InferenceClient & { close(): void } {
  const { clientCtor } = loadInferenceProto();
  const raw = new clientCtor(address, grpc.credentials.createInsecure());
  const unary = <Req, Res>(method: (request: Req, cb: grpc.requestCallback<Res>) => grpc.ClientUnaryCall) =>
    (request: Req): Promise<Res> =>
      new Promise<Res>((resolve, reject) => {
        method.call(raw, request, (error, response) => {
          if (error) {
            reject(error);
          } else if (!response) {
            reject(new Error("empty unary response"));
          } else {
            resolve(response);
          }
        });
      });
  return {
    Generate: (request) => raw.Generate(request),
    Ack: unary<AckRequest, AckResponse>((request, cb) => raw.Ack(request, cb)),
    Cancel: unary<CancelRequest, CancelResponse>((request, cb) => raw.Cancel(request, cb)),
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
  // builds one from the service definition, with the same method surface.
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
  await send(call, { token: "", tokenIndex: finalIndex, isFinal: true, replayed: false });
  call.end();
}

/**
 * Attach one call to a generation job: replay the journal from the client's
 * cursor, then follow the live run. Tokens produced during the replay are
 * buffered, so nothing can slip through the gap between "read the journal" and
 * "attach". Resolves when the job finishes or the call dies — in the latter
 * case the job is left running, which is the entire point.
 */
async function attachAndPump(
  job: GenerationJob,
  journal: TokenJournal,
  sessionId: string,
  fromIndex: number,
  call: ServerWritableStream<GenerateRequest, GenerateResponse>,
): Promise<void> {
  const buffered: GenerateResponse[] = [];
  let closed = false;
  let live = false;
  let onClosed: (() => void) | null = null;

  const detachJob = job.attach((token, index) => {
    if (closed) {
      return;
    }
    const message: GenerateResponse = { token, tokenIndex: index, isFinal: false, replayed: false };
    if (live) {
      void send(call, message).catch(() => drop(message.tokenIndex));
    } else {
      buffered.push(message);
    }
  });

  const drop = (index: number): void => {
    if (closed) {
      return;
    }
    closed = true;
    detachJob();
    onClosed?.();
    log(`session "${sessionId}" call dropped at token ${index}; generation continues in the journal`);
  };

  try {
    const replayed = journal.read(sessionId, fromIndex);
    for (let i = 0; i < replayed.length && !closed; i += 1) {
      try {
        await send(call, { token: replayed[i], tokenIndex: fromIndex + i, isFinal: false, replayed: true });
      } catch {
        drop(fromIndex + i);
      }
    }
    if (replayed.length > 0) {
      log(`session "${sessionId}" replayed ${replayed.length} journaled token(s) from index ${fromIndex}`);
    }
    live = true;
    for (const message of buffered.splice(0)) {
      if (closed) {
        return;
      }
      void send(call, message).catch(() => drop(message.tokenIndex));
    }
    if (closed || !job.running) {
      return;
    }
    await new Promise<void>((resolve) => {
      const off = job.onDone(() => {
        off();
        resolve();
      });
      onClosed = () => {
        off();
        resolve();
      };
    });
    if (closed) {
      return;
    }
    await finish(call, journal.lastIndex(sessionId));
  } finally {
    detachJob();
  }
}

/**
 * The Generate RPC handler. Attaches the caller to a session's generation job,
 * replaying the journal from the client's cursor where possible.
 */
function generateHandler(
  journal: TokenJournal,
  jobs: Map<string, GenerationJob>,
  call: ServerWritableStream<GenerateRequest, GenerateResponse>,
): void {
  const { prompt, sessionId, resumeFromToken } = call.request;

  /** Start a job, register it, and mark the session final when the run ends. */
  const startJob = (startIndex: number, effectivePrompt: string, continueFrom = ""): GenerationJob => {
    const job = new GenerationJob(journal, sessionId, effectivePrompt, startIndex, continueFrom);
    jobs.set(sessionId, job);
    void job.run().finally(() => {
      journal.markFinal(sessionId, journal.lastIndex(sessionId));
      jobs.delete(sessionId);
    });
    return job;
  };

  void (async () => {
    try {
      const live = jobs.get(sessionId);
      if (live?.running) {
        log(`session "${sessionId}" ATTACH at token ${resumeFromToken} (journal head ${journal.lastIndex(sessionId)})`);
        await attachAndPump(live, journal, sessionId, resumeFromToken, call);
        return;
      }

      if (!journal.has(sessionId)) {
        log(`session "${sessionId}" START fresh inference from token 0`);
        journal.setPrompt(sessionId, prompt);
        await attachAndPump(startJob(0, prompt), journal, sessionId, 0, call);
        return;
      }

      const first = journal.firstIndex(sessionId);
      if (first >= 0 && resumeFromToken < first) {
        // Never paper over a hole: tell the client to resync.
        const metadata = new grpc.Metadata();
        metadata.set("first-retained-index", String(first));
        const error = Object.assign(
          new Error(
            `resume_from_token ${resumeFromToken} is below the retained window ` +
              `(first retained index ${first}); resync required`,
          ),
          { code: grpc.status.FAILED_PRECONDITION, metadata },
        );
        log(`session "${sessionId}" REJECTED resume at ${resumeFromToken} (window starts at ${first})`);
        call.destroy(error);
        return;
      }

      const final = journal.finalIndex(sessionId);
      if (final >= 0) {
        log(`session "${sessionId}" replaying completed run to token ${final}`);
        const tokens = journal.read(sessionId, resumeFromToken);
        for (let i = 0; i < tokens.length; i += 1) {
          await send(call, { token: tokens[i], tokenIndex: resumeFromToken + i, isFinal: false, replayed: true });
        }
        await finish(call, final);
        return;
      }

      // Session on disk, no live job: the process that owned the run is gone.
      const start = journal.lastIndex(sessionId) + 1;
      log(`session "${sessionId}" CONTINUING after restart at token ${start} (continuation is not bit-identical)`);
      const resumed = startJob(start, journal.prompt(sessionId) || prompt, journal.text(sessionId, CONTINUATION_TAIL_CHARS));
      await attachAndPump(resumed, journal, sessionId, resumeFromToken, call);
    } catch (error) {
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
 * The Cancel RPC handler. The only way to stop paying for generation.
 */
function cancelHandler(
  journal: TokenJournal,
  jobs: Map<string, GenerationJob>,
  call: grpc.ServerUnaryCall<CancelRequest, CancelResponse>,
  callback: grpc.sendUnaryData<CancelResponse>,
): void {
  const { sessionId } = call.request;
  const job = jobs.get(sessionId);
  if (!job?.running) {
    callback(null, {
      sessionId,
      cancelledAtToken: journal.lastIndex(sessionId),
      wasRunning: false,
    });
    return;
  }
  job.cancel();
  jobs.delete(sessionId);
  journal.markFinal(sessionId, journal.lastIndex(sessionId));
  const stoppedAt = journal.lastIndex(sessionId);
  log(`session "${sessionId}" CANCELLED at token ${stoppedAt}`);
  callback(null, { sessionId, cancelledAtToken: stoppedAt, wasRunning: true });
}

/**
 * Build the full service implementation (Generate + Ack + Cancel) bound to a
 * journal. The jobs map lives here: it is the set of generation runs this
 * process currently owns, and it deliberately outlives any single call.
 */
export function makeServiceImplementation(journal: TokenJournal): {
  Generate: (call: ServerWritableStream<GenerateRequest, GenerateResponse>) => void;
  Ack: handleUnaryCall<AckRequest, AckResponse>;
  Cancel: handleUnaryCall<CancelRequest, CancelResponse>;
} {
  const jobs = new Map<string, GenerationJob>();
  return {
    Generate: (call) => generateHandler(journal, jobs, call),
    Ack: (call, callback) => ackHandler(journal, call, callback),
    Cancel: (call, callback) => cancelHandler(journal, jobs, call, callback),
  };
}
