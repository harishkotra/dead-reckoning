// client/index.ts
//
// A standalone gRPC client for Dead Reckoning — the terminal tool that proves
// resumption works without the browser. It connects straight to the gRPC
// server (no bridge), calls Generate, and prints every token inline.
//
// This client is the "dead reckoning" device: it tracks the last received
// token_index in memory and, when a stream drops or ends, reports exactly
// where to resume. It also sends PERIODIC ACKs (every --ack-every tokens) so
// the server can trim journaled tokens the client no longer needs — the
// protocol's memory-bounding mechanism.
//
// Usage:
//   npm run dev --workspace=client -- --prompt "..." --session-id demo --resume-from 0
//   SERVER_ADDRESS=localhost:50051 node dist/index.js --prompt "..." --session-id demo
//
// Resume semantics: --resume-from N means "give me tokens starting at index N".
// After a drop at token K, resume with --resume-from K+1 (the next token you
// haven't received). The server replays from its journal and continues without
// re-running inference.

import * as dotenv from "dotenv";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import type { GenerateRequest, GenerateResponse, AckRequest, AckResponse } from "../generated/inference.js";
import type { ClientReadableStream } from "@grpc/grpc-js/build/src/call.js";

dotenv.config({ path: fileURLToPath(new URL("../../.env", import.meta.url)) });

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROTO_PATH = join(__dirname, "../../proto/inference.proto");

/** Timestamp-prefixed stdout logger for client lifecycle events. */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [client] ${message}\n`);
}

/** Parse CLI args into a typed options object. */
interface CliOptions {
  prompt: string;
  sessionId: string;
  resumeFrom: number;
  /** Send an Ack every N tokens received (0 disables periodic ACKs). */
  ackEvery: number;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { prompt: "", sessionId: "", resumeFrom: 0, ackEvery: 25 };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = (): string => {
      const value = argv[i + 1];
      if (value === undefined) {
        throw new Error(`Missing value for ${arg}`);
      }
      i += 1;
      return value;
    };
    if (arg === "--prompt") {
      options.prompt = next();
    } else if (arg === "--session-id") {
      options.sessionId = next();
    } else if (arg === "--resume-from") {
      options.resumeFrom = Number.parseInt(next(), 10);
      if (Number.isNaN(options.resumeFrom) || options.resumeFrom < 0) {
        throw new Error(`--resume-from must be a non-negative integer`);
      }
    } else if (arg === "--ack-every") {
      options.ackEvery = Number.parseInt(next(), 10);
      if (Number.isNaN(options.ackEvery) || options.ackEvery < 0) {
        throw new Error(`--ack-every must be a non-negative integer (0 disables)`);
      }
    } else if (arg === "--help" || arg === "-h") {
      process.stdout.write(
        [
          "Dead Reckoning gRPC client",
          "",
          "Usage:",
          "  --prompt \"...\"     the prompt to send",
          "  --session-id ID     the session to generate into / resume",
          "  --resume-from N     resume from token index N (0 = fresh)",
          "  --ack-every N       send an Ack every N tokens (default 25, 0 disables)",
          "",
          "Environment:",
          "  SERVER_ADDRESS      gRPC server address (default localhost:50051)",
          "",
        ].join("\n"),
      );
      process.exit(0);
    }
  }
  if (!options.sessionId) {
    throw new Error("--session-id is required");
  }
  // --prompt is optional: on a resume the server replays from its journal and
  // continues with the session's stored prompt, so a fresh prompt is only
  // needed for brand-new sessions.
  return options;
}

/** The client surface: Generate (streaming) + Ack (promise-wrapped unary). */
interface ClientSurface {
  Generate(request: GenerateRequest): ClientReadableStream<GenerateResponse>;
  Ack(request: AckRequest): Promise<AckResponse>;
  close(): void;
}

/** Create the gRPC client for the InferenceService (Generate + Ack). */
function createClient(address: string): ClientSurface {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(packageDefinition) as unknown as {
    inference: {
      InferenceService: new (address: string, credentials: grpc.ChannelCredentials) => {
        Generate(request: GenerateRequest): ClientReadableStream<GenerateResponse>;
        Ack(request: AckRequest, callback: grpc.requestCallback<AckResponse>): grpc.ClientUnaryCall;
        close(): void;
      };
    };
  };
  const raw = new proto.inference.InferenceService(address, grpc.credentials.createInsecure());
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

/**
 * Run one Generate call. Prints every token inline (process.stdout.write, not
 * console.log — no newlines), tracks the last token_index, sends periodic ACKs
 * so the server can trim its journal, and prints a summary on stream end or
 * error showing exactly where the client stopped.
 */
function run(options: CliOptions, address: string): Promise<void> {
  return new Promise((resolve) => {
    const client = createClient(address);
    // lastIndex = index of the last RENDERED token. The final marker carries
    // an index one past the end and is not rendered, so it must not advance it.
    let lastIndex = options.resumeFrom - 1;
    let finalReceived = false;
    let acksSent = 0;
    // Tokens recovered from the journal after this call attached (visible as
    // dim text) versus tokens that came live off the model.
    let replayedCount = 0;

    log(
      `calling Generate session="${options.sessionId}" resume_from=${options.resumeFrom} ` +
        `ack_every=${options.ackEvery}`,
    );
    const stream = client.Generate({
      prompt: options.prompt,
      sessionId: options.sessionId,
      resumeFromToken: options.resumeFrom,
    } satisfies GenerateRequest);

    /** Send a periodic Ack for the current position, fire-and-forget. */
    const maybeAck = (): void => {
      if (finalReceived || options.ackEvery <= 0 || lastIndex < 0) {
        return;
      }
      if ((lastIndex + 1) % options.ackEvery !== 0) {
        return;
      }
      acksSent += 1;
      void client
        .Ack({ sessionId: options.sessionId, ackedTokenIndex: lastIndex } satisfies AckRequest)
        .then((response) => {
          log(
            `ack #${acksSent} session="${options.sessionId}" acked=${response.ackedTokenIndex} ` +
              `retained=${response.journaledTokenCount} first=${response.firstRetainedIndex}`,
          );
        })
        .catch((error: unknown) => {
          const message = error instanceof Error ? error.message : String(error);
          // A late Ack racing the stream's close is expected noise, not a bug.
          if (finalReceived || message.includes("UNAVAILABLE")) {
            return;
          }
          log(`ack #${acksSent} failed: ${message}`);
        });
    };

    stream.on("data", (response: GenerateResponse) => {
      if (response.isFinal) {
        finalReceived = true;
        return;
      }
      lastIndex = response.tokenIndex;
      // Replayed tokens are printed dim so the terminal shows the seam between
      // what the journal recovered and what the model produced live.
      if (response.replayed) {
        replayedCount += 1;
        process.stdout.write(`\x1b[2m${response.token}\x1b[0m`);
      } else {
        process.stdout.write(response.token);
      }
      maybeAck();
    });

    stream.on("error", (error: grpc.ServiceError) => {
      // Stream dropped mid-generation. Report where we stopped — the next token
      // index (lastIndex + 1) is the resume point for the next call.
      log(`stream ERROR for session="${options.sessionId}": ${error.message}`);
      if (error.code === grpc.status.FAILED_PRECONDITION) {
        const first = error.metadata?.get("first-retained-index")?.[0] ?? "unknown";
        log(
          `cursor is below the journal window (first retained index ${first}); ` +
            `resync required — a resume here would silently skip tokens`,
        );
      }
      log(
        `summary: session="${options.sessionId}" last_token_index=${lastIndex} ` +
          `replayed=${replayedCount} acks_sent=${acksSent} ` +
          `(resume with --resume-from ${lastIndex + 1})`,
      );
      client.close();
      resolve();
    });

    stream.on("end", () => {
      process.stdout.write("\n");
      log(
        `stream END for session="${options.sessionId}": ` +
          `last_token_index=${lastIndex} final=${finalReceived ? "yes" : "no"} ` +
          `replayed=${replayedCount} acks_sent=${acksSent} ` +
          `(resume with --resume-from ${lastIndex + 1})`,
      );
      client.close();
      resolve();
    });
  });
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const address = process.env.SERVER_ADDRESS ?? "localhost:50051";
  await run(options, address);
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  log(`fatal: ${message}`);
  process.exit(1);
});
