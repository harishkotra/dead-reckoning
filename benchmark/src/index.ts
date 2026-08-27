// benchmark/src/index.ts
//
// Comparison benchmark: full retry cost vs. resumption cost for a dropped
// gRPC server-stream. This is the "Additional Resources" deliverable promised
// in the conference proposal.
//
// Two modes:
//   - REAL (default): uses the .env OpenAI-compatible credentials
//     (OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL) — the same endpoint the
//     demo runs against. Real latency, real non-determinism, honest numbers.
//     Sweeps "drop after D tokens" for D in BENCH_DROPS (default 50,200,400).
//   - MOCK (--mock): boots a deterministic local OpenAI-compatible endpoint so
//     the benchmark is reproducible in CI and in the README without a live LLM.
//
// For each drop point the benchmark measures, with the real gRPC server and
// journal (the exact code the demo uses):
//
//   - retry : drop at D, then start a FRESH session from 0 (what a naive
//             client does — re-pay the full inference, re-receive everything,
//             and get a *different* answer because sampling is non-deterministic).
//   - resume: drop at D, reconnect the SAME session with resume_from = D; the
//             server replays the journaled tokens from memory (≈instant) and
//             generates only the continuation tail.
//
// Reported per scenario: time-to-first-token after reconnect (the perceived
// gap — the single most user-visible number), total wall-clock from reconnect
// to completion, tokens received, and NEW inference tokens generated (measured
// from the journal, so the saving is real, not analytic). Non-determinism note:
// retry's fresh output differs from the original; resume's replayed portion is
// bit-identical (journaled) and only its continuation is newly generated.
//
// Run: npm run bench              (real .env endpoint)
//      npm run bench -- --mock    (deterministic local endpoint)

import * as dotenv from "dotenv";
import * as grpc from "@grpc/grpc-js";
import * as http from "node:http";
import { fileURLToPath } from "node:url";
import { TokenJournal } from "../../server/src/journal.js";
import { loadInferenceProto, makeServiceImplementation } from "../../server/src/handler.js";
import type { GenerateRequest, GenerateResponse } from "../../server/generated/inference.js";
import type { ClientReadableStream } from "@grpc/grpc-js/build/src/call.js";

// Load .env from the repository root so real mode picks up the LLM creds.
dotenv.config({ path: fileURLToPath(new URL("../../.env", import.meta.url)) });

/** Milliseconds between mock tokens (mock mode only). */
const TOKEN_DELAY_MS = 2;

/** Mock stream length (mock mode only). */
const MOCK_LENGTH = 2000;

/** Default drop-after-N-tokens sweep for real mode. */
const DEFAULT_REAL_DROPS = [50, 200, 400];

/** Default drop-after-N-tokens sweep for mock mode. */
const DEFAULT_MOCK_DROPS = [500, 1000, 1800];

/** The benchmark prompt (same shape as the demo's long generation). */
const DEFAULT_PROMPT =
  "Write a detailed 500-word story about a lighthouse keeper during a storm";

/** Timestamp-prefixed stdout logger for benchmark lifecycle events. */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [bench] ${message}\n`);
}

/** Find a free TCP port by binding a listener on :0. */
async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = http.createServer();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const address = srv.address();
      if (address && typeof address === "object") {
        const port = address.port;
        srv.close(() => resolve(port));
      } else {
        reject(new Error("could not determine free port"));
      }
    });
  });
}

/**
 * A deterministic mock OpenAI-compatible endpoint (mock mode only). The model
 * name encodes the token count ("bench:<N>") so the benchmark can control
 * stream length. Every request emits exactly N tokens, then a final chunk.
 */
function startMockLlm(port: number): http.Server {
  const server = http.createServer((req, res) => {
    if (req.method !== "POST" || !req.url?.startsWith("/v1/chat/completions")) {
      res.writeHead(404).end();
      return;
    }
    let body = "";
    req.on("data", (chunk: Buffer) => {
      body += chunk.toString();
    });
    req.on("end", () => {
      let tokenCount = MOCK_LENGTH;
      try {
        const parsed = JSON.parse(body) as { model?: string };
        const match = parsed.model?.match(/^bench:(\d+)$/);
        if (match) {
          tokenCount = Number.parseInt(match[1], 10);
        }
      } catch {
        // Malformed body: fall back to the default token count.
      }

      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });

      let i = 0;
      const timer = setInterval(() => {
        if (i >= tokenCount) {
          res.write(`data: {"choices":[{"delta":{"content":""}}]}\n\n`);
          res.write("data: [DONE]\n\n");
          res.end();
          clearInterval(timer);
          return;
        }
        const chunk = {
          id: "chatcmpl-bench",
          object: "chat.completion.chunk",
          created: Date.now(),
          model: "bench",
          choices: [{ index: 0, delta: { content: `tok${i} ` }, finish_reason: null }],
        };
        res.write(`data: ${JSON.stringify(chunk)}\n\n`);
        i += 1;
      }, TOKEN_DELAY_MS);

      // Stop emitting if the client disconnects mid-stream. This must be
      // res.on('close') — req.on('close') fires as soon as the request body is
      // consumed (before the response finishes), which would kill the interval
      // before it ever emits.
      res.on("close", () => clearInterval(timer));
    });
  });
  server.listen(port, "127.0.0.1");
  return server;
}

/** A client wrapper around the InferenceService (Generate only, for the bench). */
interface BenchClient {
  Generate(request: GenerateRequest): ClientReadableStream<GenerateResponse>;
}

/**
 * Model a client drop the way the real demo does:
 *   1. the client receives `limit` tokens (indices 0..limit-1);
 *   2. then it "goes away" for `gapMs` while the gRPC stream stays ALIVE and
 *      the server keeps journaling — in the real system the bridge keeps the
 *      stream open after the browser's WebSocket closes, which is exactly why
 *      the journal grows past the drop point during the gap;
 *   3. after the gap the stream is torn down.
 * Returns { received, journaledAtDrop } where journaledAtDrop is the last
 * journaled index the server reached during the gap (read from the journal by
 * the caller, since the stream object doesn't expose it here).
 */
function receiveThenDrop(
  client: BenchClient,
  request: GenerateRequest,
  limit: number,
  gapMs: number,
): Promise<number> {
  return new Promise((resolve) => {
    const stream = client.Generate(request);
    let received = 0;
    let done = false;
    const finish = (count: number): void => {
      if (!done) {
        done = true;
        stream.cancel();
        resolve(count);
      }
    };
    // Keep consuming past the drop point so flow control keeps the server
    // journaling during the gap (as the bridge does after a WS close).
    stream.on("data", (response: GenerateResponse) => {
      if (response.isFinal) {
        finish(received);
        return;
      }
      received += 1;
    });
    stream.on("error", () => {
      // Cancellation surfaces as an error on the client; that is expected.
      finish(received);
    });
    // Wait until the client has rendered `limit` tokens, then keep the stream
    // alive for `gapMs` more (journal keeps growing), then tear it down. The
    // client's resume point is `limit` (it rendered indices 0..limit-1); the
    // extra tokens drained during the gap are bridge-side only.
    const gapTimer = setInterval(() => {
      if (received >= limit) {
        clearInterval(gapTimer);
        setTimeout(() => finish(limit), gapMs);
      }
    }, 10);
  });
}

/**
 * Run one Generate call to completion, measuring time-to-first-token after
 * the call was issued and total wall-clock to completion.
 */
function timeToFirstToken(
  client: BenchClient,
  request: GenerateRequest,
): Promise<{ firstTokenMs: number; totalMs: number; tokens: number }> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const stream = client.Generate(request);
    let firstTokenMs = -1;
    let tokens = 0;
    stream.on("data", (response: GenerateResponse) => {
      if (firstTokenMs < 0) {
        firstTokenMs = Date.now() - started;
      }
      if (!response.isFinal) {
        tokens += 1;
      }
    });
    stream.on("error", reject);
    stream.on("end", () => {
      resolve({ firstTokenMs, totalMs: Date.now() - started, tokens });
    });
  });
}

interface Row {
  drop: number;
  strategy: string;
  firstTokenMs: number;
  totalMs: number;
  receivedTokens: number;
  newInferenceTokens: number;
  inferenceSaved: number;
}

/** Run the retry-vs-resume comparison for one drop point D. */
async function runScenario(
  client: BenchClient,
  journal: TokenJournal,
  prompt: string,
  drop: number,
  gapMs: number,
  scenarioTag: string,
): Promise<Row[]> {
  const rows: Row[] = [];

  // --- RETRY: drop at D, then start a FRESH session from 0. ---
  const retrySession = `${scenarioTag}-retry`;
  const retryFirst = await receiveThenDrop(
    client,
    { prompt, sessionId: retrySession, resumeFromToken: 0 },
    drop,
    gapMs,
  );
  // A naive client re-runs from scratch with a new request/session.
  const retryFreshSession = `${scenarioTag}-retry-fresh`;
  const retrySecond = await timeToFirstToken(
    client,
    { prompt, sessionId: retryFreshSession, resumeFromToken: 0 },
  );
  const retryNewInference = journal.lastIndex(retryFreshSession) + 1;
  rows.push({
    drop,
    strategy: "retry",
    firstTokenMs: retrySecond.firstTokenMs,
    totalMs: retrySecond.totalMs,
    receivedTokens: retryFirst + retrySecond.tokens,
    newInferenceTokens: retryNewInference,
    inferenceSaved: 0,
  });

  // --- RESUME: drop at D, reconnect the SAME session from index D. ---
  const resumeSession = `${scenarioTag}-resume`;
  const resumeFirst = await receiveThenDrop(
    client,
    { prompt, sessionId: resumeSession, resumeFromToken: 0 },
    drop,
    gapMs,
  );
  // Tokens the server has journaled (replayable from memory) at resume time.
  const journaledAtResume = journal.count(resumeSession);
  const resumeSecond = await timeToFirstToken(
    client,
    { prompt: "", sessionId: resumeSession, resumeFromToken: drop },
  );
  const resumeTotalGenerated = journal.lastIndex(resumeSession) + 1;
  // New inference after reconnect = what was generated beyond the journaled
  // replay. Journaled tokens are served from memory, not re-generated.
  const resumeContinuation = Math.max(0, resumeTotalGenerated - journaledAtResume);
  rows.push({
    drop,
    strategy: "resume",
    firstTokenMs: resumeSecond.firstTokenMs,
    totalMs: resumeSecond.totalMs,
    receivedTokens: resumeFirst + resumeSecond.tokens,
    newInferenceTokens: resumeContinuation,
    inferenceSaved: retryNewInference - resumeContinuation,
  });

  return rows;
}

async function main(): Promise<void> {
  const isMock = process.argv.includes("--mock");
  const grpcPort = await freePort();

  // Mock mode overrides the LLM env; real mode leaves .env untouched.
  let mock: http.Server | null = null;
  if (isMock) {
    const llmPort = await freePort();
    mock = startMockLlm(llmPort);
    process.env.OPENAI_BASE_URL = `http://127.0.0.1:${llmPort}/v1`;
    process.env.OPENAI_API_KEY = "bench";
    process.env.OPENAI_MODEL = `bench:${MOCK_LENGTH}`;
  }
  process.env.GRPC_PORT = String(grpcPort);

  const prompt = process.env.BENCH_PROMPT ?? DEFAULT_PROMPT;
  // Parse "BENCH_DROPS=50,200" (comma-separated) or fall back to the defaults.
  const rawDrops = process.env.BENCH_DROPS;
  const drops = rawDrops
    ? rawDrops
        .split(",")
        .map((s) => Number.parseInt(s.trim(), 10))
        .filter((n) => !Number.isNaN(n) && n > 0)
    : isMock
      ? DEFAULT_MOCK_DROPS
      : DEFAULT_REAL_DROPS;
  // How long the client stays "disconnected" after receiving the target token
  // while the server keeps generating into the journal (ms).
  const gapMs = Number.parseInt(process.env.BENCH_GAP_MS ?? "1000", 10);

  const journal = new TokenJournal(60_000);
  const prompts = new Map<string, string>();
  const { service } = loadInferenceProto();
  const server = new grpc.Server();
  server.addService(service, makeServiceImplementation(journal, prompts));
  await new Promise<void>((resolve, reject) => {
    server.bindAsync(`127.0.0.1:${grpcPort}`, grpc.ServerCredentials.createInsecure(), (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });

  const { clientCtor } = loadInferenceProto();
  const raw = new clientCtor(`127.0.0.1:${grpcPort}`, grpc.credentials.createInsecure());
  const client = raw as unknown as BenchClient;

  log(
    `${isMock ? "MOCK" : "REAL"} mode: gRPC server on :${grpcPort}, ` +
      `model=${process.env.OPENAI_MODEL}, drops=[${drops.join(",")}], gap=${gapMs}ms`,
  );

  const rows: Row[] = [];
  for (const drop of drops) {
    const scenarioRows = await runScenario(client, journal, prompt, drop, gapMs, `bench-${drop}`);
    rows.push(...scenarioRows);
    log(`drop after ${drop} tokens done`);
  }

  // Print the comparison table (markdown, for the README).
  const lines: string[] = [];
  lines.push(
    "| drop after | strategy | reconnect→1st token | reconnect→complete | tokens received | new inference tokens | inference saved |",
  );
  lines.push(
    "|---|----------|-------------------|--------------------|-----------------|----------------------|-----------------|",
  );
  for (const row of rows) {
    lines.push(
      `| ${row.drop} | ${row.strategy} | ${row.firstTokenMs} ms | ${row.totalMs} ms | ` +
        `${row.receivedTokens} | ${row.newInferenceTokens} | ` +
        `${row.inferenceSaved > 0 ? `−${row.inferenceSaved}` : "—"} |`,
    );
  }
  const table = lines.join("\n");
  process.stdout.write(`\n${table}\n`);

  // Save the table for the README, resolved relative to this module so the
  // cwd never matters. Real-mode numbers are environment-dependent.
  const { writeFileSync } = await import("node:fs");
  const { join, dirname } = await import("node:path");
  const resultsPath = join(dirname(fileURLToPath(import.meta.url)), "..", "results.md");
  writeFileSync(
    resultsPath,
    `> Generated by \`npm run bench${isMock ? " -- --mock" : ""}\` on ` +
      `${new Date().toISOString()} — ${isMock ? "deterministic mock endpoint" : `real endpoint ${process.env.OPENAI_BASE_URL} / ${process.env.OPENAI_MODEL}`}.\n\n${table}\n`,
    "utf8",
  );

  // Clean shutdown.
  raw.close();
  server.tryShutdown(() => {
    journal.dispose();
    mock?.close();
    log("benchmark complete");
  });
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  log(`benchmark failed: ${message}`);
  process.exit(1);
});
