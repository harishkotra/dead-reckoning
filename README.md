# Dead Reckoning — Resumable gRPC Server-Streaming for LLM Inference

**Built by [Harish Kotra](https://harishkotra.me)** · Check out [other builds](https://dailybuild.xyz)

A production-quality open-source reference implementation of **resumable
streaming over gRPC for LLM token delivery**. When a gRPC server-stream drops
mid-generation, the client reconnects and the stream resumes from the last
confirmed token — **not from scratch**. This is a gap in gRPC's current design
for AI workloads, and this project proves it is solvable with a thin protocol
layer on top of standard gRPC.

The whole stack is TypeScript/Node.js: `@grpc/grpc-js` + `@grpc/proto-loader`
for the wire, a token journal on the server, a lightweight Express + WebSocket
bridge so a browser can drive it, and a raw-CSS React UI. Any OpenAI-compatible
endpoint (Ollama, LM Studio, vLLM, …) works as the inference backend.

---

## Table of Contents

1. [What this project demonstrates](#1-what-this-project-demonstrates)
2. [The core insight: checkpointing, not retrying](#2-the-core-insight-checkpointing-not-retrying)
3. [Technology stack](#3-technology-stack)
4. [Architecture](#4-architecture)
5. [The protocol](#5-the-protocol)
6. [Code walkthrough](#6-code-walkthrough)
7. [Prerequisites](#7-prerequisites)
8. [Installation and running](#8-installation-and-running)
9. [How to trigger the demo manually](#9-how-to-trigger-the-demo-manually)
10. [Configuration via `.env`](#10-configuration-via-env)
11. [Benchmark: retry vs. resume](#11-benchmark-retry-vs-resume)
12. [Prior art](#12-prior-art)
13. [Known limitations](#13-known-limitations)
14. [Forking and contributing](#14-forking-and-contributing)
15. [Repository layout](#15-repository-layout)

---

## 1. What this project demonstrates

Standard gRPC server-streaming has no resumability: if a `Generate` stream is
interrupted, the client has lost every token after the last one it received,
and the only recovery is to re-run the whole inference — which is expensive,
slow, and (for non-deterministic models) may produce a different answer.

Dead Reckoning adds a thin protocol layer on top of stock gRPC. Every response
carries a monotonic `token_index`. The server journals every token it produces,
keyed by `session_id`. A client that drops mid-stream reconnects and sends the
same `session_id` with `resume_from_token` set to the first index it has *not*
received. The server replays the already-generated tokens from its journal
(with their original indices, so the client's stream is gapless and
de-duplicated), then continues streaming fresh tokens from the LLM. Inference
is never re-run, and the client sees a continuous story.

The protocol also includes **periodic ACKs**: during a long stream the client
confirms its position with `Ack(session_id, acked_token_index)`. The server
records the client's confirmed watermark and **trims journaled tokens the
client no longer needs** — keeping memory bounded for very long generations,
and giving a crashed client a way to recover its cursor from the server.

## 2. The core insight: checkpointing, not retrying

This is a **checkpointing problem**, not a **retry problem**.

- *Retry* means: the stream failed, so start over. For an LLM this means
  re-paying the full inference cost and — because sampling is non-deterministic —
  very likely getting a *different* answer. The user sees the story restart and
  diverge.
- *Checkpoint* means: the stream failed, but every token that was produced is
  already recorded. The client only needs to state *where it stopped*; the
  server replays from that checkpoint and continues. Cost is bounded by the
  tokens actually missed, and the output is bit-identical from the resume point
  onward because the journal stores the exact tokens.

gRPC already gives us the primitive we need — server-streaming with ordered
messages — but no checkpointing. The `resume_from_token` field in the request
and the `token_index` field in every response *are* the protocol design. They
turn gRPC into a resumable transport with zero changes to gRPC itself.

## 3. Technology stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Language | **TypeScript / Node.js** (strict mode, no `any`) | End-to-end type safety across the whole monorepo |
| gRPC | **`@grpc/grpc-js`** + **`@grpc/proto-loader`** | The standard pure-JS gRPC implementation |
| Proto tooling | **`ts-proto`** (via `protoc`) | Generates typed message interfaces from `.proto` |
| LLM client | **OpenAI Node SDK** (`stream: true`) | Talks to any OpenAI-compatible endpoint |
| Bridge | **Express** + **`ws`** (WebSocket) | Browsers can't speak gRPC natively; this translates |
| Frontend | **React 18 + Vite** (raw CSS, no UI framework) | Lightweight, dependency-light UI |
| Orchestration | **`concurrently`** + npm workspaces | One command runs server + bridge + web |
| Benchmark | Custom harness in `benchmark/` | Retry-vs-resume comparison against a real LLM |

The repo is an **npm workspace monorepo** with four packages: `server`, `client`,
`web`, and `benchmark`. All share the root `tsconfig.base.json` (strict mode).

## 4. Architecture

```
                 ┌────────────────────────────────────────────────┐
                 │                    Browser                     │
                 │  React (Vite) — TokenStream | JournalInspector │
                 │      WebSocket  ws://localhost:5173/ws         │
                 └───────────────────────┬────────────────────────┘
                                         │ JSON messages
                                         ▼
                 ┌────────────────────────────────────────────────┐
                 │              bridge.ts  (Express+WS :3001)     │
                 │  generate / resume / ack → gRPC client → tokens│
                 │  forwards token + journal_state over WS        │
                 └───────────────────────┬────────────────────────┘
                                         │ gRPC server-streaming
                                         ▼
                 ┌────────────────────────────────────────────────┐
                 │          server/index.ts  (gRPC :50051)        │
                 │   Generate(GenerateRequest) → stream           │
                 │    ├─ resume?  →  replay journal, then fresh   │
                 │    └─ fresh?   →  stream LLM, journal each     │
                 │   Ack(AckRequest) → trim journal               │
                 └───────────────┬────────────────┬───────────────┘
                                 │                │
                    ┌────────────▼──────┐   ┌─────▼──────────────┐
                    │   journal.ts      │   │   llm.ts           │
                    │   in-memory token │   │   OpenAI-compatible│
                    │   store, TTL + ACK│   │   streaming client │
                    │   trimming        │   └────────────────────┘
                    └───────────────────┘
```

**Sequence diagram** (the resumption story):

```
Client                                  Server                          LLM
  │  Generate(session_id, resume_from=0) │                               │
  ├─────────────────────────────────────▶│  fresh inference              │
  │  token(0) ◀──────────────────────────│◀──────────────────────────────┤
  │  token(1) ◀──────────────────────────│── journal.write(1) ──────────▶│
  │  ...                                 │  ...                          │
  │  Ack(acked=25) ─────────────────────▶│── journal.ack → trim ────────▶│
  │  ...                                 │  ...                          │
  │            ⚡ STREAM DROPS ⚡        │                               │
  │  Generate(session_id, resume_from=N) │                               │
  ├─────────────────────────────────────▶│  replay journal[N..]          │
  │  token(N) ◀──────────────────────────│                               │
  │  token(N+1) ◀────────────────────────│── continue fresh ────────────▶│
  │  ...                                 │                               │
```

## 5. The protocol

The entire protocol is defined in [`proto/inference.proto`](proto/inference.proto) —
the schema itself is the thing being demonstrated:

```proto
syntax = "proto3";

package inference;

service InferenceService {
  rpc Generate(GenerateRequest) returns (stream GenerateResponse);
  rpc Ack(AckRequest) returns (AckResponse);
}

message GenerateRequest {
  string prompt = 1;
  string session_id = 2;
  int32 resume_from_token = 3;
}

message GenerateResponse {
  string token = 1;
  int32 token_index = 2;
  bool is_final = 3;
}

message AckRequest {
  string session_id = 1;
  int32 acked_token_index = 2;
}

message AckResponse {
  string session_id = 1;
  int32 acked_token_index = 2;
  int32 journaled_token_count = 3;
  int32 first_retained_index = 4;
}
```

**The semantics are the design:**

- `token_index` is the client's **dead-reckoning cursor** — a monotonic position
  in the stream.
- `resume_from_token` says "give me tokens starting at this index" — the server
  replays journaled tokens from that index with their **original** indices, so
  the client's stream is gapless and de-duplicated.
- `is_final` marks the end of a stream (the final marker carries the exclusive
  end index).
- `Ack(session_id, acked_token_index)` lets the client confirm its watermark so
  the server can **trim** journaled tokens the client no longer needs.

TypeScript types are generated with `ts-proto` into `server/generated/` and
`client/generated/`; `@grpc/grpc-js` handles the wire format.

## 6. Code walkthrough

### 6.1 `server/journal.ts` — the token journal (the heart)

A sliding-window in-memory store keyed by `session_id`, with TTL eviction and
ACK-aware trimming:

```ts
interface JournalEntry {
  tokens: string[];        // window; absolute index of tokens[i] = base_index + i
  base_index: number;      // advances as ACK-trimming drops the head
  acked_index: number;     // highest token the client confirmed via Ack
  created_at: number;
  last_accessed: number;   // drives TTL eviction
}

export class TokenJournal {
  write(session_id: string, token_index: number, token: string): void {
    // append a token; grow the window gaplessly
  }

  read(session_id: string, from_index: number): string[] {
    // return every token from from_index onward (absolute indices)
  }

  ack(session_id: string, acked_index: number): AckState | null {
    // record the confirmed watermark and TRIM tokens at-or-below it
  }

  evict(): void {
    // TTL cleanup, swept every 60s
  }
}
```

The journal **never re-runs the LLM** — it only stores and replays tokens that
were already generated. That is the whole point of the demo.

### 6.2 `server/handler.ts` — the Generate + Ack handlers

Shared by the gRPC server and the bridge. The `Generate` handler decides
between "resume from journal" and "fresh inference":

```ts
function generateHandler(journal, prompts, call): void {
  const { prompt, sessionId, resumeFromToken } = call.request;

  if (resumeFromToken > 0 && journal.has(sessionId)) {
    // 1. Replay journaled tokens from resumeFromToken with ORIGINAL indices.
    await replayJournal(journal, call, sessionId, resumeFromToken);
    // 2. Continue streaming fresh tokens from the LLM at lastIndex + 1.
    const endIndex = await streamFresh(
      journal, call, sessionId, effectivePrompt, journal.lastIndex(sessionId) + 1,
    );
    await finish(call, endIndex);
  } else {
    // Fresh inference: journal every token as it arrives.
    prompts.set(sessionId, prompt);
    const endIndex = await streamFresh(journal, call, sessionId, prompt, 0);
    await finish(call, endIndex);
  }
}
```

The `Ack` handler records the watermark and trims:

```ts
function ackHandler(journal, call, callback): void {
  const state = journal.ack(call.request.sessionId, call.request.ackedTokenIndex);
  if (!state) {
    callback({ code: grpc.status.NOT_FOUND, message: `session not in journal` });
    return;
  }
  callback(null, {
    sessionId: call.request.sessionId,
    ackedTokenIndex: state.acked_index,
    journaledTokenCount: state.journaled_token_count,
    firstRetainedIndex: state.first_retained_index,
  });
}
```

### 6.3 `server/llm.ts` — the OpenAI-compatible wrapper

A thin adapter: read `.env`, call the streaming endpoint, invoke `onToken` for
every non-empty delta. It has no knowledge of sessions or journals — the gRPC
server layers resumability on top.

```ts
export async function streamCompletion(
  config: LlmConfig,
  prompt: string,
  onToken: (token: string) => Promise<void>,
): Promise<void> {
  const client = new OpenAI({ baseURL: config.baseUrl, apiKey: config.apiKey });
  const stream = await client.chat.completions.create({
    model: config.model,
    messages: [{ role: "user", content: prompt }],
    stream: true,
  });
  for await (const chunk of stream) {
    const delta = chunk.choices[0]?.delta?.content;
    if (!delta || delta.length === 0) continue; // skip empty deltas
    await onToken(delta);
  }
}
```

### 6.4 `server/bridge.ts` — Express + WebSocket bridge

Browsers cannot speak gRPC natively, so the bridge translates WebSocket JSON
into gRPC calls and forwards tokens back. It also handles browser `ack`
messages by calling the gRPC `Ack` RPC. Crucially, it **never crashes** on a
disconnected stream — `sendToBrowser` guards against sending to a closed socket
(that's exactly the "Kill Stream" scenario):

```ts
function sendToBrowser(socket: WebSocket, message: unknown): void {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}
```

Browser → bridge messages:

```json
{ "type": "generate", "prompt": "...", "session_id": "sess_..." }
{ "type": "resume", "session_id": "sess_...", "resume_from_token": 42 }
{ "type": "ack", "session_id": "sess_...", "acked_token_index": 99 }
```

Bridge → browser messages:

```json
{ "type": "token", "token": "The", "token_index": 0, "is_final": false, "session_id": "sess_..." }
{ "type": "journal_state", "session_id": "sess_...", "token_count": 101 }
{ "type": "ack_state", "session_id": "sess_...", "acked_token_index": 99, "journaled_token_count": 2, "first_retained_index": 99 }
{ "type": "stream_started" | "stream_resumed" | "stream_dropped", ... }
```

### 6.5 `client/index.ts` — standalone gRPC CLI client

The terminal tool that proves resumption without a browser. It tracks the last
received `token_index`, sends periodic ACKs (`--ack-every N`), and prints a
summary on stream end or error showing exactly where to resume:

```ts
stream.on("data", (response) => {
  if (response.isFinal) return;
  lastIndex = response.tokenIndex;
  process.stdout.write(response.token);   // inline, no newlines
  maybeAck();                              // periodic Ack
});
// on end/error:
// summary: session="demo" last_token_index=41 (resume with --resume-from 42)
```

### 6.6 `web/` — the React UI

Two panels, raw CSS only:

- **`TokenStream.tsx`** — the live token display. Blinking green cursor while
  streaming, red cursor + 60% opacity when dropped, yellow flash on replayed
  tokens after a resume. Buttons: **Generate**, **Kill Stream** (closes the
  WebSocket abruptly), **Resume** (opens a brand-new WebSocket and resumes from
  `lastIndex + 1`).
- **`JournalInspector.tsx`** — session id (truncated to 8 chars), journal token
  count, last ACK index, server ACK watermark, color-coded stream/connection
  status, and a 10-event timeline.

The UI sends a periodic `ack` every `ACK_INTERVAL_MS` (2s) while a session is
active, so the server can trim its journal.

## 7. Prerequisites

- **Node.js ≥ 20** and npm
- **protoc** (only needed if you change the proto — the generated types are
  committed)
- An **OpenAI-compatible inference endpoint** — e.g. [Ollama](https://ollama.com)
  (`ollama serve` with a model pulled), LM Studio, vLLM, or OpenAI itself.
  Anything that exposes `POST /v1/chat/completions` with `stream: true` works.

## 8. Installation and running

```bash
# 1. Clone and install
git clone <this-repo> dead-reckoning-grpc
cd dead-reckoning-grpc
npm install

# 2. Configure your inference endpoint
cp .env.example .env
#    edit .env — set OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL

# 3. (Optional) regenerate proto types if you touched the proto
npm run proto:gen

# 4. Start everything: gRPC server + bridge + Vite dev server
npm run dev
```

Open **http://localhost:5173** — the UI should appear.

> The three processes run concurrently with
> [`concurrently`](https://www.npmjs.com/package/concurrently):
> gRPC server on `:50051`, bridge on `:3001`, Vite on `:5173`. Each can also be
> run individually: `npm run dev:server`, `npm run dev:bridge`, `npm run dev:web`.

## 9. How to trigger the demo manually

1. **Start the server**: `npm run dev:server` (or just `npm run dev`).
2. **Start the bridge**: `npm run dev:bridge`.
3. **Open the browser**: `http://localhost:5173`.
4. **Click Generate** — a 500-word lighthouse-keeper story streams token by
   token into the left panel, with a blinking green cursor. The Journal
   Inspector tracks session id, journal token count, last ACK index, and the
   server's ACK watermark (updated by the UI's periodic ACKs).
5. **Click Kill Stream mid-generation** — the WebSocket closes *abruptly* (a
   real connection close, not a render pause). The cursor turns red, the text
   fades to 60% opacity, and the timeline records the drop at the exact token.
   The server keeps generating and journaling in the background.
6. **Click Resume** — the UI opens a *brand-new* WebSocket and sends a `resume`
   message with the same `session_id` and the first index the UI has *not*
   received (last ACK index + 1). The server replays the journaled tokens from
   that index (they flash yellow briefly), then continues streaming fresh
   tokens. The story continues from the exact word where it stopped — with no
   gaps and no duplicates.
7. **Observe** — the Journal Inspector shows `RESUMED`, the token count keeps
   climbing, and the terminal logs the server's `STARTING` → `RESUMING` →
   `completed` lifecycle for the session, plus periodic `ack`/`trim` lines as
   the journal drops tokens the client has already confirmed.

### Terminal-only testing (no browser)

The standalone gRPC client talks straight to the gRPC server:

```bash
# Fresh generation
npm run dev --workspace=client -- --prompt "Tell me a story about a lighthouse" --session-id demo1 --resume-from 0

# Simulate a drop: interrupt it (Ctrl-C) and note last_token_index in the summary.

# Resume from the first token you haven't received (last_token_index + 1) —
# the server replays, not re-runs. --prompt is optional on resume.
npm run dev --workspace=client -- --session-id demo1 --resume-from 42
```

The client prints every token inline (`process.stdout.write`), tracks the last
received `token_index` in memory, sends periodic ACKs (`--ack-every N`, default
25) so the server can trim its journal, and prints a summary on stream end or
error showing `session_id` and the next index to resume from.

## 10. Configuration via `.env`

| Variable             | Default                | Description                                              |
| -------------------- | ---------------------- | -------------------------------------------------------- |
| `OPENAI_BASE_URL`    | `http://localhost:11434/v1` | OpenAI-compatible base URL (Ollama default shown)    |
| `OPENAI_API_KEY`     | `ollama`               | API key (Ollama accepts any non-empty value)             |
| `OPENAI_MODEL`       | `llama3.2`             | Model name served by the endpoint                        |
| `GRPC_PORT`          | `50051`                | gRPC server listen port (the bridge targets this by default) |
| `BRIDGE_PORT`        | `3001`                 | Express + WebSocket bridge port                          |
| `JOURNAL_TTL_MS`     | `600000`               | Journal TTL — session evicted after this much inactivity |

Optional overrides: `SERVER_ADDRESS` (explicit gRPC address for the bridge,
overrides `GRPC_PORT`) and `VITE_PORT` (default `5173`).

## 11. Benchmark: retry vs. resume

The companion benchmark (`npm run bench`) compares the two recovery strategies
against **your `.env` OpenAI-compatible endpoint** — the same one the demo runs
against — so the numbers reflect real latency and real LLM non-determinism. It
boots a real Dead Reckoning gRPC server (the same handlers the demo uses) and
drives actual `Generate` calls. A deterministic local mock is available for
reproducible CI runs: `npm run bench -- --mock`.

For each drop point D, the benchmark simulates the demo's kill-then-resume:
the client renders tokens `0..D-1`, then stays "disconnected" for a gap while
the server keeps journaling (exactly what the bridge does after a browser
WebSocket closes), then either **retries** from scratch or **resumes** from D.

Representative run against a real endpoint (drops at 50 / 200 / 400 tokens):

| drop after | strategy | reconnect→1st token | reconnect→complete | tokens received | new inference tokens | inference saved |
|---|----------|-------------------|--------------------|-----------------|----------------------|-----------------|
| 50 | retry | 13326 ms | 45871 ms | 481 | 431 | — |
| 50 | resume | 5 ms | 58480 ms | 512 | 454 | — |
| 200 | retry | 13919 ms | 37024 ms | 583 | 383 | — |
| 200 | resume | 7 ms | 21417 ms | 654 | 442 | — |
| 400 | retry | 10116 ms | 29655 ms | 828 | 411 | — |
| 400 | resume | 8117 ms | 32317 ms | 734 | 380 | −31 |

**How to read it (and the honest caveats).**

- **Reconnect→1st token is the decisive, user-visible number.** With *retry*,
  the client waits for the LLM to re-generate from scratch — 10–14 seconds of
  dead air on this endpoint. With *resume*, the server replays the journaled
  gap from memory in **5–7 ms**, so the story continues almost instantly. This
  is the difference the demo shows live.
- **Inference tokens are noisy — that is the point of using a real LLM.**
  Because sampling is non-deterministic, a retry's fresh run can be longer or
  shorter than the original (431 vs 454 tokens at drop 50), so the "saving"
  column can be negative or positive run-to-run. At drop 400, resume saved
  31 tokens of inference; at smaller drops the variance dominates. The mock
  mode (`--mock`) shows the *deterministic* version where the saving is
  exactly `D` tokens, which is what a backend with deterministic continuation
  (e.g. `temperature: 0`) would give you.
- **The 8 s first-token at drop 400** is a real edge: if the journal was
  trimmed or the gap outran the journal, resume falls through to fresh
  inference and pays the LLM's time-to-first-token again. Resume never *loses*
  data, but its advantage is bounded by what the journal retained.

Run it yourself: `npm run bench` (real) or `npm run bench -- --mock` (mock).
Tune with `BENCH_DROPS=50,200,400`, `BENCH_GAP_MS=1000`, `BENCH_PROMPT=...`.
Raw output is saved to `benchmark/results.md`.

## 12. Prior art

A review confirming that **no mainstream gRPC implementation (gRPC-core,
grpc-web, connect-rpc) provides resumable server-streaming** is in
[`docs/prior-art.md`](docs/prior-art.md). It also distinguishes this work from
adjacent mechanisms (HTTP Range, message-queue replay, prompt caching).

## 13. Known limitations

- **LLM non-determinism.** A *fresh* generation is not guaranteed to produce
  the same text as a previous one (temperature sampling). Resumption is only
  bit-identical *after* the resume point, because the journal replays the exact
  tokens that were produced. If you want identical fresh runs, use
  `temperature: 0` / a fixed seed on your endpoint.
- **In-memory journal.** The journal lives in server memory; a process restart
  loses all sessions, and `resume` then falls back to a fresh inference. A
  production version would persist the journal (append-only log, WAL, or
  object storage) — the protocol design is unchanged.
- **Single-server scope.** Sessions are pinned to one server process. Resuming
  from a different replica would require a shared journal.
- **No auth / TLS / Docker.** This is a local demo, not a production service.
  The gRPC channel and bridge are insecure by design.

## 14. Forking and contributing

### Fork it

1. Fork the repo on GitHub and clone your fork.
2. `npm install`, `cp .env.example .env`, and point `.env` at your endpoint.
3. `npm run dev` to see it working locally.
4. Make a branch, commit, and open a PR.

### Project conventions

- **TypeScript strict mode everywhere** — no `any` types. New code must pass
  `npm run typecheck`.
- **Every file has a module-level comment** explaining what it does and why.
- **The proto schema is the protocol contract** — treat field names/numbers as
  immutable (adding new messages/RPCs is fine).
- **Raw CSS only** in `web/` — no Tailwind or component libraries.
- **Tests/benchmarks** live in `benchmark/`; run `npm run bench -- --mock` for
  a fast deterministic check.

### Ideas for new features

Here are concrete, valuable additions you could build:

1. **Persistent journal (WAL / SQLite / Redis).** Replace the in-memory map with
   an append-only log or a store so sessions survive server restarts. This is
   the single biggest production gap. The protocol is unchanged.
2. **Backpressure-aware replay.** Stream journaled tokens with `await` on each
   `write` so a slow client doesn't buffer unbounded tokens.
3. **`Cancel` RPC.** Add `Cancel(session_id)` so a user can *actually stop* a
   generation (vs. Kill Stream which only drops the client). The server would
   abort the LLM call and finalize the journal.
4. **Client-side cursor persistence.** Persist `last_token_index` in
   `localStorage` so a page reload can auto-resume without the server's ACK
   watermark.
5. **Multi-model / multi-endpoint routing.** Let each `Generate` pick a model
   via the request, and route to different endpoints per model.
6. **Stream statistics.** Expose tokens/sec, TTFT, and latency percentiles via
   the bridge (`stats` message) and render them in the UI.
7. **Shared-journal multi-replica.** Move the journal behind Redis/etcd so any
   replica can serve a resume — turning the single-server limitation into a
   horizontally-scalable design.
8. **Idempotency / dedup at the client.** Add a message-level dedup key so a
   client that resumes can verify it never double-applies a token.
9. **AuthN/Z.** Add a simple token check on the bridge WebSocket and gRPC
   metadata, plus TLS for both channels.
10. **gRPC-web / connect-rpc client.** Show the same protocol working from a
    browser-native gRPC transport (grpc-web or connect) instead of the bridge.

### Code of conduct

Be kind, write clean strict-TypeScript, and keep the demo runnable with
`npm install && npm run dev`.

---

## 15. Repository layout

```
dead-reckoning-grpc/
├── proto/inference.proto      # the protocol being demonstrated (cursor + ACK)
├── server/
│   ├── index.ts               # gRPC server entry point
│   ├── handler.ts             # Generate + Ack handlers (shared with bridge)
│   ├── journal.ts             # token journal with TTL + ACK trimming (the core)
│   ├── llm.ts                 # OpenAI-compatible streaming wrapper
│   └── bridge.ts              # Express + WebSocket bridge for the browser
├── client/index.ts            # standalone gRPC CLI client (cursor + periodic ACKs)
├── benchmark/
│   ├── src/index.ts           # retry-vs-resume benchmark harness
│   └── results.md             # generated comparison table
├── docs/
│   ├── prior-art.md           # prior-art review (gRPC-core / grpc-web / connect)
│   ├── BLOG.md                # technical blog post
│   ├── X-THREAD.md            # 2-tweet X thread
│   └── LINKEDIN.md            # LinkedIn post
├── web/src/
│   ├── App.tsx                # state machine, WebSocket, ACK timer, panels
│   └── components/
│       ├── TokenStream.tsx    # live token display
│       └── JournalInspector.tsx # sidebar session state + timeline
└── package.json               # root: workspaces + concurrently dev script
```

---

**Built by [Harish Kotra](https://harishkotra.me)** — Check out [other builds](https://dailybuild.xyz)
