// server/journal.ts
//
// The token journal is the heart of Dead Reckoning. It is the server-side
// record of every token already generated for a session, keyed by session_id.
// When a gRPC stream drops, the client reconnects with a resume_from_token
// index; the journal lets the server replay exactly those already-produced
// tokens instead of re-running inference.
//
// Deliberate design: the journal NEVER re-runs the LLM. It only stores and
// replays tokens that were already generated. That is the whole point of the
// demo — resumption is a checkpointing problem (replay from a journal), not a
// retry problem (re-run the model).
//
// Memory model: the journal keeps tokens in a sliding window. Each session
// records a base_index (the absolute index of tokens[0]) and an acked_index
// (the highest token the client has confirmed via Ack). ACK-aware trimming
// drops tokens at-or-below acked_index, so very long generations don't grow
// memory without bound; TTL eviction (10 minutes of inactivity, configurable
// via JOURNAL_TTL_MS, swept every 60s) drops whole idle sessions.
//
// Persistence: the journal is mirrored to SQLite via node:sqlite (built into
// Node, so no dependency and no native build step). That is what makes the
// strongest version of the demo true — kill the SERVER mid-stream, restart it,
// and the reconnect still replays the journal instead of regenerating. Pass no
// db path to get the original in-memory-only behaviour. Every write, ack, trim
// and eviction is logged to stdout with a timestamp prefix.

import { createRequire } from "node:module";
import { setInterval, clearInterval } from "node:timers";

/** A single journaled session: a sliding window of tokens produced so far. */
interface JournalEntry {
  /** Tokens in order of generation; absolute index of tokens[i] is base_index + i. */
  tokens: string[];
  /** Absolute index of tokens[0]. Starts at 0 and advances as ACK-trimming drops the head. */
  base_index: number;
  /** Highest token index the client has confirmed via Ack (-1 if none yet). */
  acked_index: number;
  /** Last token index of a completed run, or -1 while generation is unfinished. */
  final_index: number;
  /** The prompt the session was created with (needed to continue after a restart). */
  prompt: string;
  /** Epoch ms when the session was first created. */
  created_at: number;
  /** Epoch ms of the most recent read or write — drives TTL eviction. */
  last_accessed: number;
}

/** The slice of node:sqlite the journal uses. Typed locally to avoid depending on @types/node. */
interface SqlStatement {
  run(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}
interface SqlDb {
  exec(sql: string): void;
  prepare(sql: string): SqlStatement;
}

const TTL_SWEEP_INTERVAL_MS = 60_000;

/**
 * A timestamp-prefixed stdout logger. Keeps the journal's behavior visible in
 * the terminal so the demo is self-explanatory.
 */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [journal] ${message}\n`);
}

/**
 * Open the SQLite journal. Returns null (memory-only mode) when node:sqlite is
 * unavailable, so a missing runtime feature degrades instead of crashing.
 */
function openDb(path: string): SqlDb | null {
  try {
    const require = createRequire(import.meta.url);
    const { DatabaseSync } = require("node:sqlite") as { DatabaseSync: new (p: string) => SqlDb };
    return new DatabaseSync(path);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    log(`persistence disabled (${message}); journal is memory-only`);
    return null;
  }
}

export class TokenJournal {
  private readonly sessions = new Map<string, JournalEntry>();
  private readonly ttlMs: number;
  private readonly sweepTimer: ReturnType<typeof setInterval>;
  private readonly db: SqlDb | null;

  constructor(ttlMs: number, dbPath?: string) {
    this.ttlMs = ttlMs;
    this.db = dbPath ? openDb(dbPath) : null;
    if (this.db) {
      this.db.exec(
        `CREATE TABLE IF NOT EXISTS sessions (
           session_id TEXT PRIMARY KEY,
           prompt TEXT NOT NULL DEFAULT '',
           acked_index INTEGER NOT NULL DEFAULT -1,
           base_index INTEGER NOT NULL DEFAULT 0,
           final_index INTEGER NOT NULL DEFAULT -1,
           created_at INTEGER NOT NULL,
           last_accessed INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS tokens (
           session_id TEXT NOT NULL,
           idx INTEGER NOT NULL,
           token TEXT NOT NULL,
           PRIMARY KEY (session_id, idx)
         );`,
      );
      this.load();
    }
    log(
      `initialized with TTL ${ttlMs}ms (sweep every ${TTL_SWEEP_INTERVAL_MS}ms, ` +
        `${this.db ? `persistence at ${dbPath}` : "memory-only"})`,
    );
    // Sweep expired sessions periodically. Unref'd so it never keeps the
    // process alive on its own.
    this.sweepTimer = setInterval(() => this.evict(), TTL_SWEEP_INTERVAL_MS);
    this.sweepTimer.unref();
  }

  /** Rehydrate sessions and their retained tokens from SQLite at startup. */
  private load(): void {
    if (!this.db) {
      return;
    }
    const rows = this.db.prepare("SELECT * FROM sessions").all() as Array<Record<string, unknown>>;
    for (const row of rows) {
      const session_id = String(row.session_id);
      const base_index = Number(row.base_index);
      const tokens = (this.db.prepare("SELECT idx, token FROM tokens WHERE session_id = ? ORDER BY idx").all(session_id) as Array<
        Record<string, unknown>
      >).map((t) => [Number(t.idx), String(t.token)] as const);
      const entry: JournalEntry = {
        tokens: [],
        base_index,
        acked_index: Number(row.acked_index),
        final_index: Number(row.final_index),
        prompt: String(row.prompt ?? ""),
        created_at: Number(row.created_at),
        last_accessed: Number(row.last_accessed),
      };
      for (const [idx, token] of tokens) {
        while (entry.tokens.length <= idx - base_index) {
          entry.tokens.push("");
        }
        entry.tokens[idx - base_index] = token;
      }
      this.sessions.set(session_id, entry);
    }
    if (rows.length > 0) {
      log(`rehydrated ${rows.length} session(s) from disk`);
    }
  }

  /** Mirror one session's metadata to disk. */
  private persistSession(session_id: string, entry: JournalEntry): void {
    if (!this.db) {
      return;
    }
    this.db
      .prepare(
        `INSERT INTO sessions (session_id, prompt, acked_index, base_index, final_index, created_at, last_accessed)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(session_id) DO UPDATE SET
           prompt = excluded.prompt,
           acked_index = excluded.acked_index,
           base_index = excluded.base_index,
           final_index = excluded.final_index,
           last_accessed = excluded.last_accessed`,
      )
      .run(session_id, entry.prompt, entry.acked_index, entry.base_index, entry.final_index, entry.created_at, entry.last_accessed);
  }

  /**
   * Append a token at token_index. If the session does not exist it is created
   * on the fly (an implicit start-of-journal write). Missing intermediate
   * indices are back-filled with empty strings so the window stays gapless.
   */
  write(session_id: string, token_index: number, token: string): void {
    let entry = this.sessions.get(session_id);
    if (!entry) {
      entry = {
        tokens: [],
        base_index: 0,
        acked_index: -1,
        final_index: -1,
        prompt: "",
        created_at: Date.now(),
        last_accessed: Date.now(),
      };
      this.sessions.set(session_id, entry);
      log(`session "${session_id}" created in journal`);
    }
    // Grow the window to cover the requested absolute index, back-filling any
    // gap (should not happen in normal operation, but keeps indices honest).
    while (entry.tokens.length <= token_index - entry.base_index) {
      entry.tokens.push("");
    }
    entry.tokens[token_index - entry.base_index] = token;
    entry.last_accessed = Date.now();
    if (this.db) {
      this.db.prepare("INSERT OR REPLACE INTO tokens (session_id, idx, token) VALUES (?, ?, ?)").run(session_id, token_index, token);
      this.persistSession(session_id, entry);
    }
    log(`write session="${session_id}" index=${token_index} token=${JSON.stringify(token)}`);
  }

  /** Record the prompt a session was started with (persisted for post-restart continuation). */
  setPrompt(session_id: string, prompt: string): void {
    const entry = this.sessions.get(session_id);
    if (entry) {
      entry.prompt = prompt;
      this.persistSession(session_id, entry);
    }
  }

  /** The prompt a session was started with ("" if unknown). */
  prompt(session_id: string): string {
    return this.sessions.get(session_id)?.prompt ?? "";
  }

  /**
   * The tail of a session's journal as text — what a continuation prompt is
   * built from when the process that owned the run is gone.
   */
  text(session_id: string, maxChars: number): string {
    const tokens = this.sessions.get(session_id)?.tokens ?? [];
    return tokens.join("").slice(-maxChars);
  }

  /**
   * Return every token from from_index onward (absolute indices). Returns an
   * empty array when the session does not exist, from_index is beyond the end
   * of the journal, or from_index has already been ACK-trimmed away. Callers
   * that must distinguish "nothing to replay" from "cursor fell out of the
   * window" check firstIndex() first.
   */
  read(session_id: string, from_index: number): string[] {
    const entry = this.sessions.get(session_id);
    if (!entry) {
      return [];
    }
    entry.last_accessed = Date.now();
    if (from_index < entry.base_index) {
      return [];
    }
    const offset = from_index - entry.base_index;
    return entry.tokens.slice(offset);
  }

  /** Whether the session currently exists in the journal. */
  has(session_id: string): boolean {
    return this.sessions.has(session_id);
  }

  /** Number of journaled tokens currently retained for a session (0 if unknown). */
  count(session_id: string): number {
    return this.sessions.get(session_id)?.tokens.length ?? 0;
  }

  /** Absolute index of the last journaled token for a session (-1 if none). */
  lastIndex(session_id: string): number {
    const entry = this.sessions.get(session_id);
    if (!entry || entry.tokens.length === 0) {
      return -1;
    }
    return entry.base_index + entry.tokens.length - 1;
  }

  /** The client's confirmed watermark for a session (-1 if none). */
  ackedIndex(session_id: string): number {
    return this.sessions.get(session_id)?.acked_index ?? -1;
  }

  /** Index of the first token still retained (the window head), or -1 if none. */
  firstIndex(session_id: string): number {
    const entry = this.sessions.get(session_id);
    if (!entry || entry.tokens.length === 0) {
      return -1;
    }
    return entry.base_index;
  }

  /** Mark a session's run as complete at final_index, so resumes replay and stop. */
  markFinal(session_id: string, final_index: number): void {
    const entry = this.sessions.get(session_id);
    if (!entry) {
      return;
    }
    entry.final_index = final_index;
    this.persistSession(session_id, entry);
    log(`session "${session_id}" marked final at token ${final_index}`);
  }

  /** Last index of a completed run, or -1 if the run never finished. */
  finalIndex(session_id: string): number {
    return this.sessions.get(session_id)?.final_index ?? -1;
  }

  /**
   * Record the client's confirmed watermark and trim journaled tokens the
   * client no longer needs. acked_index is the index of the LAST token the
   * client fully received, so tokens at-or-below it are safe to drop. A
   * monotonic guard prevents an out-of-order ACK from shrinking the watermark.
   * Returns the post-ack state so the server can answer AckResponse.
   */
  ack(session_id: string, acked_index: number): {
    acked_index: number;
    journaled_token_count: number;
    first_retained_index: number;
  } | null {
    const entry = this.sessions.get(session_id);
    if (!entry) {
      return null;
    }
    entry.last_accessed = Date.now();
    if (acked_index > entry.acked_index) {
      entry.acked_index = acked_index;
      log(`ack session="${session_id}" watermark=${acked_index}`);
    }
    // Trim tokens at-or-below the watermark (the client no longer needs them).
    // Guard: keep at least the final marker slot so a completed session's
    // journal is never emptied below its last index.
    const lastIdx = this.lastIndex(session_id);
    const trimUpTo = Math.min(entry.acked_index, lastIdx >= 0 ? lastIdx - 1 : entry.acked_index);
    const dropCount = Math.max(0, trimUpTo - entry.base_index + 1);
    if (dropCount > 0) {
      entry.tokens.splice(0, dropCount);
      entry.base_index += dropCount;
      if (this.db) {
        this.db.prepare("DELETE FROM tokens WHERE session_id = ? AND idx <= ?").run(session_id, trimUpTo);
      }
      log(
        `trim session="${session_id}" dropped ${dropCount} token(s); ` +
          `window=[${entry.base_index}..${this.lastIndex(session_id)}] retained=${entry.tokens.length}`,
      );
    }
    this.persistSession(session_id, entry);
    return {
      acked_index: entry.acked_index,
      journaled_token_count: entry.tokens.length,
      first_retained_index: entry.tokens.length === 0 ? -1 : entry.base_index,
    };
  }

  /**
   * TTL cleanup: drop every session whose last_accessed is older than ttlMs.
   * Runs internally on a 60s interval and is also safe to call directly.
   */
  evict(): void {
    const now = Date.now();
    let evicted = 0;
    for (const [session_id, entry] of this.sessions) {
      if (now - entry.last_accessed > this.ttlMs) {
        this.sessions.delete(session_id);
        if (this.db) {
          this.db.prepare("DELETE FROM tokens WHERE session_id = ?").run(session_id);
          this.db.prepare("DELETE FROM sessions WHERE session_id = ?").run(session_id);
        }
        evicted += 1;
        log(
          `evicted session="${session_id}" (${entry.tokens.length} tokens retained, ` +
            `acked=${entry.acked_index}, idle ${now - entry.last_accessed}ms)`,
        );
      }
    }
    if (evicted > 0) {
      log(`eviction sweep removed ${evicted} session(s); ${this.sessions.size} remain`);
    }
  }

  /** Dispose the journal: stop the sweep timer. Primarily for tests/clean shutdown. */
  dispose(): void {
    clearInterval(this.sweepTimer);
    log("disposed (sweep timer stopped)");
  }
}
