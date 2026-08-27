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
// Storage is in-memory only for this version. Every write, ack, trim and
// eviction is logged to stdout with a timestamp prefix.

import { setInterval, clearInterval } from "node:timers";

/** A single journaled session: a sliding window of tokens produced so far. */
interface JournalEntry {
  /** Tokens in order of generation; absolute index of tokens[i] is base_index + i. */
  tokens: string[];
  /** Absolute index of tokens[0]. Starts at 0 and advances as ACK-trimming drops the head. */
  base_index: number;
  /** Highest token index the client has confirmed via Ack (-1 if none yet). */
  acked_index: number;
  /** Epoch ms when the session was first created. */
  created_at: number;
  /** Epoch ms of the most recent read or write — drives TTL eviction. */
  last_accessed: number;
}

const TTL_SWEEP_INTERVAL_MS = 60_000;

/**
 * A timestamp-prefixed stdout logger. Keeps the journal's behavior visible in
 * the terminal so the demo is self-explanatory.
 */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [journal] ${message}\n`);
}

export class TokenJournal {
  private readonly sessions = new Map<string, JournalEntry>();
  private readonly ttlMs: number;
  private readonly sweepTimer: ReturnType<typeof setInterval>;

  constructor(ttlMs: number) {
    this.ttlMs = ttlMs;
    log(`initialized with TTL ${ttlMs}ms (sweep every ${TTL_SWEEP_INTERVAL_MS}ms)`);
    // Sweep expired sessions periodically. Unref'd so it never keeps the
    // process alive on its own.
    this.sweepTimer = setInterval(() => this.evict(), TTL_SWEEP_INTERVAL_MS);
    this.sweepTimer.unref();
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
    log(`write session="${session_id}" index=${token_index} token=${JSON.stringify(token)}`);
  }

  /**
   * Return every token from from_index onward (absolute indices). Returns an
   * empty array when the session does not exist, from_index is beyond the end
   * of the journal, or from_index has already been ACK-trimmed away.
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
      log(
        `trim session="${session_id}" dropped ${dropCount} token(s); ` +
          `window=[${entry.base_index}..${this.lastIndex(session_id)}] retained=${entry.tokens.length}`,
      );
    }
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
