// server/job.ts
//
// A GenerationJob owns ONE upstream inference run for a session, independent of
// any gRPC call. It journals every token the moment it arrives and fans it out
// to whatever call is currently attached (zero or one).
//
// This is the whole fix for the demo's biggest lie. Before, inference lived
// inside the RPC handler: when the client vanished, generation stopped, and a
// "resume" started a second completion from the prompt. Now:
//
//   Generate() -> attach(job)
//   client dies -> detach   (generation keeps writing to the journal)
//   reconnect  -> attach(job) again, replay the journal, join the same run
//
// Cancel() is the only thing that stops the model.

import { loadLlmConfig, streamCompletion } from "./llm.js";
import type { TokenJournal } from "./journal.js";

/** Receives every token after it has been journaled. */
type Sink = (token: string, index: number) => void;

/** Timestamp-prefixed stdout logger for job lifecycle events. */
function log(message: string): void {
  process.stdout.write(`[${new Date().toISOString()}] [job] ${message}\n`);
}

export class GenerationJob {
  private readonly sinks = new Set<Sink>();
  private readonly doneWaiters = new Set<() => void>();
  private readonly abort = new AbortController();
  private aborted = false;
  private completed = false;
  /** Absolute index the next token will be written at. */
  private nextIndex: number;

  constructor(
    private readonly journal: TokenJournal,
    readonly sessionId: string,
    private readonly prompt: string,
    startIndex: number,
    /** Tokens already generated, when continuing after a server restart. */
    private readonly continueFrom = "",
  ) {
    this.nextIndex = startIndex;
  }

  /** True while the upstream model is still producing. */
  get running(): boolean {
    return !this.completed && !this.aborted;
  }

  /** Attach a listener. Returns the detach function (a client drop is not a cancel). */
  attach(sink: Sink): () => void {
    this.sinks.add(sink);
    log(`session "${this.sessionId}" attached (${this.sinks.size} attached)`);
    return () => {
      this.sinks.delete(sink);
      log(`session "${this.sessionId}" detached (${this.sinks.size} still attached)`);
    };
  }

  /** Invoke `fn` when the job finishes (immediately if it already has). */
  onDone(fn: () => void): () => void {
    if (!this.running) {
      fn();
      return () => {};
    }
    this.doneWaiters.add(fn);
    return () => this.doneWaiters.delete(fn);
  }

  /** Start the upstream run. Resolves when the run ends, for any reason. */
  async run(): Promise<void> {
    let index = this.nextIndex;
    try {
      await streamCompletion(
        loadLlmConfig(),
        this.prompt,
        async (token: string) => {
          if (this.aborted) {
            return;
          }
          // Journal first, then fan out: a token a client received is always
          // a token the journal can replay.
          this.journal.write(this.sessionId, index, token);
          for (const sink of this.sinks) {
            sink(token, index);
          }
          index += 1;
        },
        { continueFrom: this.continueFrom, signal: this.abort.signal },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (this.aborted) {
        log(`session "${this.sessionId}" upstream aborted at token ${index - 1}`);
      } else {
        log(`session "${this.sessionId}" upstream failed at token ${index - 1}: ${message}`);
      }
    }
    this.nextIndex = index;
    this.completed = true;
    log(`session "${this.sessionId}" job finished at token ${index - 1} (${this.sinks.size} attached)`);
    for (const fn of this.doneWaiters) {
      fn();
    }
    this.doneWaiters.clear();
  }

  /** Stop the model. Returns the last token index journaled. */
  cancel(): number {
    this.aborted = true;
    this.abort.abort();
    return this.journal.lastIndex(this.sessionId);
  }
}
