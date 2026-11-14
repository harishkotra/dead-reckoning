// web/src/App.tsx
//
// Root component for the Dead Reckoning demo UI. Owns the WebSocket bridge
// connection, the session state machine (STREAMING / DROPPED / RESUMED), the
// token buffer, and the event timeline. Renders the two side-by-side panels:
// TokenStream (left) and JournalInspector (right).
//
// The three buttons demonstrate the protocol end to end:
//   - Generate: opens a WebSocket, sends a fresh generate message.
//   - Kill Stream: closes the WebSocket ABRUPTLY (a real connection close, not
//     just pausing rendering) — simulating a mid-generation drop.
//   - Resume: opens a brand-NEW WebSocket and sends a resume message with the
//     session_id and the index AFTER the last confirmed token. The server
//     replays from its journal, so the story continues from the exact word
//     where it stopped — with no duplicated tokens.
//
// Periodic ACKs: while a session is active the UI sends { type: "ack",
// acked_token_index } every ACK_INTERVAL_MS so the server can trim journaled
// tokens the client no longer needs (the protocol's memory-bounding
// mechanism). The server's post-ack state comes back as ack_state and is shown
// in the Journal Inspector.

import { useCallback, useEffect, useRef, useState } from "react";
import { TokenStream } from "./components/TokenStream";
import { JournalInspector } from "./components/JournalInspector";
import {
  randomSessionId,
  type BridgeToBrowserMessage,
  type BrowserToBridgeMessage,
} from "./protocol";

/** The hardcoded demo prompt required by the spec. */
const DEMO_PROMPT =
  "Write a detailed 500-word story about a lighthouse keeper during a storm";

/** How long a replayed token stays highlighted yellow after a resume. */
const FLASH_MS = 700;

/** How often the UI sends a periodic Ack to the bridge (ms). */
const ACK_INTERVAL_MS = 2000;

/** Stream lifecycle states shown in the UI. */
export type StreamStatus = "IDLE" | "STREAMING" | "DROPPED" | "RESUMED";

/** One journaled token as rendered in the left panel. */
interface Token {
  index: number;
  text: string;
}

/** A timeline entry in the journal inspector. */
export interface TimelineEvent {
  time: string;
  label: string;
}

/** Time-of-day string for timeline events, e.g. "10:42:01". */
function nowLabel(): string {
  return new Date().toLocaleTimeString("en-GB", { hour12: false });
}

export function App(): JSX.Element {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [tokens, setTokens] = useState<Token[]>([]);
  const [streamStatus, setStreamStatus] = useState<StreamStatus>("IDLE");
  const [connectionStatus, setConnectionStatus] = useState<"CONNECTED" | "DISCONNECTED">(
    "DISCONNECTED",
  );
  const [journalTokens, setJournalTokens] = useState(0);
  const [serverAckedIndex, setServerAckedIndex] = useState(0);
  const [flashing, setFlashing] = useState<ReadonlySet<number>>(new Set());
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  const [lastAckIndex, setLastAckIndex] = useState(0);

  const socketRef = useRef<WebSocket | null>(null);
  // The last token_index the UI has received — the "dead reckoning" cursor.
  // Stored in a ref so event handlers always read the latest value.
  const lastAckIndexRef = useRef(0);
  // Resume replay window: tokens in [flashFromRef, replayEndRef) are the ones
  // just replayed from the journal and get the yellow flash.
  const flashFromRef = useRef<number | null>(null);
  const replayEndRef = useRef<number | null>(null);
  // Journal token count at the moment of the drop — the resume replay window's
  // upper bound, captured so the flash covers exactly the replayed tokens.
  const journalTokensAtDropRef = useRef(0);
  const flashTimersRef = useRef<number[]>([]);

  /** Record the last ACKed token index in both ref and state. */
  const setLastAckIndexRef = useCallback((index: number) => {
    lastAckIndexRef.current = index;
    setLastAckIndex(index);
  }, []);

  /** Push an event onto the timeline, keeping only the last 10. */
  const pushEvent = useCallback((label: string) => {
    setEvents((prev) => [...prev, { time: nowLabel(), label }].slice(-10));
  }, []);

  /** Flash a set of token indices yellow, then fade back to white. */
  const flashTokens = useCallback((indices: number[]) => {
    if (indices.length === 0) {
      return;
    }
    setFlashing(new Set(indices));
    const timer = window.setTimeout(() => {
      setFlashing(new Set());
    }, FLASH_MS);
    flashTimersRef.current.push(timer);
  }, []);

  /** Append a streamed token to the buffer and advance the cursor. */
  const appendToken = useCallback(
    (token: Token) => {
      setTokens((prev) => [...prev, token]);
      // Highlight replayed tokens (index within the journal replay window).
      if (flashFromRef.current !== null && replayEndRef.current !== null) {
        if (token.index >= flashFromRef.current && token.index < replayEndRef.current) {
          flashTokens([token.index]);
        }
        // Fresh tokens have arrived — the replay window is over.
        if (token.index >= replayEndRef.current) {
          flashFromRef.current = null;
          replayEndRef.current = null;
        }
      }
    },
    [flashTokens],
  );

  /** Handle one message from the bridge. */
  const handleBridgeMessage = useCallback(
    (raw: string) => {
      const message = JSON.parse(raw) as BridgeToBrowserMessage;
      switch (message.type) {
        case "token": {
          if (message.token.length > 0) {
            appendToken({ index: message.token_index, text: message.token });
            // A token flagged replayed came out of the journal after a resume;
            // flash it so the audience can see exactly what was recovered
            // rather than regenerated.
            if (message.replayed) {
              flashTokens([message.token_index]);
            }
          }
          setLastAckIndexRef(message.token_index);
          // The final marker ends the stream: stop the blinking cursor.
          setStreamStatus(message.is_final ? "IDLE" : (prev) => (prev === "RESUMED" ? prev : "STREAMING"));
          break;
        }
        case "journal_state": {
          setJournalTokens(message.token_count);
          break;
        }
        case "ack_state": {
          // The server's confirmed watermark after it applied our ACK.
          setServerAckedIndex(message.acked_token_index);
          break;
        }
        case "stream_started": {
          setSessionId(message.session_id);
          setStreamStatus("STREAMING");
          setConnectionStatus("CONNECTED");
          pushEvent("Stream started");
          break;
        }
        case "stream_resumed": {
          setSessionId(message.session_id);
          setStreamStatus("RESUMED");
          setConnectionStatus("CONNECTED");
          // The replay window spans [resume_from_token, journal length at drop).
          flashFromRef.current = message.resume_from_token;
          replayEndRef.current = journalTokensAtDropRef.current;
          pushEvent(`Resumed from token ${message.resume_from_token}`);
          break;
        }
        case "stream_dropped": {
          setStreamStatus("DROPPED");
          setConnectionStatus("DISCONNECTED");
          journalTokensAtDropRef.current = journalTokens;
          pushEvent(`Stream dropped at token ${message.last_token_index}`);
          break;
        }
        case "resync_required": {
          // The cursor fell below the journal's retained window (ACK trimming
          // + TTL). Refuse to fake continuity: the client must start over.
          setStreamStatus("DROPPED");
          setConnectionStatus("DISCONNECTED");
          pushEvent(
            `Resync required: cursor below journal window (first retained ${message.first_retained_index})`,
          );
          break;
        }
        case "cancelled": {
          setStreamStatus("IDLE");
          pushEvent(`Cancelled at token ${message.cancelled_at_token}`);
          break;
        }
        default: {
          // Exhaustiveness guard: unknown message types are ignored.
          const never: never = message;
          void never;
        }
      }
    },
    [appendToken, flashTokens, journalTokens, pushEvent, setLastAckIndexRef],
  );

  /** Open a fresh WebSocket to the bridge and wire up its handlers. */
  const connectSocket = useCallback((): WebSocket => {
    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${protocol}://${window.location.host}/ws`);

    ws.onopen = () => {
      setConnectionStatus("CONNECTED");
    };
    ws.onmessage = (event: MessageEvent<string>) => {
      try {
        handleBridgeMessage(event.data);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error("Bad bridge message:", message);
      }
    };
    ws.onclose = () => {
      setConnectionStatus("DISCONNECTED");
      // If the socket closes while a stream was active and not finished, mark
      // it dropped so the UI shows the red cursor / faded text state.
      setStreamStatus((prev) => (prev === "STREAMING" || prev === "RESUMED" ? "DROPPED" : prev));
      journalTokensAtDropRef.current = journalTokens;
      if (socketRef.current === ws) {
        socketRef.current = null;
      }
    };
    ws.onerror = () => {
      // The bridge may be unreachable; the close handler will surface the
      // disconnect state. Nothing to do here beyond logging.
      console.error("WebSocket error");
    };

    socketRef.current = ws;
    return ws;
  }, [handleBridgeMessage, journalTokens]);

  /** Send a message to the currently open socket, if any. */
  const send = useCallback((message: BrowserToBridgeMessage): void => {
    const ws = socketRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }, []);

  /** Generate: fresh session, fresh socket, fresh inference. */
  const handleGenerate = useCallback(() => {
    const id = randomSessionId();
    setSessionId(id);
    setTokens([]);
    setJournalTokens(0);
    setServerAckedIndex(0);
    setStreamStatus("STREAMING");
    journalTokensAtDropRef.current = 0;
    flashFromRef.current = null;
    replayEndRef.current = null;
    lastAckIndexRef.current = 0;
    setLastAckIndex(0);

    const ws = connectSocket();
    ws.onopen = () => {
      send({ type: "generate", prompt: DEMO_PROMPT, session_id: id });
    };
  }, [connectSocket, send, setLastAckIndex]);

  /** Kill Stream: close the WebSocket abruptly to simulate a drop. */
  const handleKill = useCallback(() => {
    const ws = socketRef.current;
    if (ws) {
      ws.close();
    }
    setStreamStatus("DROPPED");
    setConnectionStatus("DISCONNECTED");
    journalTokensAtDropRef.current = journalTokens;
    pushEvent(`Stream dropped at token ${lastAckIndexRef.current}`);
  }, [journalTokens, pushEvent]);

  /** Resume: open a brand-new WebSocket and ask the server to replay. */
  const handleResume = useCallback(() => {
    if (!sessionId) {
      return;
    }
    // Resume from the FIRST token the client has not received yet: the last
    // confirmed index + 1. (With zero rendered tokens, resume from 0.)
    const resumeFrom = tokens.length === 0 ? 0 : lastAckIndexRef.current + 1;
    setStreamStatus("RESUMED");
    const ws = connectSocket();
    ws.onopen = () => {
      send({ type: "resume", session_id: sessionId, resume_from_token: resumeFrom });
    };
  }, [connectSocket, send, sessionId, tokens.length]);

  // Periodic ACKs: while a session exists, confirm our position so the server
  // can trim journaled tokens we no longer need. No-ops when the socket is
  // closed (Kill Stream), so a dropped client simply stops acking.
  useEffect(() => {
    if (!sessionId) {
      return;
    }
    const timer = window.setInterval(() => {
      const acked = lastAckIndexRef.current;
      if (acked >= 0) {
        send({ type: "ack", session_id: sessionId, acked_token_index: acked });
      }
    }, ACK_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [sessionId, send]);

  // Clean up flash timers on unmount.
  useEffect(() => {
    const timers = flashTimersRef.current;
    return () => {
      for (const timer of timers) {
        window.clearTimeout(timer);
      }
    };
  }, []);

  const text = tokens.map((token) => token.text).join("");

  return (
    <div className="app">
      <header className="app-header">
        <h1>Dead Reckoning</h1>
        <p className="tagline">
          Resumable gRPC server-streaming for LLM inference. The call drops, the
          model keeps writing to the journal, and the reconnect replays from your
          last confirmed token.
        </p>
      </header>
      <main className="panels">
        <TokenStream
          text={text}
          tokens={tokens}
          flashingIndices={flashing}
          sessionId={sessionId}
          streamStatus={streamStatus}
          connectionStatus={connectionStatus}
          onGenerate={handleGenerate}
          onKill={handleKill}
          onResume={handleResume}
        />
        <JournalInspector
          sessionId={sessionId}
          journalTokens={journalTokens}
          lastAckIndex={lastAckIndex}
          serverAckedIndex={serverAckedIndex}
          streamStatus={streamStatus}
          connectionStatus={connectionStatus}
          events={events}
        />
      </main>
      <footer className="app-footer">
        Built by{" "}
        <a href="https://harishkotra.me" target="_blank" rel="noreferrer">
          Harish Kotra
        </a>{" "}
        · Check out{" "}
        <a href="https://dailybuild.xyz" target="_blank" rel="noreferrer">
          other builds
        </a>
      </footer>
    </div>
  );
}
