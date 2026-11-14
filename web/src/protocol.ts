// web/src/protocol.ts
//
// The browser <-> bridge WebSocket message protocol, typed. Mirrors the JSON
// messages defined in server/bridge.ts so both sides stay in sync.

/** Messages the bridge sends to the browser. */
export type BridgeToBrowserMessage =
  | {
      type: "token";
      token: string;
      token_index: number;
      is_final: boolean;
      replayed: boolean;
      session_id: string;
    }
  | {
      type: "journal_state";
      session_id: string;
      token_count: number;
    }
  | {
      type: "ack_state";
      session_id: string;
      acked_token_index: number;
      journaled_token_count: number;
      first_retained_index: number;
    }
  | {
      type: "stream_started";
      session_id: string;
      resume_from_token: number;
    }
  | {
      type: "stream_resumed";
      session_id: string;
      resume_from_token: number;
    }
  | {
      type: "stream_dropped";
      session_id: string;
      last_token_index: number;
    }
  | {
      type: "resync_required";
      session_id: string;
      first_retained_index: number;
    }
  | {
      type: "cancelled";
      session_id: string;
      cancelled_at_token: number;
      was_running: boolean;
    };

/** Messages the browser sends to the bridge. */
export type BrowserToBridgeMessage =
  | { type: "generate"; prompt: string; session_id: string }
  | { type: "resume"; session_id: string; resume_from_token: number }
  | { type: "ack"; session_id: string; acked_token_index: number }
  | { type: "cancel"; session_id: string };

/** Generate a random session id for a fresh run. */
export function randomSessionId(): string {
  return `sess_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}
