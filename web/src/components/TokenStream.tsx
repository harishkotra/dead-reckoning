// web/src/components/TokenStream.tsx
//
// Left panel: the live token display. Renders the accumulated story text in a
// dark monospace area, word by word as tokens arrive over WebSocket.
//
// Visual states driven by the stream lifecycle:
//   - Active stream: blinking green cursor at the end of the text.
//   - Dropped/paused: cursor turns red, text opacity drops to 0.6.
//   - Resumed: tokens that were just replayed from the journal flash yellow
//     briefly, then settle back to white.
//
// The three demo buttons live at the bottom: Generate, Kill Stream, Resume.

import type { StreamStatus } from "../App";

/** One journaled token as rendered in the left panel. */
export interface Token {
  index: number;
  text: string;
}

interface TokenStreamProps {
  text: string;
  tokens: Token[];
  /** Indices of tokens currently flashing yellow (just replayed). */
  flashingIndices: ReadonlySet<number>;
  sessionId: string | null;
  streamStatus: StreamStatus;
  connectionStatus: "CONNECTED" | "DISCONNECTED";
  onGenerate: () => void;
  onKill: () => void;
  onResume: () => void;
}

/** Status text shown above the token area. */
function statusLabel(status: StreamStatus): string {
  switch (status) {
    case "IDLE":
      return "Idle";
    case "STREAMING":
      return "Streaming";
    case "DROPPED":
      return "Dropped — click Resume to continue";
    case "RESUMED":
      return "Resumed";
  }
}

export function TokenStream({
  text,
  tokens,
  flashingIndices,
  sessionId,
  streamStatus,
  connectionStatus,
  onGenerate,
  onKill,
  onResume,
}: TokenStreamProps): JSX.Element {
  const isActive = streamStatus === "STREAMING" || streamStatus === "RESUMED";
  const isDropped = streamStatus === "DROPPED";

  return (
    <section className="panel token-stream">
      <div className="panel-header">
        <span className="session-label">
          {sessionId ? `session: ${sessionId}` : "session: —"}
        </span>
        <span className={`stream-status status-${streamStatus.toLowerCase()}`}>
          {statusLabel(streamStatus)}
        </span>
      </div>

      <div className={`token-area ${isDropped ? "dropped" : ""}`}>
        {text.length === 0 ? (
          <span className="placeholder">
            Click <strong>Generate</strong> to start a 500-word story, then{" "}
            <strong>Kill Stream</strong> mid-generation and{" "}
            <strong>Resume</strong> to watch it continue from the exact token.
          </span>
        ) : (
          <>
            {tokens.map((token) => (
              <span
                key={token.index}
                className={flashingIndices.has(token.index) ? "token flash" : "token"}
              >
                {token.text}
              </span>
            ))}
            {isActive && <span className="cursor" />}
          </>
        )}
      </div>

      <div className="controls">
        <button className="btn btn-generate" onClick={onGenerate}>
          Generate
        </button>
        <button
          className="btn btn-kill"
          onClick={onKill}
          disabled={!isActive}
          title="Close the WebSocket abruptly to simulate a dropped stream"
        >
          Kill Stream
        </button>
        <button
          className="btn btn-resume"
          onClick={onResume}
          disabled={!sessionId || streamStatus === "IDLE"}
          title="Reconnect with a new WebSocket and resume from the last token"
        >
          Resume
        </button>
      </div>

      <div className="conn-status">
        WebSocket:{" "}
        <span className={connectionStatus === "CONNECTED" ? "conn-ok" : "conn-bad"}>
          {connectionStatus}
        </span>
      </div>
    </section>
  );
}
