// web/src/components/JournalInspector.tsx
//
// Right panel: live session state received from the bridge. Shows the session
// id, the journal's token count, the last ACKed token index (client cursor),
// the server's confirmed watermark (from periodic ACKs), and color-coded
// stream/connection status. A timeline at the bottom keeps the last 10 events
// (start, drop, resume) so the resumption story is visible at a glance.

import type { StreamStatus, TimelineEvent } from "../App";

interface JournalInspectorProps {
  sessionId: string | null;
  journalTokens: number;
  lastAckIndex: number;
  /** The server's confirmed watermark, echoed back after periodic ACKs. */
  serverAckedIndex: number;
  streamStatus: StreamStatus;
  connectionStatus: "CONNECTED" | "DISCONNECTED";
  events: TimelineEvent[];
}

/** Stream status color-coded per the spec: green/red/yellow. */
function statusClass(status: StreamStatus): string {
  switch (status) {
    case "STREAMING":
      return "status-streaming";
    case "DROPPED":
      return "status-dropped";
    case "RESUMED":
      return "status-resumed";
    case "IDLE":
      return "status-idle";
  }
}

/** Truncate a session id to 8 characters for display. */
function shortSessionId(id: string | null): string {
  if (!id) {
    return "—";
  }
  return id.length <= 8 ? id : `${id.slice(0, 8)}…`;
}

export function JournalInspector({
  sessionId,
  journalTokens,
  lastAckIndex,
  serverAckedIndex,
  streamStatus,
  connectionStatus,
  events,
}: JournalInspectorProps): JSX.Element {
  return (
    <section className="panel journal-inspector">
      <h2>Journal Inspector</h2>

      <dl className="fields">
        <div className="field">
          <dt>Session ID</dt>
          <dd className="mono">{shortSessionId(sessionId)}</dd>
        </div>
        <div className="field">
          <dt>Tokens in journal</dt>
          <dd className="mono">{journalTokens}</dd>
        </div>
        <div className="field">
          <dt>Last ACK index</dt>
          <dd className="mono">{lastAckIndex}</dd>
        </div>
        <div className="field">
          <dt>Server ACK watermark</dt>
          <dd className="mono">{serverAckedIndex}</dd>
        </div>
        <div className="field">
          <dt>Stream status</dt>
          <dd>
            <span className={`badge ${statusClass(streamStatus)}`}>{streamStatus}</span>
          </dd>
        </div>
        <div className="field">
          <dt>Connection</dt>
          <dd>
            <span
              className={`badge ${connectionStatus === "CONNECTED" ? "status-streaming" : "status-dropped"}`}
            >
              {connectionStatus}
            </span>
          </dd>
        </div>
      </dl>

      <h3>Timeline</h3>
      <ol className="timeline">
        {events.length === 0 ? (
          <li className="timeline-empty">No events yet — click Generate.</li>
        ) : (
          events.map((event, index) => (
            <li key={`${event.time}-${index}`} className="timeline-event">
              <span className="timeline-time">{event.time}</span>
              <span className="timeline-label">{event.label}</span>
            </li>
          ))
        )}
      </ol>
    </section>
  );
}
