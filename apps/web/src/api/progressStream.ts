// Reads GET /v1/investigations/{id}/events. EventSource cannot send the operator
// token header, so this reads the stream with fetch and parses SSE itself. It
// reconnects with Last-Event-ID; the server replays what was missed from
// PostgreSQL, or sends `reset` when the cursor is unknown.

import { baseUrl } from "./client";
import { LIVE_SCOPE } from "./incidents";
import { clearOperatorToken, getOperatorToken } from "./operatorToken";
import { isProgressEvent, type ProgressEvent } from "./progress";

export type StreamState = "connecting" | "live" | "reconnecting" | "ended" | "failed";

export interface StreamHandlers {
  onEvent: (event: ProgressEvent) => void;
  /** The server did not know the cursor: drop held events; the history follows. */
  onReset: () => void;
  onState: (state: StreamState) => void;
}

export interface StreamOptions {
  /** The last event already held; 0 for the whole history. */
  after: number;
  /** No bytes for this long, keep-alives included, means a dead connection. */
  idleTimeoutMs?: number;
  maxBackoffMs?: number;
}

const DEFAULT_RETRY_MS = 3_000;
const DEFAULT_IDLE_MS = 20_000;
const DEFAULT_MAX_BACKOFF_MS = 30_000;

export interface SseMessage {
  id: string | undefined;
  event: string;
  data: string;
  retry: number | undefined;
}

/** Split complete SSE messages off `buffer`; returns them and the unparsed rest. */
export function parseSse(buffer: string): { messages: SseMessage[]; rest: string } {
  const normalized = buffer.replace(/\r\n?/g, "\n");
  const blocks = normalized.split("\n\n");
  const rest = blocks.pop() ?? "";
  const messages: SseMessage[] = [];
  for (const block of blocks) {
    let id: string | undefined;
    let event = "message";
    const data: string[] = [];
    let retry: number | undefined;
    for (const line of block.split("\n")) {
      if (line === "" || line.startsWith(":")) {
        continue;
      }
      const colon = line.indexOf(":");
      const field = colon === -1 ? line : line.slice(0, colon);
      const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");
      if (field === "id") {
        id = value;
      } else if (field === "event") {
        event = value;
      } else if (field === "data") {
        data.push(value);
      } else if (field === "retry" && /^\d+$/.test(value)) {
        retry = Number(value);
      }
    }
    if (id !== undefined || data.length > 0 || retry !== undefined) {
      messages.push({ id, event, data: data.join("\n"), retry });
    }
  }
  return { messages, rest };
}

class StreamStop extends Error {}

/** Opens the stream and keeps it open until the investigation ends or `close` is called. */
export function openProgressStream(
  investigationId: string,
  replayScope: string,
  handlers: StreamHandlers,
  { after, idleTimeoutMs = DEFAULT_IDLE_MS, maxBackoffMs = DEFAULT_MAX_BACKOFF_MS }: StreamOptions,
): () => void {
  const closed = new AbortController();
  // A function, not a property read, so narrowing does not assume it stays false.
  const isClosed = () => closed.signal.aborted;
  let cursor = after;
  let retryMs = DEFAULT_RETRY_MS;
  let failures = 0;

  const search = replayScope === LIVE_SCOPE ? "" : `?replay_scope=${encodeURIComponent(replayScope)}`;
  const url = `${baseUrl}/v1/investigations/${encodeURIComponent(investigationId)}/events${search}`;

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, ms);
      closed.signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          resolve();
        },
        { once: true },
      );
    });

  /** One connection; true when the investigation ended. */
  async function connect(): Promise<boolean> {
    const attempt = new AbortController();
    const signal = AbortSignal.any([closed.signal, attempt.signal]);
    let idle: ReturnType<typeof setTimeout> | undefined;
    const touch = () => {
      clearTimeout(idle);
      idle = setTimeout(() => {
        attempt.abort();
      }, idleTimeoutMs);
    };
    const headers: Record<string, string> = { Accept: "text/event-stream" };
    const token = getOperatorToken();
    if (token !== null) {
      headers.Authorization = `Bearer ${token}`;
    }
    if (cursor > 0) {
      headers["Last-Event-ID"] = String(cursor);
    }
    try {
      touch();
      const response = await fetch(url, { headers, signal, cache: "no-store" });
      if (response.status === 401) {
        clearOperatorToken({ rejected: true });
        throw new StreamStop("unauthorized");
      }
      if (response.status === 404) {
        throw new StreamStop("not found");
      }
      if (!response.ok || response.body === null) {
        throw new Error(`HTTP ${String(response.status)}`);
      }
      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      let buffer = "";
      let opened = false;
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          return false;
        }
        touch();
        if (!opened) {
          opened = true;
          failures = 0;
          handlers.onState("live");
        }
        const parsed = parseSse(buffer + value);
        buffer = parsed.rest;
        for (const message of parsed.messages) {
          if (message.retry !== undefined) {
            retryMs = message.retry;
          }
          if (message.event === "reset") {
            cursor = 0;
            handlers.onReset();
            continue;
          }
          if (message.event !== "progress") {
            continue;
          }
          let event: unknown;
          try {
            event = JSON.parse(message.data);
          } catch {
            throw new Error("invalid event data");
          }
          if (!isProgressEvent(event)) {
            throw new Error("invalid event");
          }
          const progress = event as ProgressEvent;
          cursor = progress.id;
          handlers.onEvent(progress);
          if (progress.event_type === "investigation.ended") {
            await reader.cancel();
            return true;
          }
        }
      }
    } finally {
      clearTimeout(idle);
    }
  }

  void (async () => {
    handlers.onState("connecting");
    while (!isClosed()) {
      try {
        if (await connect()) {
          handlers.onState("ended");
          return;
        }
        // The server closed at its stream lifetime: reconnect from the cursor.
      } catch (error) {
        if (isClosed()) {
          return;
        }
        if (error instanceof StreamStop) {
          handlers.onState("failed");
          return;
        }
        failures += 1;
      }
      if (isClosed()) {
        return;
      }
      handlers.onState("reconnecting");
      await sleep(Math.min(retryMs * 2 ** Math.max(0, failures - 1), maxBackoffMs));
    }
  })();

  return () => {
    closed.abort();
  };
}
