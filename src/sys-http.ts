/**
 * `sys:http` — privileged HTTP-over-IPC bridge.
 *
 * The webview's `fetch` / `EventSource` polyfills dispatch through this
 * pseudo-tool so same-origin /api/* requests never leave the IPC channel.
 * That removes the webview's HTTP-pool-per-host pressure entirely (the
 * connection-pool exhaustion that hangs the live agent-thinking popover
 * on the harness page; see host-architecture-2026-05-20-v2.md Phase 1).
 *
 * Wire shape over the existing IPC protocol:
 *
 *   REQUEST  { id, toolName: 'sys:http',
 *              input: { method, path, headers?, body? } }
 *   ─►  fetch <upstream-base><path> with forwarded method/headers/body
 *
 *   EVENT_JSON { id, name: 'head', data: { status, headers } }
 *     Always first, before any body chunk.
 *
 *   For `text/event-stream` responses:
 *     EVENT_JSON { id, name: 'sse-chunk', data: <raw SSE wire bytes as UTF-8> }
 *       Repeated. The client polyfill feeds these to a standard SSE
 *       line parser. No DONE until the upstream stream ends (or CANCEL).
 *
 *   For non-SSE responses:
 *     EVENT_BIN  [id][nameLen][name='body'][raw bytes]
 *       Repeated; chunked under the 16-MiB frame cap.
 *     DONE { id, result: { content: [] } } when the body ends.
 *
 *   ERROR { id, error: { code, message } } on bad input / aborted /
 *     upstream failure. Terminal.
 *
 * The upstream target is the operator's own loopback HTTP port — the
 * Next sidecar today. This is *not* HTTP from the webview's perspective
 * (the webview only ever speaks IPC); it's a Node→Node loopback hop
 * that sidesteps the browser's per-host connection pool entirely.
 */

import { FrameType, encodeEventBinPayload, type FrameTypeValue } from '@papercusp/ipc-framing';

// RFC 7230 hop-by-hop headers + h2 forbidden headers — must not be
// forwarded across the bridge in either direction.
const HOP_BY_HOP = new Set([
  'connection',
  'keep-alive',
  'transfer-encoding',
  'upgrade',
  'te',
  'trailers',
  'proxy-authenticate',
  'proxy-authorization',
]);

// Largest EVENT_BIN body chunk we emit; well under the 16-MiB frame cap
// to leave room for the id/name prefix in the frame payload.
const MAX_BODY_CHUNK_BYTES = 15 * 1024 * 1024;

export interface SysHttpDeps {
  writeFrame: (type: FrameTypeValue, payload: Buffer) => boolean;
  writeJson: (type: FrameTypeValue, value: unknown) => boolean;
  logger: { info: (msg: string) => void; warn: (msg: string) => void };
  /** Override the upstream base for tests; in prod resolved from env. */
  upstreamBase?: string;
  /** Override fetch for tests. */
  fetchImpl?: typeof fetch;
  /**
   * Headers the host injects into every bridged upstream request — e.g. an
   * in-boundary auth credential. The webview is in-boundary (same process
   * tree as the trusted shell), but its HttpOnly session cookie cannot ride
   * the IPC bridge: JS can neither read it to forward nor honor the
   * Set-Cookie that comes back. So a host that gates `/api/*` on a session
   * supplies the equivalent trusted credential here. Resolved per request
   * so the host can return a freshly-read (rotatable) token. Merged OVER the
   * forwarded headers (host wins, so a webview header can't shadow the
   * credential); hop-by-hop names are still dropped. The package itself
   * never reads any credential — it stays domain-free.
   */
  injectHeaders?: () => Record<string, string> | undefined;
}

function resolveUpstreamBase(override?: string): string {
  if (override) return override;
  // Generic, unbranded env fallback. The host normally injects
  // `upstreamBaseUrl` (mapped from its own env) via the server options.
  if (process.env.IPC_UPSTREAM_BASE) return process.env.IPC_UPSTREAM_BASE;
  const port = process.env.PORT || '3055';
  return `http://127.0.0.1:${port}`;
}

export async function handleSysHttp(
  id: bigint,
  input: unknown,
  signal: AbortSignal,
  deps: SysHttpDeps,
): Promise<void> {
  const { writeFrame, writeJson, logger } = deps;
  const fetchImpl = deps.fetchImpl ?? fetch;

  // Input validation. Reject anything that isn't a same-origin relative
  // path — the bridge is not an open proxy.
  if (!input || typeof input !== 'object') {
    writeJson(FrameType.ERROR, {
      id: Number(id),
      error: { code: 'bad_input', message: 'sys:http input must be an object' },
    });
    return;
  }
  const { method, path, headers: inHeaders, body } = input as {
    method?: unknown;
    path?: unknown;
    headers?: unknown;
    body?: unknown;
  };
  if (typeof method !== 'string' || typeof path !== 'string') {
    writeJson(FrameType.ERROR, {
      id: Number(id),
      error: { code: 'bad_input', message: 'sys:http requires { method, path }' },
    });
    return;
  }
  if (!path.startsWith('/') || path.includes('://') || path.includes('/../')) {
    writeJson(FrameType.ERROR, {
      id: Number(id),
      error: {
        code: 'bad_path',
        message: 'sys:http path must be a relative path without traversal',
      },
    });
    return;
  }

  const upstreamUrl = resolveUpstreamBase(deps.upstreamBase) + path;

  const upstreamHeaders: Record<string, string> = {};
  if (inHeaders && typeof inHeaders === 'object') {
    for (const [k, v] of Object.entries(inHeaders as Record<string, unknown>)) {
      if (typeof v === 'string' && !HOP_BY_HOP.has(k.toLowerCase())) {
        upstreamHeaders[k] = v;
      }
    }
  }
  // Host-injected credential (e.g. the in-boundary trusted bearer). Merged
  // last so it wins over any same-named webview-forwarded header; hop-by-hop
  // names are still dropped.
  const injected = deps.injectHeaders?.();
  if (injected) {
    for (const [k, v] of Object.entries(injected)) {
      if (typeof v === 'string' && !HOP_BY_HOP.has(k.toLowerCase())) {
        upstreamHeaders[k] = v;
      }
    }
  }

  let res: Response;
  try {
    res = await fetchImpl(upstreamUrl, {
      method,
      headers: upstreamHeaders,
      body: typeof body === 'string' ? body : undefined,
      signal,
    });
  } catch (err) {
    if (signal.aborted) {
      writeJson(FrameType.ERROR, {
        id: Number(id),
        error: { code: 'aborted', message: 'request aborted' },
      });
    } else {
      writeJson(FrameType.ERROR, {
        id: Number(id),
        error: {
          code: 'upstream_error',
          message: err instanceof Error ? err.message : String(err),
        },
      });
    }
    return;
  }

  const contentType = res.headers.get('content-type') ?? '';
  const responseHeaders: Record<string, string> = {};
  res.headers.forEach((v, k) => {
    if (!HOP_BY_HOP.has(k.toLowerCase())) responseHeaders[k] = v;
  });

  writeJson(FrameType.EVENT_JSON, {
    id: Number(id),
    name: 'head',
    data: { status: res.status, headers: responseHeaders },
  });

  if (!res.body) {
    writeJson(FrameType.DONE, { id: Number(id), result: { content: [] } });
    return;
  }

  const isSse = contentType.toLowerCase().includes('text/event-stream');
  const reader = res.body.getReader();
  const decoder = new TextDecoder();

  try {
    while (true) {
      if (signal.aborted) break;
      const { value, done } = await reader.read();
      if (done) break;
      if (!value || value.length === 0) continue;
      if (isSse) {
        writeJson(FrameType.EVENT_JSON, {
          id: Number(id),
          name: 'sse-chunk',
          data: decoder.decode(value, { stream: true }),
        });
      } else {
        let offset = 0;
        while (offset < value.length) {
          if (signal.aborted) break;
          const end = Math.min(offset + MAX_BODY_CHUNK_BYTES, value.length);
          const slice = value.subarray(offset, end);
          writeFrame(FrameType.EVENT_BIN, encodeEventBinPayload(id, 'body', slice));
          offset = end;
        }
      }
    }
    // Flush any bytes the streaming TextDecoder buffered mid-multibyte-UTF-8
    // sequence at end-of-stream (e.g. truncated upstream). decode() with no
    // args finalizes the decoder; emit the remainder so SSE text isn't
    // silently dropped.
    if (isSse && !signal.aborted) {
      const tail = decoder.decode();
      if (tail) {
        writeJson(FrameType.EVENT_JSON, { id: Number(id), name: 'sse-chunk', data: tail });
      }
    }
  } catch (err) {
    if (!signal.aborted) {
      const message = err instanceof Error ? err.message : String(err);
      if (isSse) {
        // An SSE stream has no in-band "end" frame — the server signals
        // end-of-stream by simply CLOSING the socket, which surfaces here as
        // undici's `terminated` (or a comparable connection-reset) on the next
        // read. That is the NORMAL lifecycle of a long-lived SSE upstream
        // (operator restart, keep-alive/idle rotation, a deliberate recycle),
        // not a fault. The IpcEventSource client treats a graceful DONE and a
        // `stream_error` ERROR IDENTICALLY — both → reconnect with Last-Event-ID
        // (see ipc-event-source.ts runOnce: `done` → 'drop', non-IPC-unavailable
        // `error` → 'drop') — so emitting the DONE below is behaviorally
        // identical client-side while no longer mischaracterizing a routine drop
        // as a WARN-level "error". Log at info WITH request context (method+path)
        // so the genuinely-rare case where it matters is diagnosable: the bare
        // `sys:http stream error: terminated` named neither the stream nor the
        // route, making the recurring bg-host warnings un-triageable.
        logger.info(`sys:http SSE upstream closed (${method} ${path}): ${message}`);
        // Fall through to the graceful DONE emitted after the finally block.
      } else {
        // A NON-SSE body that terminates mid-stream IS a real truncation the
        // client must see (a partial JSON/binary response), so it stays a hard
        // error — now with request context so it can actually be diagnosed.
        logger.warn(`sys:http stream error (${method} ${path}): ${message}`);
        writeJson(FrameType.ERROR, {
          id: Number(id),
          error: { code: 'stream_error', message },
        });
        return;
      }
    }
  } finally {
    try {
      reader.releaseLock();
    } catch {
      /* already released */
    }
  }

  if (signal.aborted) {
    writeJson(FrameType.ERROR, {
      id: Number(id),
      error: { code: 'aborted', message: 'request aborted' },
    });
  } else {
    writeJson(FrameType.DONE, { id: Number(id), result: { content: [] } });
  }
}
