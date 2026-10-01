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
import type { CookieJar } from 'tough-cookie';

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
  /** Host-only session storage, owned by one IPC connection. */
  cookieJar?: CookieJar;
  /**
   * Headers the host injects into every bridged upstream request — e.g. an
   * in-boundary auth credential. The webview is in-boundary (same process
   * tree as the trusted shell). User sessions stay in the connection's
   * host-side cookie jar; these headers supply a separate host credential
   * where the application requires one. Resolved per request
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
  const { method, path, headers: inHeaders, body, credentials = 'same-origin' } = input as {
    method?: unknown;
    path?: unknown;
    headers?: unknown;
    body?: unknown;
    credentials?: unknown;
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
  if (!['omit', 'same-origin', 'include'].includes(credentials as string)) {
    writeJson(FrameType.ERROR, {
      id: Number(id),
      error: { code: 'bad_input', message: 'sys:http credentials must be omit, same-origin, or include' },
    });
    return;
  }

  const upstreamUrl = new URL(resolveUpstreamBase(deps.upstreamBase) + path);
  const useCookies = credentials !== 'omit' && !!deps.cookieJar;

  const upstreamHeaders: Record<string, string> = {};
  if (inHeaders && typeof inHeaders === 'object') {
    for (const [k, v] of Object.entries(inHeaders as Record<string, unknown>)) {
      const name = k.toLowerCase();
      if (typeof v === 'string' && !HOP_BY_HOP.has(name) && name !== 'cookie' && name !== 'cookie2') {
        upstreamHeaders[name] = v;
      }
    }
  }
  // Host-injected credential (e.g. the in-boundary trusted bearer). Merged
  // last so it wins over any same-named webview-forwarded header; hop-by-hop
  // names are still dropped.
  const injected = deps.injectHeaders?.();
  if (injected) {
    for (const [k, v] of Object.entries(injected)) {
      const name = k.toLowerCase();
      if (typeof v === 'string' && !HOP_BY_HOP.has(name) && name !== 'cookie' && name !== 'cookie2') {
        upstreamHeaders[name] = v;
      }
    }
  }

  let res: Response;
  try {
    let url = upstreamUrl;
    let requestMethod = method.toUpperCase();
    let requestBody = typeof body === 'string' ? body : undefined;
    for (let redirects = 0; ; redirects++) {
      // Cookies are never accepted from webview headers and never cross
      // origins. Recompute for each redirect's path/expiry/security rules.
      delete upstreamHeaders.cookie;
      if (useCookies) {
        const cookie = await deps.cookieJar!.getCookieString(url.href);
        if (cookie) upstreamHeaders.cookie = cookie;
      }
      signal.throwIfAborted();
      res = await fetchImpl(url.href, {
        method: requestMethod,
        headers: upstreamHeaders,
        body: requestBody,
        signal,
        redirect: 'manual',
      });
      if (useCookies) {
        for (const cookie of res.headers.getSetCookie()) {
          await deps.cookieJar!.setCookie(cookie, url.href, { ignoreError: true });
        }
      }
      const location = res.headers.get('location');
      if (![301, 302, 303, 307, 308].includes(res.status) || !location) break;
      await res.body?.cancel();
      const nextUrl = new URL(location, url);
      if (nextUrl.origin !== upstreamUrl.origin) {
        throw new TypeError('sys:http refuses a cross-origin redirect');
      }
      if (redirects >= 20) throw new TypeError('sys:http redirect limit exceeded');
      if (((res.status === 301 || res.status === 302) && requestMethod === 'POST') ||
          (res.status === 303 && requestMethod !== 'GET' && requestMethod !== 'HEAD')) {
        requestMethod = 'GET';
        requestBody = undefined;
        delete upstreamHeaders['content-type'];
        delete upstreamHeaders['content-length'];
      }
      url = nextUrl;
    }
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
    const name = k.toLowerCase();
    // Match browser fetch: Set-Cookie (including HttpOnly values) is a
    // forbidden response header, consumed only by the trusted host jar.
    if (!HOP_BY_HOP.has(name) && name !== 'set-cookie' && name !== 'set-cookie2') responseHeaders[k] = v;
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
