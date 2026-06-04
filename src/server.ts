/**
 * Endpoint IPC server — webview→Tauri→Node fast path for the endpoint
 * system. See `apps/operator/lib/endpoint-ipc/PROTOCOL.md` for the wire
 * spec and trust model.
 *
 * Lifecycle:
 *   1. Caller invokes `startEndpointIpcServer({ socketPath, host })`.
 *   2. Server binds, unlinks any stale socket, prints the
 *      `<readyToken> socket=<path>` line to stdout (default token
 *      `IPC_READY`; the host sets it to whatever its client expects).
 *   3. The client connects, sends REQUEST frames, receives EVENT_JSON /
 *      EVENT_BIN / DONE / ERROR frames, may send CANCEL.
 *   4. On process exit, server unlinks the socket file.
 *
 * Host coupling: the server speaks the wire protocol and owns the
 * dispatch loop, but it does NOT know how to resolve or run projected
 * tools, nor what the active workspace is. Those three operations are
 * injected via the `host` seam (`IpcEndpointHost`) so this package
 * carries zero dependency on any concrete tool registry or operator
 * state. The Papercusp operator binds the host in
 * `apps/operator/lib/endpoint-ipc/server.ts`.
 *
 * Trust: synthetic operator principal, sig-verification skipped. See
 * transports.mdx § Trust model.
 */

import net from 'node:net';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  FrameType,
  FrameDecoder,
  FrameError,
  encodeFrame,
  encodeEventBinPayload,
  type Frame,
  type FrameTypeValue,
} from '@papercusp/ipc-framing';
import type {
  ProjectedTool,
  UnifiedToolContext,
  DispatchProjectedDeps,
  DispatchStreamEvent,
} from '@papercusp/tooldef';
import { handleSysHttp } from './sys-http';

/**
 * The host seam — the three operations the IPC server needs from its
 * embedding application, injected so this package stays free of any
 * concrete tool-registry / workspace-state dependency.
 *
 * Crucially, the caller supplies its OWN `lookupByMcpName` +
 * `dispatchProjectedToolStream`, so they read/write the same projected-
 * tool registry singleton the rest of the host uses. There is no
 * package-level import of those functions, which removes any chance of
 * a second module instance of the registry sneaking in.
 */
export interface IpcEndpointHost {
  /** Resolve a projected tool by its MCP name from the host's registry. */
  lookupByMcpName(name: string): ProjectedTool | undefined;
  /**
   * Stream-dispatch a projected tool through the host's full gating
   * pipeline (role check, quota, telemetry, idle watchdog). Yields
   * exactly one terminal (`done` | `error`) event.
   */
  dispatchProjectedToolStream(
    tool: ProjectedTool,
    toolName: string,
    input: unknown,
    ctx: UnifiedToolContext,
    deps: DispatchProjectedDeps,
  ): AsyncIterable<DispatchStreamEvent>;
  /** The active workspace id stamped onto the synthetic operator ctx. */
  getWorkspaceId(): string;
}

/**
 * Returns the default per-process socket path on this OS. Neutral,
 * unbranded fallback — hosts that want a specific location (e.g. under a
 * project config dir) pass an explicit `socketPath` instead.
 */
export function defaultSocketPath(pid: number = process.pid): string {
  if (process.platform === 'win32') {
    return `\\\\.\\pipe\\ipc-endpoint-server-${pid}`;
  }
  return path.join(os.tmpdir(), 'ipc-endpoint-server', `${pid}.sock`);
}

export interface StartEndpointIpcServerOptions {
  /**
   * Host seam — resolve/dispatch projected tools + the active workspace
   * id. Required: without it the server can't run any tool.
   */
  host: IpcEndpointHost;
  /** Override the default OS socket / named-pipe path (see `defaultSocketPath`). */
  socketPath?: string;
  /**
   * Token prefix for the stdout handshake line, printed as
   * `<readyToken> socket=<path>` once the server is bound. A spawning
   * parent watches stdout for this exact prefix. Default `'IPC_READY'`;
   * the host sets it to whatever its client expects.
   */
  readyToken?: string;
  /**
   * Upstream base URL for the `sys:http` bridge — relative `/api/*` calls
   * a tool makes are proxied here. When unset, falls back to the generic
   * `IPC_UPSTREAM_BASE` env var, then `http://127.0.0.1:${PORT||3055}`.
   */
  upstreamBaseUrl?: string;
  /**
   * Allowed tool names. When unset, every registered projected tool is
   * dispatchable; when set, REQUESTs for other tool names return an
   * `ERROR { code: 'tool_not_allowed' }`. The Phase-3 rollout starts
   * with `['operator:scan']` and widens once burn-in is clean.
   */
  allowedTools?: ReadonlySet<string> | readonly string[];
  /**
   * Dispatcher deps. Defaults to `{}` (no PG telemetry / quota). The
   * host wires a real `recordInvocation` + `readQuotaState` here.
   */
  deps?: DispatchProjectedDeps;
  /**
   * Optional logger. Defaults to console.log/warn with an [ipc] prefix.
   */
  logger?: { info: (msg: string) => void; warn: (msg: string) => void };
  /**
   * Headers the host injects into every `sys:http` bridged upstream request.
   * The webview is in-boundary but its HttpOnly session cookie can't ride the
   * IPC bridge, so a host that gates `/api/*` on a session supplies the
   * equivalent trusted credential here (resolved per request for rotation).
   * See `SysHttpDeps.injectHeaders`. The package never reads any credential
   * itself — this stays domain-free.
   */
  sysHttpInjectHeaders?: () => Record<string, string> | undefined;
}

export interface EndpointIpcServer {
  /** Underlying Node server (for tests). */
  server: net.Server;
  /** Resolved socket path the server is listening on. */
  socketPath: string;
  /** Number of currently-open client connections. */
  connectionCount(): number;
  /** Close listener + all open connections; resolves when fully shut down. */
  close(): Promise<void>;
}

/**
 * Start listening for IPC connections. Resolves once the server has
 * bound and printed the ready line to stdout.
 *
 * Bind failures (path-in-use, EACCES) reject with the underlying error.
 */
export async function startEndpointIpcServer(
  options: StartEndpointIpcServerOptions,
): Promise<EndpointIpcServer> {
  const host = options.host;
  const socketPath = options.socketPath ?? defaultSocketPath();
  const readyToken = options.readyToken ?? 'IPC_READY';
  const upstreamBase = options.upstreamBaseUrl;
  const sysHttpInjectHeaders = options.sysHttpInjectHeaders;
  const allowed =
    options.allowedTools !== undefined
      ? new Set(options.allowedTools)
      : null;
  const deps = options.deps ?? {};
  const logger =
    options.logger ?? {
      info: (m) => console.log(`[ipc] ${m}`),
      warn: (m) => console.warn(`[ipc] ${m}`),
    };

  if (process.platform !== 'win32') {
    await fs.promises.mkdir(path.dirname(socketPath), {
      recursive: true,
      mode: 0o700,
    });
    // Stale socket cleanup. Ignore ENOENT.
    try {
      await fs.promises.unlink(socketPath);
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
    }
  }

  const connections = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    connections.add(socket);
    handleConnection(socket, { allowed, deps, logger, host, upstreamBase, sysHttpInjectHeaders });
    socket.on('close', () => {
      connections.delete(socket);
    });
  });

  server.on('error', (err) => {
    logger.warn(`server error: ${err instanceof Error ? err.message : String(err)}`);
  });

  await new Promise<void>((resolve, reject) => {
    const onError = (err: Error): void => {
      server.off('error', onError);
      reject(err);
    };
    server.once('error', onError);
    server.listen(socketPath, () => {
      server.off('error', onError);
      resolve();
    });
  });

  // Stdout-ready-line per PROTOCOL.md. The spawning client waits on this
  // exact prefix (the host sets it via `readyToken`).
  process.stdout.write(`${readyToken} socket=${socketPath}\n`);

  return {
    server,
    socketPath,
    connectionCount: () => connections.size,
    close: async () => {
      for (const c of connections) c.destroy();
      connections.clear();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      if (process.platform !== 'win32') {
        try {
          await fs.promises.unlink(socketPath);
        } catch {
          /* already gone */
        }
      }
    },
  };
}

/* ─── Per-connection handler ────────────────────────────────────────── */

interface PerConnectionDeps {
  allowed: Set<string> | null;
  deps: DispatchProjectedDeps;
  logger: { info: (msg: string) => void; warn: (msg: string) => void };
  host: IpcEndpointHost;
  /** Upstream base for the sys:http bridge; undefined → resolved from env. */
  upstreamBase?: string;
  /** Host credential injected into sys:http upstream requests; see options. */
  sysHttpInjectHeaders?: () => Record<string, string> | undefined;
}

function handleConnection(socket: net.Socket, deps: PerConnectionDeps): void {
  const dec = new FrameDecoder();
  // Per-call AbortControllers, keyed by request id.
  const inFlight = new Map<bigint, AbortController>();
  // Cancel ids that arrived BEFORE their REQUEST. The handler checks
  // this set on registration and pre-aborts the new controller. Without
  // this, a CANCEL frame that races ahead of its REQUEST (possible on
  // pipelined writes) would silently no-op and the tool would run to
  // completion. Bounded by lastSeenId so we don't grow indefinitely —
  // monotonic id check drops anything reused.
  const preCancelledIds = new Set<bigint>();
  let lastSeenId: bigint = 0n;

  const writeFrame = (type: FrameTypeValue, payload: Buffer): boolean => {
    if (socket.destroyed) return false;
    // Returns false when write buffer is full; we don't currently apply
    // back-pressure on emit (the bounded-mpsc-10k design is implemented
    // on the Rust client side). Node sockets buffer in memory.
    return socket.write(encodeFrame(type, payload));
  };
  const writeJson = (type: FrameTypeValue, value: unknown): boolean =>
    writeFrame(type, Buffer.from(JSON.stringify(value), 'utf8'));

  const sendError = (id: bigint, code: string, message: string): void => {
    writeJson(FrameType.ERROR, {
      id: Number(id),
      error: { code, message },
    });
  };

  socket.on('data', (chunk: Buffer) => {
    try {
      dec.push(chunk);
    } catch (err) {
      if (err instanceof FrameError) {
        deps.logger.warn(`frame decode error: ${err.message}; closing connection`);
        socket.destroy();
        return;
      }
      throw err;
    }
    for (const frame of dec.drain()) {
      void dispatchFrame(frame).catch((err) => {
        deps.logger.warn(
          `unexpected dispatchFrame throw: ${err instanceof Error ? err.message : String(err)}`,
        );
      });
    }
  });

  socket.on('close', () => {
    for (const ac of inFlight.values()) ac.abort();
    inFlight.clear();
  });

  socket.on('error', (err) => {
    deps.logger.warn(`socket error: ${err.message}`);
  });

  async function dispatchFrame(frame: Frame): Promise<void> {
    if (frame.type === FrameType.REQUEST) {
      await handleRequest(frame.payload);
    } else if (frame.type === FrameType.CANCEL) {
      handleCancel(frame.payload);
    } else {
      deps.logger.warn(
        `unexpected client frame type 0x${frame.type.toString(16)}; closing`,
      );
      socket.destroy();
    }
  }

  async function handleRequest(payload: Buffer): Promise<void> {
    let parsed: { id: number; toolName: string; input?: unknown };
    try {
      parsed = JSON.parse(payload.toString('utf8'));
    } catch {
      deps.logger.warn('REQUEST payload not JSON; closing');
      socket.destroy();
      return;
    }
    if (
      typeof parsed.id !== 'number' ||
      !Number.isFinite(parsed.id) ||
      typeof parsed.toolName !== 'string'
    ) {
      deps.logger.warn('REQUEST missing id/toolName; closing');
      socket.destroy();
      return;
    }
    const id = BigInt(parsed.id);
    // Monotonic id check — protocol requires strict-increasing per
    // connection.
    if (id <= lastSeenId) {
      deps.logger.warn(`non-monotonic REQUEST id=${id} (last=${lastSeenId}); closing`);
      socket.destroy();
      return;
    }
    lastSeenId = id;

    if (deps.allowed && !deps.allowed.has(parsed.toolName)) {
      sendError(id, 'tool_not_allowed', `tool '${parsed.toolName}' is not on the IPC allowlist`);
      return;
    }

    // sys:http — privileged HTTP-over-IPC bridge for the webview's
    // fetch/EventSource polyfills. Not a defineTool projection; handled
    // here so all /api/* traffic from the webview can ride the existing
    // UDS instead of opening webview HTTP connections. See sys-http.ts
    // for the wire shape. Still passes through the allowlist above for
    // uniform gating + telemetry intent.
    if (parsed.toolName === 'sys:http') {
      const abort = new AbortController();
      inFlight.set(id, abort);
      if (preCancelledIds.delete(id)) abort.abort();
      void handleSysHttp(id, parsed.input, abort.signal, {
        writeFrame,
        writeJson,
        logger: deps.logger,
        upstreamBase: deps.upstreamBase,
        injectHeaders: deps.sysHttpInjectHeaders,
      })
        .catch((err) =>
          sendError(id, 'sys_http_error', err instanceof Error ? err.message : String(err)),
        )
        .finally(() => {
          inFlight.delete(id);
        });
      return;
    }

    const tool = deps.host.lookupByMcpName(parsed.toolName);
    if (!tool) {
      sendError(id, 'unknown_tool', `no projected tool named '${parsed.toolName}'`);
      return;
    }

    const abort = new AbortController();
    inFlight.set(id, abort);
    // Race: a CANCEL frame for this id may have arrived BEFORE the
    // REQUEST (pipelined writes). Pre-abort if so.
    if (preCancelledIds.delete(id)) {
      abort.abort();
    }

    // Synthetic operator principal. The webview is in-boundary; the
    // signed-URL model doesn't apply (see transports.mdx § Trust model).
    // workspaceId comes from the host (matches what the HTTP route's
    // `activeWorkspaceId()` returns) so per-workspace state and RLS
    // resolve correctly.
    const ctx: UnifiedToolContext = {
      log: (m) => deps.logger.info(`[${parsed.toolName}] ${m}`),
      signal: abort.signal,
      progress: () => { /* dispatchProjectedToolStream overrides this */ },
      emit: () => { /* dispatchProjectedToolStream overrides this */ },
      transport: 'ipc',
      workspaceId: deps.host.getWorkspaceId(),
      role: 'operator',
      // Phase 4 T2.2: UUIDs match the HTTP-side decision (reviewer
      // concern #4 in v1 plan review). The per-connection `id` is
      // already unique within one socket but multiple webviews to
      // one sidecar could conflict on `ipc-1` etc; UUIDs sidestep.
      runId: globalThis.crypto.randomUUID(),
      spawnId: globalThis.crypto.randomUUID(),
    };

    const wireKinds = tool.eventWireKinds ?? {};

    try {
      for await (const ev of deps.host.dispatchProjectedToolStream(
        tool,
        parsed.toolName,
        parsed.input ?? {},
        ctx,
        deps.deps,
      )) {
        if (ev.kind === 'event') {
          const kind = wireKinds[ev.name];
          if (
            kind === 'binary' &&
            ev.data instanceof Uint8Array
          ) {
            writeFrame(
              FrameType.EVENT_BIN,
              encodeEventBinPayload(id, ev.name, ev.data),
            );
          } else {
            writeJson(FrameType.EVENT_JSON, {
              id: Number(id),
              name: ev.name,
              data: ev.data,
            });
          }
        } else if (ev.kind === 'done') {
          writeJson(FrameType.DONE, { id: Number(id), result: ev.result });
        } else {
          writeJson(FrameType.ERROR, { id: Number(id), error: ev.error });
        }
      }
    } catch (err) {
      sendError(
        id,
        'handler_error',
        err instanceof Error ? err.message : String(err),
      );
    } finally {
      inFlight.delete(id);
    }
  }

  function handleCancel(payload: Buffer): void {
    let parsed: { id: number };
    try {
      parsed = JSON.parse(payload.toString('utf8'));
    } catch {
      deps.logger.warn('CANCEL payload not JSON; ignoring');
      return;
    }
    const id = BigInt(parsed.id ?? 0);
    const ac = inFlight.get(id);
    if (ac) {
      ac.abort();
    } else if (id > lastSeenId) {
      // CANCEL arrived ahead of its REQUEST. Remember so handleRequest
      // can pre-abort. Cap the set size defensively: only track ids
      // strictly ahead of lastSeenId (stale cancels are ignored).
      preCancelledIds.add(id);
    }
    // else: id ≤ lastSeenId AND not in flight → call already terminated.
    // Silently drop; idempotent.
  }
}
