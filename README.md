# @papercusp/ipc-endpoint-server

A host-agnostic Unix-socket / named-pipe server that speaks the
length-prefixed **endpoint IPC protocol** (wire spec:
`apps/operator/lib/endpoint-ipc/PROTOCOL.md`) and dispatches projected
tools through an injected host seam.

It is the webview→Tauri→Node fast path for the endpoint system: the
Rust client connects over the socket, sends framed `REQUEST` / `CANCEL`
frames, and receives `EVENT_JSON` / `EVENT_BIN` / `DONE` / `ERROR`
frames back. The package also implements the privileged `sys:http`
HTTP-over-IPC bridge so the webview's `fetch` / `EventSource` polyfills
ride the same socket instead of opening their own HTTP connections.

## What it owns

- The wire protocol (framing via `@papercusp/ipc-framing`).
- The per-connection dispatch loop, monotonic-id enforcement, and the
  CANCEL-before-REQUEST pre-abort race handling.
- The `sys:http` bridge (`handleSysHttp`).

## What it does NOT own — the host seam

Tool resolution, tool dispatch, and the active workspace id are
injected via `IpcEndpointHost`:

```ts
import { startEndpointIpcServer } from '@papercusp/ipc-endpoint-server';
import {
  lookupByMcpName,
  dispatchProjectedToolStream,
} from '@papercusp/tooldef'; // or your host's re-export

const server = await startEndpointIpcServer({
  socketPath,
  allowedTools: ['operator:scan', 'sys:http'],
  deps: PROJECTED_DEPS,
  host: {
    lookupByMcpName,
    dispatchProjectedToolStream,
    getWorkspaceId: () => activeWorkspaceId(),
  },
});
```

Because the caller passes in its **own** `lookupByMcpName` +
`dispatchProjectedToolStream`, the server always reads/writes the same
projected-tool registry singleton the host uses — there is no
package-level import of the registry, so a second module instance can't
sneak in.

The Papercusp operator binds the host in
`apps/operator/lib/endpoint-ipc/server.ts`.

## Extraction status

Extracted per `papercusp-systems-abstraction-2026-05-29`, item P-030
(the third and final IPC piece, after `@papercusp/ipc-framing` and
`@papercusp/desktop-ipc`). The Rust client in `papercusp-desktop`
mirrors the wire codec byte-for-byte — the protocol is unchanged by the
move.
