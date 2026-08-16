/**
 * @papercusp/ipc-endpoint-server — a host-agnostic Unix-socket/named-pipe
 * server that speaks the length-prefixed endpoint IPC protocol
 * (see PROTOCOL.md) and dispatches projected tools through an injected
 * host seam.
 *
 * The package owns the wire protocol, the per-connection dispatch loop,
 * cancellation/abort semantics, and the privileged `sys:http`
 * HTTP-over-IPC bridge. It does NOT own the tool registry or any
 * application state — resolve/dispatch/workspace are injected via
 * `IpcEndpointHost`. The Papercusp operator binds the host in
 * `apps/operator/lib/endpoint-ipc/server.ts`.
 */

export {
  startEndpointIpcServer,
  defaultSocketPath,
  type IpcEndpointHost,
  type StartEndpointIpcServerOptions,
  type EndpointIpcServer,
} from './server';

export { handleSysHttp, type SysHttpDeps } from './sys-http';
