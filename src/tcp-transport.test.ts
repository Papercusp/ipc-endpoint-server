/**
 * TCP-loopback transport for the endpoint IPC server (WI-3395).
 *
 * The Windows desktop runs the sidecar inside WSL2, where a Unix socket is
 * unreachable by the Windows-native webview host and a named pipe can't be
 * created — so the endpoint-IPC bypass was silently OFF on Windows and `/api`
 * fell back to the WebView2 HTTP stack under its ~6-connection-per-host cap.
 * The fix is a `tcp://host:port` transport across the WSL boundary (WSL2's
 * localhost-forwarding makes `127.0.0.1:<port>` reachable from the host).
 *
 * These tests prove (a) `parseEndpoint` classifies the discovery string and
 * (b) the FULL wire protocol runs unchanged over a loopback TCP listener,
 * including ephemeral-port resolution reported back in `server.socketPath`
 * (the exact value the client discovers + dials).
 */

import net from 'node:net';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { z } from 'zod';
import {
  registerProjectedTool,
  lookupByMcpName,
  dispatchProjectedToolStream,
  type ProjectedTool,
} from '@papercusp/tooldef';
import {
  FrameType,
  FrameDecoder,
  encodeJsonFrame,
  type Frame,
} from '@papercusp/ipc-framing';
import {
  startEndpointIpcServer,
  parseEndpoint,
  type EndpointIpcServer,
  type IpcEndpointHost,
} from './server';

const host: IpcEndpointHost = {
  lookupByMcpName,
  dispatchProjectedToolStream,
  getWorkspaceId: () => 'test-workspace',
};

const SYNTHETIC_TOOL_NAME = 'ipc_tcp_test:synth';

function readFramesUntil(
  socket: net.Socket,
  predicate: (frames: Frame[]) => boolean,
  timeoutMs = 3000,
): Promise<Frame[]> {
  return new Promise((resolve, reject) => {
    const dec = new FrameDecoder();
    const collected: Frame[] = [];
    const onData = (chunk: Buffer): void => {
      try {
        dec.push(chunk);
      } catch (e) {
        cleanup();
        reject(e);
        return;
      }
      for (const f of dec.drain()) collected.push(f);
      if (predicate(collected)) {
        cleanup();
        resolve(collected);
      }
    };
    const onError = (err: Error): void => {
      cleanup();
      reject(err);
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`readFramesUntil timeout; got ${collected.length} frames`));
    }, timeoutMs);
    function cleanup(): void {
      clearTimeout(timer);
      socket.off('data', onData);
      socket.off('error', onError);
    }
    socket.on('data', onData);
    socket.on('error', onError);
  });
}

/** Connect a raw TCP client to host:port and resolve once connected. */
function connectTcp(hostname: string, port: number): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const sock = net.connect(port, hostname);
    sock.once('connect', () => resolve(sock));
    sock.once('error', reject);
  });
}

describe('parseEndpoint', () => {
  it('classifies a Unix socket path', () => {
    expect(parseEndpoint('/home/u/.papercusp/sockets/42.sock')).toEqual({
      kind: 'unix',
      path: '/home/u/.papercusp/sockets/42.sock',
    });
  });

  it('classifies a Windows named pipe path (either prefix, case-insensitive)', () => {
    expect(parseEndpoint('\\\\.\\pipe\\papercusp-42')).toEqual({
      kind: 'pipe',
      path: '\\\\.\\pipe\\papercusp-42',
    });
    expect(parseEndpoint('\\\\?\\PIPE\\Papercusp-7').kind).toBe('pipe');
  });

  it('classifies a tcp:// endpoint, including the ephemeral :0 form', () => {
    expect(parseEndpoint('tcp://127.0.0.1:54321')).toEqual({
      kind: 'tcp',
      host: '127.0.0.1',
      port: 54321,
    });
    expect(parseEndpoint('tcp://127.0.0.1:0')).toEqual({
      kind: 'tcp',
      host: '127.0.0.1',
      port: 0,
    });
  });

  it('strips the brackets from a bracketed IPv6 tcp literal', () => {
    expect(parseEndpoint('tcp://[::1]:8080')).toEqual({
      kind: 'tcp',
      host: '::1',
      port: 8080,
    });
  });

  it('rejects malformed or case-mismatched tcp lookalikes as Unix paths', () => {
    for (const socketPath of [
      'TCP://127.0.0.1:35745',
      'tcp://127.0.0.1',
      'tcp://127.0.0.1:not-a-port',
      'tcp://127.0.0.1:35745:1',
      'tcp://:1',
      'tcp://[]:1',
    ]) {
      expect(parseEndpoint(socketPath), socketPath).toEqual({
        kind: 'unix',
        path: socketPath,
      });
    }
  });
});

describe('startEndpointIpcServer over TCP loopback', () => {
  let server: EndpointIpcServer;

  beforeAll(async () => {
    const tool: ProjectedTool = {
      pluginName: 'ipc_tcp_test',
      description: 'synthetic tool exercising the IPC wire protocol over TCP',
      inputSchema: { type: 'object' },
      capabilities: [],
      events: { delta: z.object({ text: z.string() }) },
      eventWireKinds: { delta: 'json' },
      timeoutSec: 30,
      expose: { mcp: { name: SYNTHETIC_TOOL_NAME } },
      fn: async (input, ctx) => {
        const args = input as { text?: string };
        const text = args.text ?? 'hi';
        ctx.emit?.('delta', { text: `${text} ` });
        ctx.emit?.('delta', { text: 'world' });
        return { content: [{ type: 'text', text: `${text} world` }] };
      },
    };
    try {
      registerProjectedTool(tool);
    } catch (e) {
      if (!(e instanceof Error) || !/already registered/i.test(e.message)) throw e;
    }

    server = await startEndpointIpcServer({
      // Ephemeral port — the server must resolve + report the real one.
      socketPath: 'tcp://127.0.0.1:0',
      host,
      allowedTools: [SYNTHETIC_TOOL_NAME],
      logger: { info: () => {}, warn: () => {} },
    });
  });

  afterAll(async () => {
    await server.close();
  });

  it('resolves the ephemeral port and reports it in socketPath', () => {
    const parsed = parseEndpoint(server.socketPath);
    expect(parsed.kind).toBe('tcp');
    if (parsed.kind !== 'tcp') throw new Error('unreachable');
    expect(parsed.host).toBe('127.0.0.1');
    // Port 0 must have been replaced by the real OS-assigned port.
    expect(parsed.port).toBeGreaterThan(0);
    expect(parsed.port).toBe((server.server.address() as net.AddressInfo).port);
  });

  it('runs the full REQUEST → 2 EVENT_JSON → DONE round-trip over TCP', async () => {
    const { host: h, port } = parseEndpoint(server.socketPath) as {
      kind: 'tcp';
      host: string;
      port: number;
    };
    const sock = await connectTcp(h, port);
    try {
      sock.write(
        encodeJsonFrame(FrameType.REQUEST, {
          id: 1,
          toolName: SYNTHETIC_TOOL_NAME,
          input: { text: 'hello' },
        }),
      );
      const frames = await readFramesUntil(
        sock,
        (f) => f.some((x) => x.type === FrameType.DONE),
      );
      const events = frames.filter((f) => f.type === FrameType.EVENT_JSON);
      expect(events).toHaveLength(2);
      expect(events.map((f) => JSON.parse(f.payload.toString()))).toEqual([
        { id: 1, name: 'delta', data: { text: 'hello ' } },
        { id: 1, name: 'delta', data: { text: 'world' } },
      ]);
      expect(frames.some((f) => f.type === FrameType.DONE)).toBe(true);
    } finally {
      sock.destroy();
    }
  });
});
