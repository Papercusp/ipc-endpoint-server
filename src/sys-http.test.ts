import { describe, expect, it } from 'vitest';
import { FrameType, decodeEventBinPayload, type FrameTypeValue } from '@papercusp/ipc-framing';
import { handleSysHttp, type SysHttpDeps } from './sys-http';

interface Captured {
  type: FrameTypeValue;
  payload: unknown;
}

function makeDeps(fetchImpl: typeof fetch): { deps: SysHttpDeps; emitted: Captured[]; bin: Buffer[] } {
  const emitted: Captured[] = [];
  const bin: Buffer[] = [];
  const deps: SysHttpDeps = {
    upstreamBase: 'http://test.local',
    fetchImpl,
    writeFrame: (type, payload) => {
      if (type === FrameType.EVENT_BIN) bin.push(payload);
      else emitted.push({ type, payload });
      return true;
    },
    writeJson: (type, value) => {
      emitted.push({ type, payload: value });
      return true;
    },
    logger: { info: () => {}, warn: () => {} },
  };
  return { deps, emitted, bin };
}

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json; charset=utf-8' },
    ...init,
  });
}

function sseResponse(chunks: string[]): Response {
  const stream = new ReadableStream({
    async start(controller) {
      const enc = new TextEncoder();
      for (const c of chunks) controller.enqueue(enc.encode(c));
      controller.close();
    },
  });
  return new Response(stream, {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  });
}

describe('handleSysHttp — input validation', () => {
  it('rejects non-object input', async () => {
    const { deps, emitted } = makeDeps(async () => jsonResponse({}));
    await handleSysHttp(1n, 'not-an-object', new AbortController().signal, deps);
    expect(emitted).toHaveLength(1);
    expect(emitted[0].type).toBe(FrameType.ERROR);
    expect((emitted[0].payload as any).error.code).toBe('bad_input');
  });

  it('rejects missing method/path', async () => {
    const { deps, emitted } = makeDeps(async () => jsonResponse({}));
    await handleSysHttp(1n, { method: 'GET' }, new AbortController().signal, deps);
    expect(emitted[0].type).toBe(FrameType.ERROR);
    expect((emitted[0].payload as any).error.code).toBe('bad_input');
  });

  it('rejects absolute-URL path (would be an open proxy)', async () => {
    const { deps, emitted } = makeDeps(async () => jsonResponse({}));
    await handleSysHttp(1n, { method: 'GET', path: 'https://evil.example/' }, new AbortController().signal, deps);
    expect(emitted[0].type).toBe(FrameType.ERROR);
    expect((emitted[0].payload as any).error.code).toBe('bad_path');
  });

  it('rejects parent-dir traversal', async () => {
    const { deps, emitted } = makeDeps(async () => jsonResponse({}));
    await handleSysHttp(1n, { method: 'GET', path: '/api/../etc/passwd' }, new AbortController().signal, deps);
    expect(emitted[0].type).toBe(FrameType.ERROR);
    expect((emitted[0].payload as any).error.code).toBe('bad_path');
  });

  it('rejects relative paths that do not start with /', async () => {
    const { deps, emitted } = makeDeps(async () => jsonResponse({}));
    await handleSysHttp(1n, { method: 'GET', path: 'api/foo' }, new AbortController().signal, deps);
    expect(emitted[0].type).toBe(FrameType.ERROR);
    expect((emitted[0].payload as any).error.code).toBe('bad_path');
  });
});

describe('handleSysHttp — JSON GET (non-SSE)', () => {
  it('emits head then body EVENT_BIN then DONE', async () => {
    const { deps, emitted, bin } = makeDeps(async (url) => {
      expect(url).toBe('http://test.local/api/foo');
      return jsonResponse({ ok: true, n: 42 });
    });

    await handleSysHttp(7n, { method: 'GET', path: '/api/foo' }, new AbortController().signal, deps);

    expect(emitted[0].type).toBe(FrameType.EVENT_JSON);
    expect((emitted[0].payload as any).name).toBe('head');
    expect((emitted[0].payload as any).data.status).toBe(200);
    expect((emitted[0].payload as any).data.headers['content-type']).toMatch(/application\/json/);

    expect(bin.length).toBeGreaterThan(0);
    const decoded = decodeEventBinPayload(bin[0]);
    expect(decoded.id).toBe(7n);
    expect(decoded.name).toBe('body');
    expect(JSON.parse(decoded.binary.toString('utf8'))).toEqual({ ok: true, n: 42 });

    const last = emitted[emitted.length - 1];
    expect(last.type).toBe(FrameType.DONE);
    expect((last.payload as any).id).toBe(7);
  });

  it('forwards method, headers, and body to upstream; filters hop-by-hop headers', async () => {
    const seen: { method?: string; headers?: Headers; body?: string } = {};
    const { deps } = makeDeps(async (url, init) => {
      seen.method = init?.method;
      seen.headers = new Headers(init?.headers as HeadersInit);
      if (typeof init?.body === 'string') seen.body = init.body;
      return jsonResponse({ ok: true });
    });

    await handleSysHttp(
      1n,
      {
        method: 'POST',
        path: '/api/echo',
        headers: {
          'content-type': 'application/json',
          authorization: 'Bearer xyz',
          'connection': 'close', // hop-by-hop — must be stripped
          'transfer-encoding': 'chunked', // hop-by-hop — must be stripped
        },
        body: '{"hello":"world"}',
      },
      new AbortController().signal,
      deps,
    );

    expect(seen.method).toBe('POST');
    expect(seen.headers!.get('content-type')).toBe('application/json');
    expect(seen.headers!.get('authorization')).toBe('Bearer xyz');
    expect(seen.headers!.get('connection')).toBeNull();
    expect(seen.headers!.get('transfer-encoding')).toBeNull();
    expect(seen.body).toBe('{"hello":"world"}');
  });

  it('strips hop-by-hop headers from upstream response too', async () => {
    const { deps, emitted } = makeDeps(async () => {
      return new Response('hi', {
        status: 200,
        headers: {
          'content-type': 'text/plain',
          'connection': 'close',
          'keep-alive': 'timeout=5',
          'transfer-encoding': 'chunked',
        },
      });
    });
    await handleSysHttp(1n, { method: 'GET', path: '/api/x' }, new AbortController().signal, deps);
    const head = emitted.find((e) => e.type === FrameType.EVENT_JSON && (e.payload as any).name === 'head');
    expect(head).toBeDefined();
    const headers = (head!.payload as any).data.headers as Record<string, string>;
    expect(headers['content-type']).toBeDefined();
    expect(headers['connection']).toBeUndefined();
    expect(headers['keep-alive']).toBeUndefined();
    expect(headers['transfer-encoding']).toBeUndefined();
  });
});

describe('handleSysHttp — host-injected headers (in-boundary credential)', () => {
  it('injects host headers upstream, overriding a forwarded same-name header', async () => {
    const seen: { headers?: Headers } = {};
    const { deps } = makeDeps(async (_url, init) => {
      seen.headers = new Headers(init?.headers as HeadersInit);
      return jsonResponse({ ok: true });
    });
    // The host supplies the in-boundary credential; a webview-forwarded
    // `authorization` must NOT be able to shadow it.
    deps.injectHeaders = () => ({ authorization: 'Bearer host-token', 'x-extra': '1' });

    await handleSysHttp(
      1n,
      {
        method: 'GET',
        path: '/api/admin/plans/get',
        headers: { authorization: 'Bearer webview-supplied' },
      },
      new AbortController().signal,
      deps,
    );

    expect(seen.headers!.get('authorization')).toBe('Bearer host-token');
    expect(seen.headers!.get('x-extra')).toBe('1');
  });

  it('drops hop-by-hop names from injected headers and tolerates an undefined return', async () => {
    const seen: { headers?: Headers } = {};
    const { deps } = makeDeps(async (_url, init) => {
      seen.headers = new Headers(init?.headers as HeadersInit);
      return jsonResponse({ ok: true });
    });
    deps.injectHeaders = () => ({ authorization: 'Bearer t', connection: 'close' });
    await handleSysHttp(1n, { method: 'GET', path: '/api/x' }, new AbortController().signal, deps);
    expect(seen.headers!.get('authorization')).toBe('Bearer t');
    expect(seen.headers!.get('connection')).toBeNull();

    // A thunk that returns undefined → no injection, no throw.
    const seen2: { headers?: Headers } = {};
    const { deps: deps2 } = makeDeps(async (_url, init) => {
      seen2.headers = new Headers(init?.headers as HeadersInit);
      return jsonResponse({ ok: true });
    });
    deps2.injectHeaders = () => undefined;
    await handleSysHttp(1n, { method: 'GET', path: '/api/y' }, new AbortController().signal, deps2);
    expect(seen2.headers!.get('authorization')).toBeNull();
  });
});

describe('handleSysHttp — SSE streaming', () => {
  it('emits head then sse-chunk events with raw wire text, then DONE on stream end', async () => {
    const { deps, emitted } = makeDeps(async () =>
      sseResponse(['event: foo\ndata: 1\n\n', 'event: bar\ndata: 2\n\n']),
    );

    await handleSysHttp(3n, { method: 'GET', path: '/api/stream' }, new AbortController().signal, deps);

    const head = emitted[0].payload as any;
    expect(emitted[0].type).toBe(FrameType.EVENT_JSON);
    expect(head.name).toBe('head');
    expect(head.data.headers['content-type']).toMatch(/text\/event-stream/);

    const chunks = emitted.filter(
      (e) => e.type === FrameType.EVENT_JSON && (e.payload as any).name === 'sse-chunk',
    );
    expect(chunks).toHaveLength(2);
    expect((chunks[0].payload as any).data).toBe('event: foo\ndata: 1\n\n');
    expect((chunks[1].payload as any).data).toBe('event: bar\ndata: 2\n\n');

    const last = emitted[emitted.length - 1];
    expect(last.type).toBe(FrameType.DONE);
  });
});

describe('handleSysHttp — abort + upstream errors', () => {
  it('emits ERROR{code:aborted} when the AbortSignal fires before fetch resolves', async () => {
    const ac = new AbortController();
    const { deps, emitted } = makeDeps(async (_url, init) => {
      // Honor the passed signal — abort propagates as a fetch rejection.
      return await new Promise<Response>((_, reject) => {
        init?.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
      });
    });

    const p = handleSysHttp(9n, { method: 'GET', path: '/api/slow' }, ac.signal, deps);
    ac.abort();
    await p;

    const err = emitted.find((e) => e.type === FrameType.ERROR);
    expect(err).toBeDefined();
    expect((err!.payload as any).error.code).toBe('aborted');
  });

  it('emits ERROR{code:upstream_error} when fetch rejects (not aborted)', async () => {
    const { deps, emitted } = makeDeps(async () => {
      throw new Error('econnrefused');
    });
    await handleSysHttp(2n, { method: 'GET', path: '/api/x' }, new AbortController().signal, deps);
    const err = emitted.find((e) => e.type === FrameType.ERROR);
    expect(err).toBeDefined();
    expect((err!.payload as any).error.code).toBe('upstream_error');
    expect((err!.payload as any).error.message).toMatch(/econnrefused/);
  });
});
