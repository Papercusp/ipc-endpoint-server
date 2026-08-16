import { defineConfig } from 'vitest/config';

// WI-4973: standalone config — @papercusp/ipc-endpoint-server is a public,
// independently consumable package (github.com/Papercusp/ipc-endpoint-server);
// it must build/test with only its own declared deps, never routing through
// the Papercusp-monorepo private `@papercusp/test-config` harness. Pure node
// socket/IPC tests, no DOM.
export default defineConfig({
  test: {
    environment: 'node',
    exclude: ['node_modules', 'dist'],
    testTimeout: 15_000,
  },
});
