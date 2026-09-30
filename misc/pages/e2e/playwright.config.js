// Playwright E2E configuration for the RDF Fusion playground.
//
// The webServer builds the frontend (rollup + hugo) and serves it on a local
// port. The default local/CI flow first builds the current WASM bindings with
// `wasm-pack build --target web --dev` and uploads that build as a *custom*
// version through the playground UI, so the E2E exercises the real current
// engine.
import { defineConfig, devices } from '@playwright/test';

const PORT = Number(process.env.E2E_PORT ?? 8089);
const isCI = !!process.env.CI;

export default defineConfig({
  testDir: './tests',
  workers: isCI ? 2 : 1,
  fullyParallel: true,
  forbidOnly: isCI,
  retries: isCI ? 2 : 0,
  timeout: 90_000,
  expect: {
    timeout: 15_000,
    toHaveScreenshot: {
      maxDiffPixelRatio: 0.025,
    },
  },
  reporter: isCI
    ? [['list'], ['html', { outputFolder: 'playwright-report', open: 'never' }]]
    : [['list']],

  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    viewport: { width: 1440, height: 900 },
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },

  // Each project stores its own committed screenshot baselines. The E2E suite
  // targets Firefox (Playwright's bundled build) so the golden screenshots stay
  // stable and reproducible in CI.
  projects: [
    {
      name: 'firefox',
      use: {
        ...devices['Desktop Firefox'],
        viewport: { width: 1440, height: 900 },
        browserName: 'firefox',
        headless: true,
      },
      snapshotPathTemplate:
        '{testDir}/__screenshots__/{projectName}/{testFilePath}/{arg}{ext}',
    },
  ],

  webServer: {
    command: 'bash scripts/serve.sh',
    url: `http://127.0.0.1:${PORT}/playground/`,
    reuseExistingServer: !isCI,
    timeout: 240_000,
    env: { E2E_PORT: String(PORT) },
  },
});
