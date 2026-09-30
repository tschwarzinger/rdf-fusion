// E2E tests for playground lifecycle/reset behaviour: configuring state and
// then wiping it back to the initial state via "Clear All".
import { test, expect } from '../fixtures.js';
import { CUSTOM_WASM_JS, CUSTOM_WASM_BG, SPIDERMAN_PARQUET } from '../constants.mjs';

test.describe('playground lifecycle', () => {
  test('Clear All wipes configured state and returns to the initial screen', async ({
    page,
    playground,
  }) => {
    // Configure engine + dataset so there is state to clear.
    await playground.uploadQueryEngine({ jsPath: CUSTOM_WASM_JS, wasmPath: CUSTOM_WASM_BG });
    await playground.dataset.addParquetDataset(SPIDERMAN_PARQUET, 'spiderman');

    // Sanity: engine + dataset are active (Run is enabled).
    await expect(playground.queryEditor.runButton).toBeEnabled({ timeout: 30_000 });

    await playground.status.clearAllAndConfirm();

    // After the reload no custom version/dataset remains: the "Quick
    // Configure" shortcut for a fresh user reappears and engine is unset.
    await expect(playground.status.quickConfigureButton).toBeVisible({ timeout: 15_000 });
    await expect(playground.engineStepCard).toBeEnabled();
    await expect(playground.datasetStepCard).toBeDisabled();
  });
});
