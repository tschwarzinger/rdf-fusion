// End-to-end tests for the RDF Fusion playground against the *current* WASM
// build, uploaded through the UI as a custom version (hermetic: no object-store
// network access required).
import { test, expect } from '../fixtures.js';
import { CUSTOM_WASM_JS, CUSTOM_WASM_BG, SPIDERMAN_PARQUET } from '../constants.mjs';

test.describe('playground (current build)', () => {
  test('renders the playground shell', async ({ page, playground }) => {
    await playground.waitForFonts();

    await expect(page.getByRole('heading', { name: 'Query' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Results' })).toBeVisible();
    await expect(playground.runButton).toBeVisible();

    // The step pipeline should be present.
    await expect(page.getByRole('button', { name: /1\. Select Engine Version/ })).toBeVisible();
    await expect(page.getByRole('button', { name: /2\. Select Dataset/ })).toBeVisible();
    await expect(page.getByText(/3\. Create RDF Fusion Instance/)).toBeVisible();

    await expect(page).toHaveScreenshot('playground-shell.png');
  });

  test('uploads the current WASM build, runs a query and inspects the plans', async ({ playground }) => {
    const pageErrors = [];
    playground.page.on('pageerror', (err) => pageErrors.push(err));

    await playground.uploadQueryEngine({
      jsPath: CUSTOM_WASM_JS,
      wasmPath: CUSTOM_WASM_BG,
    });

    await playground.addParquetDataset(SPIDERMAN_PARQUET, 'spiderman');

    // Run the default query and assert the rendered results table.
    await playground.runQuery();
    await playground.expectResultsToContain('Spiderman');
    await playground.expectResultRowCount(7);

    // Screenshot the rendered data table. Scoped to #query-results (no live
    // timing text), so it is deterministic.
    await expect(playground.page.locator('#query-results')).toHaveScreenshot('playground-results.png');

    // Inspect the logical plan (exercises query explain).
    const logicalPre = await playground.switchToPlanTab('Logical Plan');
    await expect(logicalPre).not.toHaveText('');
    await expect(logicalPre).toContainText(/QuadPattern|Projection|TableScan|Scan|EmptyRelation/);

    expect(pageErrors).toEqual([]);
  });

  test('persists the custom engine and dataset across a reload (IndexedDB)', async ({ playground }) => {
    await playground.uploadQueryEngine({
      jsPath: CUSTOM_WASM_JS,
      wasmPath: CUSTOM_WASM_BG,
      name: 'Persisted Dev Build',
    });

    await playground.addParquetDataset(SPIDERMAN_PARQUET, 'spiderman');

    // The selected engine is recorded and the blobs are stored in IndexedDB.
    await playground.runQuery();
    await playground.expectResultsToContain('Spiderman');

    // Reload in the same context: the app should auto-load the saved version
    // and dataset purely from local storage / IndexedDB (no re-upload).
    await playground.page.reload();

    await expect(playground.engineStepCard).toBeVisible();

    // The engine + dataset are restored from IndexedDB: the Run button (already
    // enabled) proves the store re-created from persisted blobs.
    await expect(playground.runButton).toBeEnabled({ timeout: 60_000 });
    await playground.runQuery();
    await playground.expectResultsToContain('Spiderman');
  });

  test('configures engine settings and runs queries with multiple target partitions', async ({
    playground,
  }) => {
    await playground.uploadQueryEngine({
      jsPath: CUSTOM_WASM_JS,
      wasmPath: CUSTOM_WASM_BG,
    });

    // Expand DataFusion options
    await playground.engine.openDataFusionSettings();
    await expect(playground.engine.targetPartitionsInput).toBeVisible();

    // Screenshot of the engine configuration panel
    await expect(playground.engine.panel).toHaveScreenshot('engine-config-panel.png');

    // Set target partitions to 4 and apply
    await playground.engine.setTargetPartitions(4);
    await playground.engine.applyButton.click();
    await playground.engine.waitForEngineReady();

    // Add dataset and execute query across 4 partitions
    await playground.addParquetDataset(SPIDERMAN_PARQUET, 'spiderman');
    await playground.runQuery();
    await playground.expectResultsToContain('Spiderman');
    await playground.expectResultRowCount(7);

    // Verify execution plan has been generated
    const execPlan = await playground.switchToPlanTab('Execution Plan');
    await expect(execPlan).not.toHaveText('');
  });
});
