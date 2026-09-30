// E2E tests for dataset-upload validation and error handling in the playground.
//
// These exercise the "Add Dataset" (Parquet) and "Convert Dataset" (RDF) modals,
// asserting that invalid inputs produce visible inline errors and are not
// silently accepted, while valid uploads still load.
import { test, expect } from '../fixtures.js';
import { CUSTOM_WASM_JS, CUSTOM_WASM_BG, SPIDERMAN_PARQUET, PARIS_TTL } from '../constants.mjs';

// A tiny helper that configures a fresh engine so dataset validation is real
// (the Parquet verification path only runs when a WASM engine is active).
async function loadCustomEngine(playground) {
  await playground.uploadQueryEngine({ jsPath: CUSTOM_WASM_JS, wasmPath: CUSTOM_WASM_BG });
}

test.describe('dataset validation', () => {
  test('rejects a Parquet upload with a non-.parquet filename', async ({ playground }) => {
    await loadCustomEngine(playground);

    const errorBox = await playground.dataset.addInvalidParquet({
      name: 'metadata.json',
      buffer: Buffer.from('{ "not": "parquet" }'),
    });

    // The inline message mentions that only .parquet files are accepted.
    await expect(errorBox).toContainText(/Only \.parquet files are supported/);
    await expect(errorBox).toContainText(/Convert Dataset/);
  });

  test('rejects a .parquet file whose content is not a valid RDF Parquet dataset', async ({
    playground,
  }) => {
    await loadCustomEngine(playground);

    const errorBox = await playground.dataset.addInvalidParquet({
      name: 'bad.parquet',
      buffer: Buffer.from('this is definitely not a parquet file'),
    });

    await expect(errorBox).toContainText(/not a valid RDF Parquet dataset/);
  });

  test('rejects an RDF conversion whose file format is unsupported', async ({ playground }) => {
    await loadCustomEngine(playground);

    const errorBox = await playground.dataset.convertInvalidRdf({
      name: 'notes.txt',
      buffer: Buffer.from('just some plain text'),
    });

    await expect(errorBox).toContainText(/Unsupported file format/);
  });

  test('a valid Parquet dataset is accepted and queryable after an invalid rejection', async ({
    playground,
  }) => {
    await loadCustomEngine(playground);

    // First trigger a rejection...
    await playground.dataset.addInvalidParquet({
      name: 'bad.parquet',
      buffer: Buffer.from('garbage'),
    });
    await playground.dataset.closeParquetModal();

    // ...then a valid upload succeeds and a query returns rows.
    await playground.dataset.addParquetDataset(SPIDERMAN_PARQUET, 'spiderman');
    await playground.queryEditor.runQuery();
    await playground.results.expectRowsToContain('Spiderman');
  });

  test('converts an uploaded RDF (Turtle) file to Parquet and queries it', async ({
    playground,
  }) => {
    await loadCustomEngine(playground);

    await playground.dataset.convertRdfDataset(PARIS_TTL, 'paris');
    await playground.queryEditor.runQuery();

    // The converted dataset stores Q90 with a label "Paris"@fr.
    await playground.results.expectRowsToContain('Paris');
  });

  test('renders the convert RDF dataset modal with target options', async ({ playground }) => {
    await loadCustomEngine(playground);

    await playground.dataset.openConvertModal();
    await expect(playground.dataset.convertModal.locator('#rdfEncodingSelect')).toBeVisible();

    // Screenshot of the RDF conversion modal with target options
    await expect(playground.dataset.convertModal.locator('.modal-content')).toHaveScreenshot(
      'convert-rdf-modal.png',
    );

    await playground.dataset.closeConvertModal();
  });
});
