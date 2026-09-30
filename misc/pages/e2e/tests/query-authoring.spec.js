// E2E tests for query authoring: custom query input, errors, ASK results,
// empty results, and loading example queries from the query browser.
import { test, expect } from '../fixtures.js';
import { CUSTOM_WASM_JS, CUSTOM_WASM_BG, SPIDERMAN_PARQUET } from '../constants.mjs';

// Configures the current WASM engine + the spiderman dataset once per test.
async function loadSpiderman(playground) {
  await playground.uploadQueryEngine({ jsPath: CUSTOM_WASM_JS, wasmPath: CUSTOM_WASM_BG });
  await playground.dataset.addParquetDataset(SPIDERMAN_PARQUET, 'spiderman');
}

test.describe('query authoring', () => {
  test('runs a user-authored query and shows the results', async ({ playground }) => {
    await loadSpiderman(playground);

    await playground.queryEditor.setQuery(`SELECT ?name
WHERE {
  <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> ?name .
}`);
    await playground.queryEditor.runQuery();

    await playground.results.expectRowsToContain('Spiderman');
    // Spiderman has two foaf:name values (plain + Russian-language).
    await playground.results.expectRowCount(2);
  });

  test('surfaces a query error for invalid SPARQL without crashing the engine', async ({
    playground,
  }) => {
    await loadSpiderman(playground);

    await playground.queryEditor.setQuery('SELECT * WHERE { ?s ?p }');
    await playground.queryEditor.run();

    // The results pane shows an error alert describing the failure.
    await playground.results.expectError(/Error executing query/);

    // The engine must still be alive: a subsequent valid query runs.
    await playground.queryEditor.setQuery(`SELECT * WHERE { ?s ?p ?o . } LIMIT 3`);
    await playground.queryEditor.runQuery();
    await playground.results.expectRowCount(3);
  });

  test('renders an ASK query boolean result', async ({ playground }) => {
    await loadSpiderman(playground);

    await playground.queryEditor.setQuery('ASK { <http://example.org/#spiderman> ?p ?o }');
    await playground.queryEditor.runQuery();
    await playground.results.expectBooleanResult(true);

    // Visual screenshot of the ASK result badge
    await expect(playground.page.locator('#ask-result-card')).toHaveScreenshot('ask-result-badge.png');

    await playground.queryEditor.setQuery('ASK { <http://example.org/#does-not-exist> ?p ?o }');
    await playground.queryEditor.runQuery();
    await playground.results.expectBooleanResult(false);
  });

  test('shows the empty-results state when a query matches nothing', async ({ playground }) => {
    await loadSpiderman(playground);

    await playground.queryEditor.setQuery(`SELECT ?s
WHERE {
  ?s <http://example.org/nonexistent/predicate> ?o .
}`);
    await playground.queryEditor.runQuery();

    await playground.results.expectEmpty();
  });

  test('loads an example query from the query browser and runs it', async ({ playground }) => {
    await loadSpiderman(playground);

    await playground.queryBrowser.open();
    await expect(playground.queryBrowser.previewEditor).toBeVisible();

    // Screenshot of the example queries modal with syntax-highlighted preview
    await expect(playground.queryBrowser.modalContent).toHaveScreenshot('example-queries-modal.png');

    // Switch to another query and verify reactive preview update
    await playground.queryBrowser.selectByName('Count Triples');
    await expect(playground.queryBrowser.previewEditor).toContainText('COUNT');

    await playground.queryBrowser.useQuery();

    // The editor now contains the example query.
    const queryText = await playground.queryEditor.getQuery();
    await expect(queryText).toContain('COUNT');

    await playground.queryEditor.runQuery();
    // Count returns a single aggregate row.
    await playground.results.expectRowCount(1);
  });
});
