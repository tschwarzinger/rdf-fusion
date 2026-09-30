// Shared paths & app constants for the playground E2E suite.
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url)); // .../misc/pages/e2e

export const PAGES_DIR = resolve(here, '..'); // .../misc/pages
export const REPO_ROOT = resolve(here, '../../..'); // repository root

// WASM pkg produced by `wasm-pack build --target web ... lib/wasm`
export const WASM_PKG_DIR = resolve(REPO_ROOT, 'lib/wasm/pkg');
export const CUSTOM_WASM_JS = resolve(WASM_PKG_DIR, 'rdf_fusion_wasm.js');
export const CUSTOM_WASM_BG = resolve(WASM_PKG_DIR, 'rdf_fusion_wasm_bg.wasm');

// Small local RDF datasets (String-encoded Parquet / raw Turtle) used for hermetic tests.
export const SPIDERMAN_PARQUET = resolve(REPO_ROOT, 'examples/data/spiderman.parquet');
export const PARIS_TTL = resolve(REPO_ROOT, 'examples/data/paris.ttl');

// Route the playground is served at (relative to the test base URL).
export const PLAYGROUND_PATH = '/playground/';

export const CUSTOM_VERSION_NAME = 'E2E Dev Build';

// The default SPARQL query preloaded into the editor.
export const DEFAULT_QUERY = `SELECT *
WHERE {
  ?s ?p ?o .
}
LIMIT 10`;

// ---- Locator keys (single source of truth for the UI contract) ----
export const EDITOR_ID = 'query-editor';
export const RESULTS_TABLE_ID = 'query-results';
export const RESULTS_PANEL_ID = 'results-panel';

export const PLAN_IDS = {
  logical: 'query-plan-logical',
  optimized: 'query-plan-optimized',
  execution: 'query-plan-execution',
};
