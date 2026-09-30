// Root page-object for the RDF Fusion playground.
//
// Composes hierarchical sub-page objects so tests can address parts of the UI
// semantically (e.g. `playground.queryEditor.setQuery(...)` instead of reaching
// for DOM ids). Keeps backward-compatible convenience methods that existing
// specs rely on.
import { expect } from '@playwright/test';
import { EnginePanel } from './components/engine-panel.js';
import { DatasetPanel } from './components/dataset-panel.js';
import { QueryEditor } from './components/query-editor.js';
import { ResultsPanel } from './components/results-panel.js';
import { QueryBrowser } from './components/query-browser.js';
import { StatusCard } from './components/status-card.js';

export class PlaygroundPage {
  constructor(page) {
    this.page = page;

    // Hierarchical sub-page objects
    this.engine = new EnginePanel(page);
    this.dataset = new DatasetPanel(page);
    this.queryEditor = new QueryEditor(page);
    this.results = new ResultsPanel(page);
    this.queryBrowser = new QueryBrowser(page);
    this.status = new StatusCard(page);
  }

  // --- Backward-compatible locators -----------------------------------------

  get engineStepCard() {
    return this.engine.stepCard;
  }

  get datasetStepCard() {
    return this.dataset.stepCard;
  }

  get runButton() {
    return this.queryEditor.runButton;
  }

  get queryResultsRows() {
    return this.results.rows;
  }

  get engineApplyButton() {
    return this.engine.applyButton;
  }

  // --- Lifecycle ------------------------------------------------------------

  async open() {
    await this.page.goto('/playground/');
    await expect(this.engineStepCard).toBeVisible({ timeout: 15_000 });
  }

  async waitForFonts() {
    await this.page.evaluate(() => document.fonts && document.fonts.ready);
  }

  // --- Engine (backward-compatible delegations) ------------------------------

  async openEnginePanel() {
    return this.engine.openPanel();
  }

  async uploadQueryEngine(opts) {
    return this.engine.uploadQueryEngine(opts);
  }

  // --- Dataset (backward-compatible delegations) -----------------------------

  async openDatasetPanel() {
    return this.dataset.openPanel();
  }

  async addParquetDataset(path, name) {
    return this.dataset.addParquetDataset(path, name);
  }

  // --- Query (backward-compatible delegations) -------------------------------

  async runQuery() {
    return this.queryEditor.runQuery();
  }

  async switchToPlanTab(label) {
    return this.results.switchToPlanTab(label);
  }

  async expectResultsToContain(text) {
    return this.results.expectRowsToContain(text);
  }

  async expectResultRowCount(count) {
    return this.results.expectRowCount(count);
  }
}
