// Page-object for the query "Results" pane.
//
// Encapsulates assertions about the rendered data table, the ASK/boolean result,
// empty results, and query errors, plus the plan tabs.
import { expect } from '@playwright/test';
import { RESULTS_PANEL_ID, RESULTS_TABLE_ID, PLAN_IDS } from '../../constants.mjs';

export class ResultsPanel {
  constructor(page) {
    this.page = page;
  }

  get panel() {
    return this.page.locator(`#${RESULTS_PANEL_ID}`);
  }

  get table() {
    return this.page.locator(`#${RESULTS_TABLE_ID}`);
  }

  get rows() {
    return this.table.locator('tbody tr');
  }

  // The error <div.alert> shown inside the results pane after a failed query.
  get errorAlert() {
    return this.panel.locator('.alert.alert-danger');
  }

  // Waits for the data table to render (i.e. a successful SELECT/CONSTRUCT).
  async expectTableVisible() {
    await expect(this.table).toBeVisible({ timeout: 30_000 });
  }

  async expectRowsToContain(text) {
    await expect(this.table.locator('tbody')).toContainText(text, { timeout: 30_000 });
  }

  async expectRowCount(count) {
    await expect(this.rows).toHaveCount(count, { timeout: 30_000 });
  }

  // Asserts the results pane shows a query error (alert-danger) with `text`.
  async expectError(text) {
    await expect(this.errorAlert).toBeVisible({ timeout: 30_000 });
    if (text) await expect(this.errorAlert).toContainText(text);
  }

  // Asserts an ASK query rendered its boolean result.
  async expectBooleanResult(expected) {
    await expect(this.panel).toContainText('ASK Query Result', { timeout: 30_000 });
    const badge = expected ? 'TRUE' : 'FALSE';
    await expect(this.panel.locator('.badge', { hasText: badge })).toBeVisible();
  }

  // Asserts the "Query returned 0 rows." empty-state message.
  async expectEmpty() {
    await expect(this.panel).toContainText('Query returned 0 rows.', { timeout: 30_000 });
  }

  // Switches to a plan tab and returns its <pre> element.
  async switchToPlanTab(label) {
    await this.page.getByRole('button', { name: label, exact: true }).click();
    const id =
      label === 'Logical Plan'
        ? PLAN_IDS.logical
        : label === 'Optimized Plan'
          ? PLAN_IDS.optimized
          : PLAN_IDS.execution;
    return this.page.locator(`#${id}`);
  }
}
