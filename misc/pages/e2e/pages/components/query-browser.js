// Page-object for the "Example Queries" browser modal (QueryBrowser).
import { expect } from '@playwright/test';

export class QueryBrowser {
  constructor(page) {
    this.page = page;
  }

  get modal() {
    return this.page.locator('#queryBrowserModal');
  }

  get openButton() {
    return this.page.getByRole('button', { name: 'Load Example Query' });
  }

  get modalContent() {
    return this.modal.locator('.modal-content');
  }

  get previewEditor() {
    return this.modal.locator('#example-query-preview .cm-content');
  }

  // The dropdown toggle showing the currently-selected example query's name.
  get dropdownToggle() {
    return this.modal.locator('.dropdown button').first();
  }

  // Opens the example queries modal.
  async open() {
    await this.openButton.click();
    await expect(this.modal).toBeVisible();
    await expect(this.previewEditor).toBeVisible();
  }

  async selectByName(queryName) {
    await this.dropdownToggle.click();
    await this.modal.getByRole('option', { name: queryName, exact: true }).click();
  }

  async useQuery() {
    await this.modal.getByRole('button', { name: 'Use Query', exact: true }).click();
    await expect(this.modal).not.toBeVisible();
  }

  // Selects an example query by exact display name and clicks "Use Query".
  async loadByName(queryName) {
    await this.open();
    await this.selectByName(queryName);
    await this.useQuery();
  }
}
