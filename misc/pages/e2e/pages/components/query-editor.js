// Page-object for the CodeMirror SPARQL query editor.
//
// Exposes semantic actions ("set the query to ...", "run it") instead of
// reaching into CodeMirror DOM selectors from every test.
import { expect } from '@playwright/test';
import { EDITOR_ID } from '../../constants.mjs';

export class QueryEditor {
  constructor(page) {
    this.page = page;
  }

  // The CodeMirror content region (contenteditable).
  get content() {
    return this.page.locator(`#${EDITOR_ID} .cm-content`);
  }

  get runButton() {
    return this.page.getByRole('button', { name: 'Run', exact: true });
  }

  get cancelButton() {
    return this.page.getByRole('button', { name: 'Cancel', exact: true });
  }

  // Reads back the current query text from the CodeMirror DOM.
  async getQuery() {
    return this.content.innerText();
  }

  // Replaces the entire editor content with `text` (select-all + insert).
  async setQuery(text) {
    await this.content.click();
    await this.page.keyboard.press('ControlOrMeta+A');
    await this.page.keyboard.insertText(text);
    await expect.poll(() => this.getQuery().then((q) => q.replace(/\s+/g, ' ').trim())).toBe(
      text.replace(/\s+/g, ' ').trim(),
    );
  }

  // Appends `text` at the current cursor position (no clear).
  async typeQuery(text) {
    await this.content.click();
    await this.page.keyboard.insertText(text);
  }

  // Clicks "Run" once the engine is ready; does not wait for results here, so
  // callers can assert either a data table or an error afterwards.
  async run() {
    await expect(this.runButton).toBeEnabled({ timeout: 30_000 });
    await this.runButton.click();
  }

  // Clicks "Run" and waits for the results table (or error state) to settle.
  async runQuery() {
    await this.run();
    await expect(this.page.locator('#results-panel')).not.toContainText('Executing...', {
      timeout: 30_000,
    });
    await expect(this.runButton).toBeEnabled({ timeout: 30_000 });
  }
}
