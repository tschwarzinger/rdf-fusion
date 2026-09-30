// Page-object for the "Select Engine Version" step (VersionManager).
import { expect } from '@playwright/test';
import { CUSTOM_VERSION_NAME } from '../../constants.mjs';

export class EnginePanel {
  constructor(page) {
    this.page = page;
  }

  get stepCard() {
    return this.page.getByRole('button', { name: /1\. Select Engine Version/ });
  }

  get applyButton() {
    return this.page.getByRole('button', { name: /^(Download & )?Apply$/ });
  }

  get versionDropdown() {
    return this.page.locator('#versionSelectDropdown');
  }

  get panel() {
    return this.page.locator('#engine-config-panel');
  }

  get dataFusionCollapse() {
    return this.page.locator('#settingsDataFusionCollapse');
  }

  get dataFusionHeader() {
    return this.page.locator('[data-bs-target="#settingsDataFusionCollapse"]');
  }

  get targetPartitionsInput() {
    return this.page.locator('#engine-partitions');
  }

  // Returns the engine apply button (kept for compatibility with older tests).
  async openPanel() {
    if (!(await this.versionDropdown.isVisible().catch(() => false))) {
      await this.stepCard.click();
    }
    await expect(this.versionDropdown).toBeVisible();
  }

  async openDataFusionSettings() {
    await this.openPanel();
    if (!(await this.dataFusionCollapse.isVisible().catch(() => false))) {
      await this.dataFusionHeader.click();
      await expect(this.dataFusionCollapse).toBeVisible();
    }
  }

  async setTargetPartitions(count) {
    await this.openDataFusionSettings();
    await this.targetPartitionsInput.fill(String(count));
  }

  // Uploads a locally built WASM engine and activates it.
  async uploadQueryEngine({ name = CUSTOM_VERSION_NAME, jsPath, wasmPath }) {
    await this.openPanel();
    await this.page.getByRole('button', { name: 'Upload Build' }).click();
    const modal = this.page.locator('#customEngineModal');
    await expect(modal).toBeVisible();

    await modal.locator('#customVersionName').fill(name);
    await modal.locator('#customJsFile').setInputFiles(jsPath);
    await modal.locator('#customWasmFile').setInputFiles(wasmPath);
    await modal.getByRole('button', { name: 'Upload & Select' }).click();

    await expect(modal).not.toBeVisible({ timeout: 20_000 });

    await this.applyButton.click();

    // The engine is ready once the dataset step card becomes enabled.
    await this.waitForEngineReady();
  }

  async waitForEngineReady(options = {}) {
    const timeout = options.timeout ?? 30_000;
    await expect(this.page.getByRole('button', { name: /2\. Select Dataset/ })).toBeEnabled({
      timeout,
    });
  }
}
