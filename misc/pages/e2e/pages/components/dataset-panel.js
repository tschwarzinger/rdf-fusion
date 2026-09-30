// Page-object for the "Select Dataset" step (DatasetSelector).
import { expect } from '@playwright/test';

export class DatasetPanel {
  constructor(page) {
    this.page = page;
  }

  get stepCard() {
    return this.page.getByRole('button', { name: /2\. Select Dataset/ });
  }

  get distributionPicker() {
    return this.page.locator('#distributionPicker');
  }

  get parquetModal() {
    return this.page.locator('#parquetDatasetModal');
  }

  get convertModal() {
    return this.page.locator('#convertRdfModal');
  }

  async openPanel() {
    // The panel spans a card button that toggles it; once it is open (picker
    // visible) we don't re-click, which would collapse it.
    if (!(await this.distributionPicker.isVisible().catch(() => false))) {
      await this.stepCard.click();
    }
    await expect(this.distributionPicker).toBeVisible();
  }

  // --- Add Parquet ----------------------------------------------------------

  // Opens the "Add Dataset" (Parquet) modal.
  async openAddParquetModal() {
    await this.openPanel();
    await this.page.getByRole('button', { name: 'Add Dataset' }).click();
    await expect(this.parquetModal).toBeVisible();
  }

  // Uploads a .parquet RDF dataset and expects it to be accepted + loaded.
  async addParquetDataset(parquetPath, name) {
    await this.openAddParquetModal();
    await this.parquetModal.locator('#parquetDsName').fill(name);
    await this.parquetModal.locator('#parquetDsFile').setInputFiles(parquetPath);
    await this.parquetModal.getByRole('button', { name: 'Save & Select' }).click();
    await expect(this.parquetModal).not.toBeVisible({ timeout: 30_000 });
  }

  // Uploads an arbitrary file (not a valid Parquet dataset). The modal must
  // remain open and show the inline validation/verification error.
  async addInvalidParquet({ name, path = null, buffer }, datasetName = 'invalid') {
    await this.openAddParquetModal();
    await this.parquetModal.locator('#parquetDsName').fill(datasetName);
    if (path) {
      await this.parquetModal.locator('#parquetDsFile').setInputFiles(path);
    } else {
      await this.parquetModal
        .locator('#parquetDsFile')
        .setInputFiles({ name, mimeType: 'application/octet-stream', buffer });
    }
    await this.parquetModal.getByRole('button', { name: 'Save & Select' }).click();

    await expect(this.parquetModal).toBeVisible();
    await expect(this.parquetModal.locator('.alert.alert-danger')).toBeVisible({
      timeout: 30_000,
    });
    return this.parquetModal.locator('.alert.alert-danger');
  }

  // Closes the (still-open) Add Parquet modal after an error.
  async closeParquetModal() {
    await this.parquetModal.getByRole('button', { name: 'Cancel', exact: true }).click();
    await expect(this.parquetModal).not.toBeVisible();
  }

  // --- Convert RDF ----------------------------------------------------------

  // Opens the "Convert Dataset" (RDF -> Parquet) modal.
  async openConvertModal() {
    await this.openPanel();
    await this.page.getByRole('button', { name: 'Convert Dataset' }).click();
    await expect(this.convertModal).toBeVisible();
  }

  // Closes the Convert RDF modal.
  async closeConvertModal() {
    await this.convertModal.locator('.btn-close').click();
    await expect(this.convertModal).not.toBeVisible();
  }

  // Converts an RDF file and selects the resulting Parquet dataset.
  async convertRdfDataset(rdfPath, name, { encoding = 'String', sortOrder = 'GPOS' } = {}) {
    await this.openConvertModal();
    await this.convertModal.locator('#rdfDsFile').setInputFiles(rdfPath);
    await this.convertModal.locator('#rdfDsName').fill(name);
    if (encoding) await this.convertModal.locator('#rdfEncodingSelect').selectOption(encoding);
    if (sortOrder) await this.convertModal.locator('#rdfSortOrderSelect').selectOption(sortOrder);
    await this.convertModal.getByRole('button', { name: 'Convert & Select' }).click();
    await expect(this.convertModal).not.toBeVisible({ timeout: 60_000 });
  }

  // Converts an RDF file and expects an inline error (e.g. unsupported format).
  async convertInvalidRdf({ name, buffer }) {
    await this.openConvertModal();
    await this.convertModal
      .locator('#rdfDsFile')
      .setInputFiles({ name, mimeType: 'text/plain', buffer });
    await this.convertModal.locator('#rdfDsName').fill('invalid');
    await this.convertModal.getByRole('button', { name: 'Convert & Select' }).click();

    await expect(this.convertModal).toBeVisible();
    await expect(this.convertModal.locator('.alert.alert-danger')).toBeVisible({ timeout: 30_000 });
    return this.convertModal.locator('.alert.alert-danger');
  }
}
