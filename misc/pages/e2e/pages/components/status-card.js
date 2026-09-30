// Page-object for the status card: the 3-step guidance, quick configure,
// global alerts, and the "Manage Local Data" / "Clear All" actions.
import { expect } from '@playwright/test';

export class StatusCard {
  constructor(page) {
    this.page = page;
  }

  get quickConfigureButton() {
    return this.page.getByRole('button', { name: 'Quick Configure' });
  }

  // The global error/warning alert banner rendered at the top of the playground.
  get globalAlert() {
    return this.page.locator('.alert.alert-danger, .alert.alert-warning').first();
  }

  get clearAllButton() {
    return this.page.getByRole('button', { name: 'Clear All' });
  }

  get clearAllModal() {
    return this.page.locator('#clearAllModal');
  }

  // Waits for the quick-configure shortcut to be offered (fresh, unconfigured state).
  async expectQuickConfigureVisible() {
    await expect(this.quickConfigureButton).toBeVisible({ timeout: 15_000 });
  }

  // Clicks "Clear All", confirms the destructive dialog, and waits for reload.
  async clearAllAndConfirm() {
    await this.clearAllButton.click();
    await expect(this.clearAllModal).toBeVisible();
    await this.clearAllModal
      .getByRole('button', { name: 'Clear Everything & Reload' })
      .click();
    await this.page.waitForLoadState('load');
  }
}
