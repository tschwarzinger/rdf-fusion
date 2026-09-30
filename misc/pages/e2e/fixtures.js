// Playwright fixtures wrapping the playground page object, so tests can write:
//
//   test('...', async ({ playground }) => {
//     await playground.uploadQueryEngine({ jsPath, wasmPath });
//     await playground.addParquetDataset(path, 'name');
//     await playground.runQuery();
//   });
import { test as base } from '@playwright/test';
import { PlaygroundPage } from './pages/playground.js';

export const test = base.extend({
  playground: async ({ page }, use) => {
    const playground = new PlaygroundPage(page);
    await playground.open();
    await use(playground);
  },
});

export const expect = base.expect;
