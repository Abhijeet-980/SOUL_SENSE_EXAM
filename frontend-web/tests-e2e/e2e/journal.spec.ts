import { test, expect } from './fixtures';
import { JournalPage } from './pages/JournalPage';

test.describe('Journal Flow', () => {
  let journalPage: JournalPage;

  test.beforeEach(async ({ page }) => {
    journalPage = new JournalPage(page);
  });

  test('should display journal page', async ({ authenticatedPage }) => {
    await journalPage.goto();
    await expect(journalPage.newEntryButton).toBeVisible();
  });

  test('should create new journal entry', async ({ authenticatedPage }) => {
    await journalPage.goto();
    await journalPage.createEntry('Today was a productive day', 'happy', ['work', 'progress']);

    const entryCount = await journalPage.getEntryCount();
    expect(entryCount).toBeGreaterThan(0);
  });

  test('should display journal entries with mood', async ({ authenticatedPage }) => {
    await journalPage.goto();
    await journalPage.createEntry('Feeling grateful today', 'grateful');

    await expect(journalPage.journalEntries.first()).toBeVisible();
  });

  test('should edit journal entry', async ({ authenticatedPage }) => {
    await journalPage.goto();
    await journalPage.createEntry('Initial entry');

    await journalPage.editEntry(0, 'Updated entry content');

    await expect(journalPage.journalEntries.first()).toContainText('Updated');
  });

  test('should delete journal entry', async ({ authenticatedPage }) => {
    await journalPage.goto();
    await journalPage.createEntry('Entry to delete');

    const initialCount = await journalPage.getEntryCount();
    await journalPage.deleteEntry(0);

    const finalCount = await journalPage.getEntryCount();
    expect(finalCount).toBeLessThan(initialCount);
  });

  test('should filter entries by mood', async ({ authenticatedPage }) => {
    await journalPage.goto();
    await journalPage.createEntry('Happy day', 'happy');
    await journalPage.createEntry('Sad moment', 'sad');
    await journalPage.createEntry('Another happy moment', 'happy');

    await authenticatedPage.selectOption('[data-testid="mood-filter"]', 'happy');

    const entries = await journalPage.journalEntries.all();
    expect(entries.length).toBeGreaterThan(0);
  });

  test('should filter entries by date', async ({ authenticatedPage }) => {
    await journalPage.goto();

    const today = new Date().toISOString().split('T')[0];
    await authenticatedPage.fill('[data-testid="date-filter"]', today);

    await expect(journalPage.journalEntries.first()).toBeVisible();
  });

  test('should add tags to journal entry', async ({ authenticatedPage }) => {
    await journalPage.goto();
    await journalPage.createEntry('Important meeting', 'neutral', ['work', 'meeting']);

    const entry = journalPage.journalEntries.first();
    await expect(entry).toContainText('work');
    await expect(entry).toContainText('meeting');
  });

  test('should display sentiment analysis', async ({ authenticatedPage }) => {
    await journalPage.goto();
    await journalPage.createEntry('I am very happy and excited about the progress!');

    const entry = journalPage.journalEntries.first();
    await expect(entry.locator('[data-testid="sentiment"]')).toBeVisible();
  });

  test('should handle multiple entries', async ({ authenticatedPage }) => {
    await journalPage.goto();

    for (let i = 0; i < 5; i++) {
      await journalPage.createEntry(`Entry number ${i}`);
    }

    const entryCount = await journalPage.getEntryCount();
    expect(entryCount).toBeGreaterThanOrEqual(5);
  });
});
