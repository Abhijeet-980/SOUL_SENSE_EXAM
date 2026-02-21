import type { Page, Locator } from '@playwright/test';

export class JournalPage {
  readonly page: Page;
  readonly newEntryButton: Locator;
  readonly journalContentInput: Locator;
  readonly saveButton: Locator;
  readonly moodSelector: Locator;
  readonly tagInput: Locator;
  readonly journalEntries: Locator;
  readonly deleteButton: Locator;
  readonly editButton: Locator;

  constructor(page: Page) {
    this.page = page;
    this.newEntryButton = page.locator('button:has-text("New Entry")');
    this.journalContentInput = page.locator('textarea[name="content"]');
    this.saveButton = page.locator('button:has-text("Save")');
    this.moodSelector = page.locator('[data-testid="mood-selector"]');
    this.tagInput = page.locator('input[name="tags"]');
    this.journalEntries = page.locator('[data-testid="journal-entry"]');
    this.deleteButton = page.locator('button:has-text("Delete")');
    this.editButton = page.locator('button:has-text("Edit")');
  }

  async goto() {
    await this.page.goto('/journal');
  }

  async createEntry(content: string, mood?: string, tags?: string[]) {
    await this.newEntryButton.click();
    await this.journalContentInput.fill(content);
    if (mood) {
      await this.moodSelector.selectOption(mood);
    }
    if (tags && tags.length > 0) {
      for (const tag of tags) {
        await this.tagInput.fill(tag);
        await this.page.keyboard.press('Enter');
      }
    }
    await this.saveButton.click();
  }

  async getEntryCount(): Promise<number> {
    return await this.journalEntries.count();
  }

  async deleteEntry(index: number) {
    const entries = await this.journalEntries.all();
    await entries[index].hover();
    await this.deleteButton.click();
  }

  async editEntry(index: number, newContent: string) {
    const entries = await this.journalEntries.all();
    await entries[index].hover();
    await this.editButton.click();
    await this.journalContentInput.fill(newContent);
    await this.saveButton.click();
  }
}
