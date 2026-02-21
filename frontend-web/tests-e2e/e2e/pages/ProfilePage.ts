import type { Page, Locator } from '@playwright/test';

export class ProfilePage {
  readonly page: Page;
  readonly editProfileButton: Locator;
  readonly nameInput: Locator;
  readonly emailInput: Locator;
  readonly saveButton: Locator;
  readonly changePasswordButton: Locator;
  readonly currentPasswordInput: Locator;
  readonly newPasswordInput: Locator;
  readonly confirmPasswordInput: Locator;
  readonly deleteAccountButton: Locator;
  readonly confirmDeleteButton: Locator;
  readonly successMessage: Locator;

  constructor(page: Page) {
    this.page = page;
    this.editProfileButton = page.locator('button:has-text("Edit Profile")');
    this.nameInput = page.locator('input[name="name"]');
    this.emailInput = page.locator('input[name="email"]');
    this.saveButton = page.locator('button:has-text("Save")');
    this.changePasswordButton = page.locator('button:has-text("Change Password")');
    this.currentPasswordInput = page.locator('input[name="current_password"]');
    this.newPasswordInput = page.locator('input[name="new_password"]');
    this.confirmPasswordInput = page.locator('input[name="confirm_password"]');
    this.deleteAccountButton = page.locator('button:has-text("Delete Account")');
    this.confirmDeleteButton = page.locator('button:has-text("Confirm")');
    this.successMessage = page.locator('[data-testid="success-message"]');
  }

  async goto() {
    await this.page.goto('/profile');
  }

  async updateProfile(name: string, email: string) {
    await this.editProfileButton.click();
    await this.nameInput.fill(name);
    await this.emailInput.fill(email);
    await this.saveButton.click();
  }

  async changePassword(currentPassword: string, newPassword: string) {
    await this.changePasswordButton.click();
    await this.currentPasswordInput.fill(currentPassword);
    await this.newPasswordInput.fill(newPassword);
    await this.confirmPasswordInput.fill(newPassword);
    await this.saveButton.click();
  }

  async deleteAccount() {
    await this.deleteAccountButton.click();
    await this.confirmDeleteButton.click();
  }

  async getSuccessMessage(): Promise<string> {
    return await this.successMessage.textContent() || '';
  }
}
