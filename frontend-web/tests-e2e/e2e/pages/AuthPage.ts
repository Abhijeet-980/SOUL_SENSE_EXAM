import type { Page, Locator } from '@playwright/test';

export class AuthPage {
  readonly page: Page;
  readonly usernameInput: Locator;
  readonly passwordInput: Locator;
  readonly submitButton: Locator;
  readonly errorMessage: Locator;
  readonly loginLink: Locator;
  readonly signupLink: Locator;

  constructor(page: Page) {
    this.page = page;
    this.usernameInput = page.locator('input[name="username"]');
    this.passwordInput = page.locator('input[name="password"]');
    this.submitButton = page.locator('button[type="submit"]');
    this.errorMessage = page.locator('[data-testid="error-message"]');
    this.loginLink = page.locator('a[href="/login"]');
    this.signupLink = page.locator('a[href="/signup"]');
  }

  async goto() {
    await this.page.goto('/login');
  }

  async gotoSignup() {
    await this.page.goto('/signup');
  }

  async login(username: string, password: string) {
    await this.usernameInput.fill(username);
    await this.passwordInput.fill(password);
    await this.submitButton.click();
  }

  async signup(username: string, password: string, email?: string) {
    await this.gotoSignup();
    await this.usernameInput.fill(username);
    await this.passwordInput.fill(password);
    if (email) {
      await this.page.fill('input[name="email"]', email);
    }
    await this.submitButton.click();
  }

  async getErrorMessage(): Promise<string> {
    return await this.errorMessage.textContent() || '';
  }
}
