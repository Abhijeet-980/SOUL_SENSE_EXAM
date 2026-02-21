import { test as base } from '@playwright/test';

export type TestOptions = {
  authenticatedPage: boolean;
};

export const test = base.extend<TestOptions>({
  authenticatedPage: async ({ page }, use) => {
    const testUser = {
      username: 'e2e_test_user',
      password: 'TestPass123!',
    };

    await page.goto('/login');

    await page.fill('input[name="username"]', testUser.username);
    await page.fill('input[name="password"]', testUser.password);
    await page.click('button[type="submit"]');

    await page.waitForURL('/dashboard');

    await use(page);

    await page.goto('/logout');
  },
});

export { expect } from '@playwright/test';
