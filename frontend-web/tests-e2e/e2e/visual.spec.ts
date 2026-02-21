import { test, expect } from './fixtures';

test.describe('Visual Regression Tests', () => {
  test('should match dashboard screenshot', async ({ authenticatedPage }) => {
    await authenticatedPage.goto('/dashboard');
    await expect(authenticatedPage).toHaveScreenshot('dashboard.png', {
      maxDiffPixels: 100,
      threshold: 0.2
    });
  });

  test('should match assessment page screenshot', async ({ authenticatedPage }) => {
    await authenticatedPage.goto('/exam');
    await expect(authenticatedPage).toHaveScreenshot('assessment-start.png');
  });

  test('should match journal page screenshot', async ({ authenticatedPage }) => {
    await authenticatedPage.goto('/journal');
    await expect(authenticatedPage).toHaveScreenshot('journal.png');
  });

  test('should match profile page screenshot', async ({ authenticatedPage }) => {
    await authenticatedPage.goto('/profile');
    await expect(authenticatedPage).toHaveScreenshot('profile.png');
  });

  test('should match login page screenshot', async ({ page }) => {
    await page.goto('/login');
    await expect(page).toHaveScreenshot('login.png');
  });

  test('should match screenshots in different themes', async ({ authenticatedPage }) => {
    await authenticatedPage.goto('/dashboard');

    await authenticatedPage.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark');
    });
    await expect(authenticatedPage).toHaveScreenshot('dashboard-dark.png');

    await authenticatedPage.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'light');
    });
    await expect(authenticatedPage).toHaveScreenshot('dashboard-light.png');
  });

  test('should match screenshots on mobile viewport', async ({ authenticatedPage }) => {
    await authenticatedPage.setViewportSize({ width: 375, height: 667 });
    await authenticatedPage.goto('/dashboard');
    await expect(authenticatedPage).toHaveScreenshot('dashboard-mobile.png');
  });

  test('should match screenshots on tablet viewport', async ({ authenticatedPage }) => {
    await authenticatedPage.setViewportSize({ width: 768, height: 1024 });
    await authenticatedPage.goto('/dashboard');
    await expect(authenticatedPage).toHaveScreenshot('dashboard-tablet.png');
  });
});
