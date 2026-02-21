import { test, expect } from './fixtures';
import { ProfilePage } from './pages/ProfilePage';

test.describe('Profile Management Flow', () => {
  let profilePage: ProfilePage;

  test.beforeEach(async ({ page }) => {
    profilePage = new ProfilePage(page);
  });

  test('should display profile page', async ({ authenticatedPage }) => {
    await profilePage.goto();
    await expect(profilePage.editProfileButton).toBeVisible();
  });

  test('should update personal information', async ({ authenticatedPage }) => {
    await profilePage.goto();
    await profilePage.updateProfile('John Doe', 'john.doe@example.com');

    const successMessage = await profilePage.getSuccessMessage();
    expect(successMessage).toContain('updated');
  });

  test('should change password', async ({ authenticatedPage }) => {
    await profilePage.goto();
    await profilePage.changePassword('TestPass123!', 'NewTestPass456!');

    const successMessage = await profilePage.getSuccessMessage();
    expect(successMessage).toContain('changed');

    await authenticatedPage.goto('/logout');
    await authenticatedPage.goto('/login');
    await authenticatedPage.fill('input[name="username"]', 'e2e_test_user');
    await authenticatedPage.fill('input[name="password"]', 'NewTestPass456!');
    await authenticatedPage.click('button[type="submit"]');

    await expect(authenticatedPage).toHaveURL(/.*dashboard/);
  });

  test('should validate password confirmation', async ({ authenticatedPage }) => {
    await profilePage.goto();
    await profilePage.changePasswordButton.click();

    await profilePage.currentPasswordInput.fill('TestPass123!');
    await profilePage.newPasswordInput.fill('NewTestPass456!');
    await profilePage.confirmPasswordInput.fill('DifferentPass789!');
    await profilePage.saveButton.click();

    await expect(profilePage.page.locator('[data-testid="error-message"]')).toBeVisible();
  });

  test('should display privacy settings', async ({ authenticatedPage }) => {
    await profilePage.goto();
    await authenticatedPage.click('button:has-text("Privacy Settings")');

    await expect(profilePage.page.locator('[data-testid="privacy-settings"]')).toBeVisible();
  });

  test('should update privacy settings', async ({ authenticatedPage }) => {
    await profilePage.goto();
    await authenticatedPage.click('button:has-text("Privacy Settings")');

    await authenticatedPage.check('input[name="share_analytics"]');
    await authenticatedPage.click('button:has-text("Save")');

    const successMessage = await profilePage.getSuccessMessage();
    expect(successMessage).toContain('saved');
  });

  test('should export data', async ({ authenticatedPage }) => {
    await profilePage.goto();
    await authenticatedPage.click('button:has-text("Export Data")');

    const [download] = await Promise.all([
      authenticatedPage.waitForEvent('download'),
      authenticatedPage.click('button:has-text("Download JSON")')
    ]);

    expect(download.suggestedFilename()).toMatch(/\.json$/);
  });

  test('should delete account with confirmation', async ({ page }) => {
    await page.goto('/login');
    const timestamp = Date.now();
    await page.fill('input[name="username"]', `delete_test_${timestamp}`);
    await page.fill('input[name="password"]', 'TestPass123!');
    await page.click('button[type="submit"]');

    await profilePage.goto();
    await profilePage.deleteAccount();

    await expect(page).toHaveURL(/.*login/);
  });
});
