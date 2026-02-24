import { test, expect } from './fixtures';
import { AuthPage } from './pages/AuthPage';

test.describe('Authentication Flow', () => {
  let authPage: AuthPage;

  test.beforeEach(async ({ page }) => {
    authPage = new AuthPage(page);
  });

  test('should display login page', async ({ page }) => {
    await authPage.goto();
    await expect(page).toHaveTitle(/Soul Sense/);
    await expect(authPage.usernameInput).toBeVisible();
    await expect(authPage.passwordInput).toBeVisible();
    await expect(authPage.submitButton).toBeVisible();
  });

  test('should show error with invalid credentials', async ({ page }) => {
    await authPage.goto();
    await authPage.login('invalid_user', 'wrong_password');

    const errorMessage = await authPage.getErrorMessage();
    expect(errorMessage).toBeTruthy();
  });

  test('should redirect to dashboard on successful login', async ({ page }) => {
    await authPage.goto();
    await authPage.login('e2e_test_user', 'TestPass123!');

    await page.waitForURL('/dashboard', { timeout: 5000 });
    await expect(page).toHaveURL(/.*dashboard/);
  });



  test('should not emit unmounted state update warning during rapid auth-page navigation', async ({ page }) => {
    const consoleWarnings: string[] = [];

    page.on('console', msg => {
      if (msg.type() === 'warning' || msg.type() === 'error') {
        consoleWarnings.push(msg.text());
      }
    });

    await authPage.goto();

    // Trigger route change quickly while auth initialization timeout is still pending.
    await authPage.signupLink.click();

    await expect(page).toHaveURL(/.*signup/);
    await page.waitForTimeout(200);

    const unmountedStateWarning = consoleWarnings.find(message =>
      message.includes("Can't perform a React state update on an unmounted component")
    );

    expect(unmountedStateWarning).toBeUndefined();
  });

  test('should navigate to signup page', async ({ page }) => {
    await authPage.goto();
    await authPage.signupLink.click();

    await expect(page).toHaveURL(/.*signup/);
  });

  test('should register new user', async ({ page }) => {
    const timestamp = Date.now();
    const username = `test_user_${timestamp}`;
    const password = 'TestPass123!';

    await authPage.signup(username, password, `test${timestamp}@example.com`);

    await page.waitForURL('/dashboard', { timeout: 5000 });
    await expect(page).toHaveURL(/.*dashboard/);
  });

  test('should validate required fields', async ({ page }) => {
    await authPage.goto();
    await authPage.submitButton.click();

    await expect(authPage.errorMessage).toBeVisible();
  });

  test('should logout successfully', async ({ authenticatedPage }) => {
    await authenticatedPage.goto('/profile');
    await authenticatedPage.click('button:has-text("Logout")');

    await expect(authenticatedPage).toHaveURL(/.*login/);
  });

  test('should remember me functionality', async ({ page, context }) => {
    await authPage.goto();
    await page.check('input[name="remember"]');
    await authPage.login('e2e_test_user', 'TestPass123!');

    await page.waitForURL('/dashboard');

    const cookies = await context.cookies();
    const rememberCookie = cookies.find(c => c.name === 'remember_token');
    expect(rememberCookie).toBeDefined();
  });
});
