# Testing Guide for SOUL_SENSE_EXAM

This guide covers the comprehensive testing infrastructure implemented for the SOUL_SENSE_EXAM project, including E2E tests, integration tests, API contract tests, and performance tests.

## Table of Contents

- [Overview](#overview)
- [Technology Stack](#technology-stack)
- [Test Structure](#test-structure)
- [Running Tests](#running-tests)
- [Writing Tests](#writing-tests)
- [CI/CD Integration](#cicd-integration)
- [Best Practices](#best-practices)

---

## Overview

The testing suite provides:

- **End-to-End (E2E) Tests**: Playwright-based tests for critical user flows in the Next.js frontend
- **Integration Tests**: Pytest-based tests for FastAPI backend endpoints
- **API Contract Tests**: Validation of OpenAPI schema compliance
- **Performance Tests**: Locust-based load testing for API endpoints
- **Visual Regression Tests**: Screenshot comparison testing for UI components

---

## Technology Stack

### Frontend Testing (E2E)

- **Playwright**: Cross-browser end-to-end testing framework
- **TypeScript**: Type-safe test implementations
- **Page Object Model**: Reusable page abstractions

### Backend Testing (Integration & Contract)

- **Pytest**: Python testing framework
- **FastAPI TestClient**: API testing utilities
- **SQLAlchemy**: Database testing with test databases

### Performance Testing

- **Locust**: Python-based load testing framework

---

## Test Structure

```
SOUL_SENSE_EXAM/
├── frontend-web/
│   └── tests-e2e/
│       ├── playwright.config.ts    # Playwright configuration
│       └── e2e/
│           ├── fixtures.ts         # Test fixtures and extensions
│           ├── pages/              # Page Object Models
│           │   ├── AuthPage.ts
│           │   ├── AssessmentPage.ts
│           │   ├── JournalPage.ts
│           │   └── ProfilePage.ts
│           ├── auth.spec.ts        # Authentication flow tests
│           ├── assessment.spec.ts  # Assessment flow tests
│           ├── journal.spec.ts     # Journal feature tests
│           ├── profile.spec.ts     # Profile management tests
│           └── visual.spec.ts      # Visual regression tests
├── tests/
│   ├── integration/
│   │   └── api/
│   │       ├── test_auth_api_integration.py
│   │       ├── test_assessments_api_integration.py
│   │       ├── test_journal_api_integration.py
│   │       ├── test_profile_api_integration.py
│   │       └── test_api_contract_validation.py
│   └── performance/
│       └── locustfile.py
```

---

## Running Tests

### Prerequisites

```bash
# Install frontend dependencies
cd frontend-web
npm install

# Install Playwright browsers
npm run test:e2e:install

# Install Python dependencies
pip install -r requirements.txt
pip install pytest locust
```

### E2E Tests

```bash
cd frontend-web

# Run all E2E tests
npm run test:e2e

# Run E2E tests in UI mode
npm run test:e2e:ui

# Debug E2E tests
npm run test:e2e:debug

# View test report
npm run test:e2e:report
```

### Integration Tests

```bash
# Run all integration tests
python -m pytest tests/integration/ -v

# Run specific test file
python -m pytest tests/integration/api/test_auth_api_integration.py -v

# Run with coverage
python -m pytest tests/integration/ --cov=backend/fastapi --cov-report=html

# Run with verbose output
python -m pytest tests/integration/ -vv -s
```

### API Contract Tests

```bash
# Run contract tests
python -m pytest tests/integration/api/test_api_contract_validation.py -v
```

### Performance Tests

```bash
# Run Locust web UI
cd tests/performance
locust -f locustfile.py --host=http://localhost:8000

# Run Locust headless
locust -f locustfile.py --host=http://localhost:8000 --headless -u 100 -r 10 -t 1m
```

**Parameters:**
- `-u`: Number of users to simulate
- `-r`: Hatch rate (users per second)
- `-t`: Run time (e.g., 1m, 5m, 1h)

---

## Writing Tests

### E2E Test Example

```typescript
import { test, expect } from './fixtures';
import { AuthPage } from './pages/AuthPage';

test.describe('Authentication Flow', () => {
  test('should login successfully', async ({ page }) => {
    const authPage = new AuthPage(page);
    await authPage.goto();
    await authPage.login('testuser', 'password123');

    await expect(page).toHaveURL('/dashboard');
  });
});
```

### Integration Test Example

```python
def test_user_registration(client):
    response = client.post(
        "/api/v1/auth/register",
        json={
            "username": "testuser",
            "password": "TestPass123!",
            "email": "test@example.com"
        }
    )
    assert response.status_code == 201
    data = response.json()
    assert "access_token" in data
    assert data["username"] == "testuser"
```

### Page Object Model Example

```typescript
export class AuthPage {
  readonly page: Page;
  readonly usernameInput: Locator;
  readonly passwordInput: Locator;
  readonly submitButton: Locator;

  constructor(page: Page) {
    this.page = page;
    this.usernameInput = page.locator('input[name="username"]');
    this.passwordInput = page.locator('input[name="password"]');
    this.submitButton = page.locator('button[type="submit"]');
  }

  async login(username: string, password: string) {
    await this.usernameInput.fill(username);
    await this.passwordInput.fill(password);
    await this.submitButton.click();
  }
}
```

---

## CI/CD Integration

### GitHub Actions Workflow

Create `.github/workflows/e2e-tests.yml`:

```yaml
name: E2E Tests

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

jobs:
  e2e:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18'

      - name: Install dependencies
        run: |
          cd frontend-web
          npm ci

      - name: Install Playwright
        run: |
          cd frontend-web
          npx playwright install --with-deps

      - name: Run E2E tests
        run: |
          cd frontend-web
          npm run test:e2e

      - name: Upload test report
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: playwright-report
          path: frontend-web/playwright-report/
```

Create `.github/workflows/integration-tests.yml`:

```yaml
name: Integration Tests

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

jobs:
  integration:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov

      - name: Run integration tests
        run: |
          python -m pytest tests/integration/ -v --cov=backend/fastapi

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

---

## Best Practices

### General Testing Guidelines

1. **Test Isolation**: Each test should be independent and runnable in isolation
2. **Clear Naming**: Use descriptive test names that explain what is being tested
3. **Arrange-Act-Assert**: Structure tests in AAA pattern
4. **Fixtures**: Use fixtures for common setup/teardown logic
5. **Mock External Services**: Don't depend on external APIs in tests

### E2E Testing Best Practices

1. **Use Page Object Models**: Abstract page interactions
2. **Wait for Elements**: Use explicit waits instead of sleep
3. **Test Critical Flows**: Focus on user journeys, not implementation details
4. **Avoid Flaky Tests**: Use stable selectors and proper waits
5. **Run in Multiple Browsers**: Test cross-browser compatibility

### Integration Testing Best Practices

1. **Use Test Databases**: Never run tests against production data
2. **Clean Up**: Use fixtures to clean up test data
3. **Test Edge Cases**: Include error conditions and edge cases
4. **Mock External Services**: Use test doubles for external dependencies
5. **Fast Execution**: Tests should run quickly

### API Contract Testing Best Practices

1. **Validate Schemas**: Ensure responses match OpenAPI specs
2. **Check Status Codes**: Validate correct HTTP status codes
3. **Test Error Responses**: Verify error response formats
4. **Version Compatibility**: Test deprecated endpoints
5. **Documentation Alignment**: Keep tests in sync with API docs

### Performance Testing Best Practices

1. **Realistic Scenarios**: Simulate real user behavior
2. **Gradual Ramp-up**: Increase load gradually
3. **Monitor Resources**: Track CPU, memory, database connections
4. **Set Baselines**: Establish performance baselines
5. **Test Regularly**: Run performance tests on schedule

---

## Troubleshooting

### E2E Test Issues

**Problem**: Tests fail with "Element not found"

**Solution**:
- Check if selectors are correct
- Increase wait time: `await expect(element).toBeVisible({ timeout: 10000 })`
- Verify the element exists in the current page state

**Problem**: Tests timeout

**Solution**:
- Increase timeout in `playwright.config.ts`
- Check if the backend server is running
- Verify network requests are not blocked

### Integration Test Issues

**Problem**: Database connection errors

**Solution**:
- Ensure test database URL is correct
- Run migrations: `alembic upgrade head`
- Check database file permissions

**Problem**: Tests pass locally but fail in CI

**Solution**:
- Check environment variables
- Ensure all dependencies are installed
- Verify test isolation (no shared state)

### Performance Test Issues

**Problem**: Locust can't connect

**Solution**:
- Verify the API server is running
- Check firewall settings
- Ensure correct host URL is provided

**Problem**: Out of memory errors

**Solution**:
- Reduce number of concurrent users
- Increase available memory
- Check for memory leaks in code

---

## Coverage Reports

### Generate Coverage Report

```bash
# Python backend
python -m pytest tests/integration/ --cov=backend/fastapi --cov-report=html

# View report
open htmlcov/index.html
```

### Target Coverage Goals

- **Backend API**: 80%+ coverage
- **Critical Flows**: 90%+ coverage
- **Edge Cases**: 70%+ coverage

---

## Continuous Improvement

1. **Review Test Results**: Regularly review test failures and flaky tests
2. **Update Tests**: Keep tests updated with feature changes
3. **Add New Tests**: Add tests for new features and bug fixes
4. **Refactor Tests**: Improve test code quality over time
5. **Monitor Performance**: Track test execution time and optimize

---

## Additional Resources

- [Playwright Documentation](https://playwright.dev)
- [Pytest Documentation](https://docs.pytest.org)
- [Locust Documentation](https://locust.io)
- [FastAPI Testing Guide](https://fastapi.tiangolo.com/tutorial/testing/)
