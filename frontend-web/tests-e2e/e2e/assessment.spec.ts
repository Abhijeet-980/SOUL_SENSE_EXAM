import { test, expect } from './fixtures';
import { AssessmentPage } from './pages/AssessmentPage';

test.describe('EQ Assessment Flow', () => {
  let assessmentPage: AssessmentPage;

  test.beforeEach(async ({ page }) => {
    assessmentPage = new AssessmentPage(page);
  });

  test('should start new assessment', async ({ authenticatedPage }) => {
    await assessmentPage.goto();
    await expect(assessmentPage.startButton).toBeVisible();

    await assessmentPage.startAssessment();
    await expect(assessmentPage.questionText).toBeVisible();
  });

  test('should navigate through questions', async ({ authenticatedPage }) => {
    await assessmentPage.goto();
    await assessmentPage.startAssessment();

    const firstQuestion = await assessmentPage.getQuestionText();
    expect(firstQuestion).toBeTruthy();

    await assessmentPage.answerQuestion(0);
    await assessmentPage.nextQuestion();

    const secondQuestion = await assessmentPage.getQuestionText();
    expect(secondQuestion).toBeTruthy();
    expect(secondQuestion).not.toBe(firstQuestion);
  });

  test('should save progress and resume', async ({ authenticatedPage }) => {
    await assessmentPage.goto();
    await assessmentPage.startAssessment();

    for (let i = 0; i < 3; i++) {
      await assessmentPage.answerQuestion(0);
      await assessmentPage.nextQuestion();
    }

    await authenticatedPage.goto('/dashboard');
    await assessmentPage.goto();

    const questionText = await assessmentPage.getQuestionText();
    expect(questionText).toBeTruthy();
  });

  test('should complete assessment and show results', async ({ authenticatedPage }) => {
    await assessmentPage.goto();
    await assessmentPage.startAssessment();

    for (let i = 0; i < 10; i++) {
      await assessmentPage.answerQuestion(0);
      if (i < 9) {
        await assessmentPage.nextQuestion();
      }
    }

    await assessmentPage.submitAssessment();

    await expect(assessmentPage.resultsContainer).toBeVisible({ timeout: 10000 });

    const score = await assessmentPage.getScore();
    expect(score).toBeTruthy();
  });

  test('should allow retaking assessment', async ({ authenticatedPage }) => {
    await assessmentPage.goto();
    await assessmentPage.startAssessment();

    for (let i = 0; i < 10; i++) {
      await assessmentPage.answerQuestion(0);
      if (i < 9) {
        await assessmentPage.nextQuestion();
      }
    }

    await assessmentPage.submitAssessment();
    await expect(assessmentPage.resultsContainer).toBeVisible();

    await authenticatedPage.click('button:has-text("Retake Assessment")');
    await expect(assessmentPage.startButton).toBeVisible();
  });

  test('should validate age-appropriate questions', async ({ page }) => {
    await page.goto('/exam?age=25');
    await assessmentPage.startAssessment();

    const questionText = await assessmentPage.getQuestionText();
    expect(questionText).toBeTruthy();

    await assessmentPage.answerQuestion(0);
    await assessmentPage.nextQuestion();

    const nextQuestion = await assessmentPage.getQuestionText();
    expect(nextQuestion).toBeTruthy();
  });
});
