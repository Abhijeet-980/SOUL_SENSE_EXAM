import type { Page, Locator } from '@playwright/test';

export class AssessmentPage {
  readonly page: Page;
  readonly startButton: Locator;
  readonly nextButton: Locator;
  readonly previousButton: Locator;
  readonly submitButton: Locator;
  readonly questionText: Locator;
  readonly answerOptions: Locator;
  readonly progressBar: Locator;
  readonly scoreDisplay: Locator;
  readonly resultsContainer: Locator;

  constructor(page: Page) {
    this.page = page;
    this.startButton = page.locator('button:has-text("Start Assessment")');
    this.nextButton = page.locator('button:has-text("Next")');
    this.previousButton = page.locator('button:has-text("Previous")');
    this.submitButton = page.locator('button:has-text("Submit")');
    this.questionText = page.locator('[data-testid="question-text"]');
    this.answerOptions = page.locator('[data-testid="answer-option"]');
    this.progressBar = page.locator('[data-testid="progress-bar"]');
    this.scoreDisplay = page.locator('[data-testid="score-display"]');
    this.resultsContainer = page.locator('[data-testid="results-container"]');
  }

  async goto() {
    await this.page.goto('/exam');
  }

  async startAssessment() {
    await this.startButton.click();
  }

  async answerQuestion(answerIndex: number) {
    const options = await this.answerOptions.all();
    await options[answerIndex].click();
  }

  async nextQuestion() {
    await this.nextButton.click();
  }

  async previousQuestion() {
    await this.previousButton.click();
  }

  async submitAssessment() {
    await this.submitButton.click();
  }

  async getQuestionText(): Promise<string> {
    return await this.questionText.textContent() || '';
  }

  async getScore(): Promise<string> {
    return await this.scoreDisplay.textContent() || '';
  }

  async isResultsVisible(): Promise<boolean> {
    return await this.resultsContainer.isVisible();
  }
}
