import { apiClient } from './client';

export interface ExamAnswer {
  question_id: number;
  value: number;
}

export interface ExamSubmissionRequest {
  answers: ExamAnswer[];
  reflection?: string;
  duration_seconds: number;
}

export interface ExamResult {
  id: number;
  total_score: number;
  sentiment_score?: number;
  reflection?: string;
  timestamp: string;
}

export const examsApi = {
  async submitExam(data: ExamSubmissionRequest): Promise<ExamResult> {
    return apiClient('/exams', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
  },
};