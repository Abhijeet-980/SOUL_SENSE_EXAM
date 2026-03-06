import { apiClient } from './client';
import { DetailedExamResult, AssessmentResponse } from '@/types/results';

export interface ExamAnswer {
  question_id: number;
  value: number;
}

export interface ExamSubmissionRequest {
  answers: ExamAnswer[];
  reflection?: string;
  duration_seconds: number;
}

export interface ExamSubmissionResponse {
  id: number;
  total_score: number;
  sentiment_score?: number;
  reflection?: string;
  timestamp: string;
}

export interface ExamDraftRequest {
  answers: Record<number, number>; // questionId -> answer value
  current_question_index?: number;
  duration_seconds?: number;
}

export interface ExamDraftResponse {
  id: string;
  answers: Record<number, number>;
  current_question_index: number;
  duration_seconds: number;
  updated_at: string;
}

export const examsApi = {
  async submitExam(data: ExamSubmissionRequest): Promise<ExamSubmissionResponse> {
    return apiClient('/exams', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
  },

  async saveDraft(examId: string, data: ExamDraftRequest): Promise<ExamDraftResponse> {
    return apiClient(`/exams/${examId}/draft`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
  },

  async getDraft(examId: string): Promise<ExamDraftResponse | null> {
    try {
      return await apiClient(`/exams/${examId}/draft`, {
        method: 'GET',
      });
    } catch (error) {
      // Return null if draft doesn't exist
      return null;
    }
  },

  async getExamResult(id: number): Promise<DetailedExamResult> {
    return apiClient(`/exams/${id}`, {
      method: 'GET',
    });
  },

  async getExamResults(): Promise<AssessmentResponse[]> {
    return apiClient('/exams', {
      method: 'GET',
    });
  },
};
