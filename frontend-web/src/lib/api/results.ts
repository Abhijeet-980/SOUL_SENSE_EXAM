import { apiClient } from './client';
import { DetailedExamResult, AssessmentListResponse } from '../../types/results';

/**
 * Service for fetching assessment results and detailed breakdowns.
 */
export const resultsApi = {
  /**
   * Fetches the user's exam history with pagination.
   */
  async getHistory(page = 1, pageSize = 10): Promise<AssessmentListResponse> {
    const params = new URLSearchParams({
      page: page.toString(),
      page_size: pageSize.toString(),
    });
    return apiClient(`/exams/history?${params.toString()}`, {
      method: 'GET',
    });
  },

  /**
   * Fetches a detailed breakdown for a specific assessment.
   */
  async getDetailedResult(assessmentId: number): Promise<DetailedExamResult> {
    return apiClient(`/exams/${assessmentId}/results`, {
      method: 'GET',
    });
  },
};
