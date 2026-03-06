import { apiClient } from './client';
import { deduplicateRequest } from '../utils/requestUtils';

export interface DashboardSummary {
  total_assessments: number;
  average_score: number;
  journal_entries_count: number;
  current_streak: number;
  wellbeing_score?: number;
  recent_activity?: RecentActivity[];
}

export interface RecentActivity {
  id: number;
  type: 'assessment' | 'journal' | 'achievement';
  title: string;
  description?: string;
  timestamp: string;
}

export interface TrendsData {
  dates: string[];
  wellbeing_scores: number[];
  mood_scores?: number[];
  stress_levels?: number[];
}

export const dashboardApi = {
  async getSummary(): Promise<DashboardSummary> {
    return deduplicateRequest('dashboard-summary', () =>
      apiClient('/analytics/me/summary', { retry: true })
    );
  },

  async getTrends(days: number = 30): Promise<TrendsData> {
    return deduplicateRequest(`dashboard-trends-${days}`, () =>
      apiClient(`/analytics/me/trends?days=${days}`, { retry: true })
    );
  },
};
