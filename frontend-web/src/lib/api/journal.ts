import { apiClient } from './client';
import { deduplicateRequest } from '../utils/requestUtils';

export interface JournalEntry {
  id: number;
  content: string;
  sentiment_score?: number;
  mood_score?: number;
  energy_level?: number;
  stress_level?: number;
  tags: string[];
  created_at: string;
  updated_at: string;
}

export interface JournalListResponse {
  entries: JournalEntry[];
  total: number;
  page: number;
  page_size: number;
}

export interface CreateJournalEntry {
  content: string;
  tags?: string[];
  privacy_level?: string;
  energy_level?: number;
  stress_level?: number;
}

export interface JournalAnalytics {
  total_entries: number;
  average_sentiment?: number;
  most_common_mood?: string;
  streak_days: number;
}

export const journalApi = {
  async listEntries(page: number = 1, limit: number = 10): Promise<JournalListResponse> {
    const skip = (page - 1) * limit;
    return deduplicateRequest(`journal-list-${page}-${limit}`, () =>
      apiClient(`/journal?skip=${skip}&limit=${limit}`, { retry: true })
    );
  },

  async getEntry(id: number): Promise<JournalEntry> {
    return apiClient(`/journal/${id}`);
  },

  async createEntry(data: CreateJournalEntry): Promise<JournalEntry> {
    return apiClient('/journal', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  },

  async updateEntry(id: number, data: Partial<CreateJournalEntry>): Promise<JournalEntry> {
    return apiClient(`/journal/${id}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  async deleteEntry(id: number): Promise<void> {
    return apiClient(`/journal/${id}`, {
      method: 'DELETE',
    });
  },

  async getAnalytics(): Promise<JournalAnalytics> {
    return deduplicateRequest('journal-analytics', () =>
      apiClient('/journal/analytics', { retry: true })
    );
  },

  async searchEntries(query: string, page: number = 1): Promise<JournalListResponse> {
    return apiClient(`/journal/search?q=${encodeURIComponent(query)}&page=${page}`);
  },
};
