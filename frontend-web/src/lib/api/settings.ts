import { apiClient } from './client';

export interface UserSettings {
  theme: 'light' | 'dark' | 'system';
  notifications: {
    email: boolean;
    push: boolean;
    frequency: 'immediate' | 'daily' | 'weekly';
    types: {
      exam_reminders: boolean;
      journal_prompts: boolean;
      progress_updates: boolean;
      system_updates: boolean;
    };
  };
  privacy: {
    data_collection: boolean;
    analytics: boolean;
    data_retention_days: number;
    profile_visibility: 'public' | 'private' | 'friends';
  };
  accessibility: {
    high_contrast: boolean;
    reduced_motion: boolean;
    font_size: 'small' | 'medium' | 'large';
  };
  account: {
    language: string;
    timezone: string;
    date_format: string;
  };
}

export const settingsApi = {
  async getSettings(): Promise<UserSettings> {
    try {
      const response = await apiClient.get('/api/v1/settings');
      return response.data;
    } catch (error) {
      // Return default settings if API fails
      return {
        theme: 'system',
        notifications: {
          email: true,
          push: true,
          frequency: 'daily',
          types: {
            exam_reminders: true,
            journal_prompts: true,
            progress_updates: true,
            system_updates: false,
          },
        },
        privacy: {
          data_collection: true,
          analytics: true,
          data_retention_days: 365,
          profile_visibility: 'private',
        },
        accessibility: {
          high_contrast: false,
          reduced_motion: false,
          font_size: 'medium',
        },
        account: {
          language: 'en',
          timezone: 'UTC',
          date_format: 'MM/DD/YYYY',
        },
      };
    }
  },

  async updateSettings(updates: Partial<UserSettings>): Promise<UserSettings> {
    try {
      const response = await apiClient.put('/api/v1/settings', updates);
      return response.data;
    } catch (error) {
      throw new Error('Failed to update settings');
    }
  },

  async syncSettings(): Promise<UserSettings> {
    try {
      const response = await apiClient.post('/api/v1/settings/sync');
      return response.data;
    } catch (error) {
      throw new Error('Failed to sync settings');
    }
  },
};
