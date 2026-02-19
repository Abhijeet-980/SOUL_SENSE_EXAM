import { apiClient } from './client';
import { deduplicateRequest } from '../utils/requestUtils';

export interface UserSettings {
  theme: 'light' | 'dark' | 'system';
  notifications: {
    enabled: boolean;
    email: boolean;
    push: boolean;
    frequency: 'immediate' | 'daily' | 'weekly' | 'never';
  };
  privacy: {
    share_analytics: boolean;
    data_retention_days: number;
  };
  accessibility: {
    high_contrast: boolean;
    reduced_motion: boolean;
    font_size: 'small' | 'medium' | 'large';
  };
  sync: {
    enabled: boolean;
    last_synced: string | null;
  };
}

export interface UpdateSettingsData {
  theme?: 'light' | 'dark' | 'system';
  notifications?: Partial<UserSettings['notifications']>;
  privacy?: Partial<UserSettings['privacy']>;
  accessibility?: Partial<UserSettings['accessibility']>;
  sync?: Partial<UserSettings['sync']>;
}

export const settingsApi = {
  async getSettings(): Promise<UserSettings> {
    return deduplicateRequest('settings', async () => {
      try {
        return await apiClient<UserSettings>('/settings');
      } catch (error) {
        // Return defaults if the endpoint fails
        console.warn('[settingsApi] getSettings failed, using defaults:', error);
        return {
          theme: 'system',
          notifications: {
            enabled: true,
            email: true,
            push: false,
            frequency: 'daily',
          },
          privacy: {
            share_analytics: true,
            data_retention_days: 365,
          },
          accessibility: {
            high_contrast: false,
            reduced_motion: false,
            font_size: 'medium',
          },
          sync: {
            enabled: true,
            last_synced: null,
          },
        };
      }
    });
  },

  async updateSettings(data: UpdateSettingsData): Promise<UserSettings> {
    return apiClient<UserSettings>('/settings', {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  async syncSettings(): Promise<{ last_synced: string }> {
    return apiClient('/settings/sync', {
      method: 'POST',
    });
  },
};