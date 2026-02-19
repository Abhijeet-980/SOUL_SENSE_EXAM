import { apiClient } from './client';
import { ApiError } from './errors';
import { deduplicateRequest } from '../utils/requestUtils';

export interface PersonalProfile {
  first_name: string;
  last_name: string;
  age?: number;
  gender?: string;
  email?: string;
  occupation?: string;
  education_level?: string;
  bio?: string;
  avatar_url?: string;
  member_since?: string;
  eq_stats?: {
    last_score?: number;
    total_assessments?: number;
  };
}

export interface MedicalProfile {
  conditions?: string[];
  medications?: string[];
  mental_health_history?: string;
}

export interface UserSettings {
  theme?: 'light' | 'dark' | 'system';
  notifications_enabled?: boolean;
  email_notifications?: boolean;
  language?: string;
}

export interface UpdatePersonalProfile {
  first_name?: string;
  last_name?: string;
  age?: number;
  gender?: string;
  occupation?: string;
  education_level?: string;
}

export interface UpdateSettings {
  theme?: 'light' | 'dark' | 'system';
  notifications_enabled?: boolean;
  email_notifications?: boolean;
  language?: string;
}

export interface UserProfile {
  id: number;
  user_id: number;
  first_name: string;
  last_name: string;
  bio: string;
  age: number;
  gender: string;
  avatar_url: string;
  goals: {
    short_term: string;
    long_term: string;
  };
  preferences: {
    notification_frequency: string;
    theme: string;
  };
  created_at: string;
  updated_at: string;
}

export interface UpdateUserProfile {
  first_name?: string;
  last_name?: string;
  bio?: string;
  age?: number;
  gender?: string;
  goals?: {
    short_term?: string;
    long_term?: string;
  };
  preferences?: {
    notification_frequency?: string;
    theme?: string;
  };
}

export const profileApi = {
  async getPersonalProfile(): Promise<PersonalProfile | null> {
    return deduplicateRequest('profile-personal', async () => {
      try {
        return await apiClient<PersonalProfile>('/profiles/personal');
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) {
          return null;
        }
        throw error;
      }
    });
  },

  async updatePersonalProfile(data: UpdatePersonalProfile): Promise<void> {
    return apiClient('/profiles/personal', {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  async getMedicalProfile(): Promise<MedicalProfile> {
    return deduplicateRequest('profile-medical', () => apiClient('/profiles/medical'));
  },

  async updateMedicalProfile(data: Partial<MedicalProfile>): Promise<void> {
    return apiClient('/profiles/medical', {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  async getSettings(): Promise<UserSettings> {
    return deduplicateRequest('profile-settings', async () => {
      try {
        return await apiClient<UserSettings>('/profiles/settings');
      } catch (error) {
        // Return defaults if the endpoint fails for any reason:
        // - 404: user hasn't created settings yet
        // - 500: backend model/schema mismatch
        // - Network error: server unreachable
        console.warn('[profileApi] getSettings failed, using defaults:', error);
        return {
          theme: 'system',
          notifications_enabled: true,
          email_notifications: true,
          language: 'en',
        };
      }
    });
  },

  async updateSettings(data: UpdateSettings): Promise<void> {
    return apiClient('/profiles/settings', {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  async getUserProfile(): Promise<UserProfile> {
    return deduplicateRequest('profile-me', () => apiClient<UserProfile>('/profiles/me'));
  },

  async updateUserProfile(data: UpdateUserProfile): Promise<UserProfile> {
    return apiClient<UserProfile>('/profiles/me', {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  async uploadAvatar(file: File): Promise<{ avatar_url: string }> {
    const formData = new FormData();
    formData.append('avatar', file);

    return apiClient('/profiles/me/avatar', {
      method: 'POST',
      body: formData,
      headers: {
        // Let the browser set the Content-Type for FormData
        'Content-Type': undefined,
      },
    });
  },

  async deleteAvatar(): Promise<void> {
    return apiClient('/profiles/me/avatar', {
      method: 'DELETE',
    });
  },
};
