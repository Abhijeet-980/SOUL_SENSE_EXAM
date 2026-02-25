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
  sleep_hours?: number;
  exercise_freq?: string;
  dietary_patterns?: string;
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
  sleep_hours?: number;
  exercise_freq?: string;
  dietary_patterns?: string;
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
    return deduplicateRequest('profile-me', async () => {
      const data = await apiClient<any>('/users/me/complete');
      return {
        id: data.user.id,
        user_id: data.user.id,
        first_name: data.personal_profile?.first_name || '',
        last_name: data.personal_profile?.last_name || '',
        bio: data.personal_profile?.bio || '',
        age: data.personal_profile?.age || 0,
        gender: data.personal_profile?.gender || '',
        avatar_url: data.personal_profile?.avatar_path || '',
        goals: {
          short_term: data.strengths?.short_term_goals || '',
          long_term: data.strengths?.long_term_vision || '',
        },
        preferences: {
          notification_frequency: 'daily',
          theme: data.settings?.theme || 'system',
        },
        created_at: data.user.created_at,
        updated_at: data.personal_profile?.last_updated || data.user.created_at,
        sleep_hours: data.personal_profile?.sleep_hours,
        exercise_freq: data.personal_profile?.exercise_freq,
        dietary_patterns: data.personal_profile?.dietary_patterns,
      };
    });
  },

  async updateUserProfile(data: UpdateUserProfile): Promise<UserProfile> {
    // 1. Update personal profile
    await apiClient('/profiles/personal', {
      method: 'PUT',
      body: JSON.stringify({
        first_name: data.first_name,
        last_name: data.last_name,
        bio: data.bio,
        age: data.age,
        gender: data.gender,
        sleep_hours: data.sleep_hours,
        exercise_freq: data.exercise_freq,
        dietary_patterns: data.dietary_patterns,
      }),
    });

    // 2. Update strengths
    if (data.goals) {
      await apiClient('/profiles/strengths', {
        method: 'PUT',
        body: JSON.stringify({
          short_term_goals: data.goals.short_term,
          long_term_vision: data.goals.long_term,
        }),
      });
    }

    // 3. Update settings
    if (data.preferences) {
      const settingsValidData: Record<string, any> = {};
      if (data.preferences.theme && ['light', 'dark'].includes(data.preferences.theme)) {
        settingsValidData.theme = data.preferences.theme;
      }
      if (Object.keys(settingsValidData).length > 0) {
        await apiClient('/profiles/settings', {
          method: 'PUT',
          body: JSON.stringify(settingsValidData),
        });
      }
    }

    return this.getUserProfile();
  },

  async uploadAvatar(file: File): Promise<{ avatar_url: string }> {
    const formData = new FormData();
    formData.append('avatar', file);

    return apiClient('/profiles/me/avatar', {
      method: 'POST',
      body: formData,
    });
  },

  async deleteAvatar(): Promise<void> {
    return apiClient('/profiles/me/avatar', {
      method: 'DELETE',
    });
  },
};
