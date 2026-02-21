import { apiClient } from './client';

export interface AnalyticsEvent {
  anonymous_id: string;
  event_type: string;
  event_name: string;
  event_data?: Record<string, any>;
}

export const getAnonymousId = (): string => {
  if (typeof window === 'undefined') return '';

  const STORAGE_KEY = 'analytics_anonymous_id';
  let id = localStorage.getItem(STORAGE_KEY);

  if (!id) {
    // Generate a simple UUID-like string if crypto.randomUUID is not available
    if (typeof crypto !== 'undefined' && crypto.randomUUID) {
      id = crypto.randomUUID();
    } else {
      id = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        var r = (Math.random() * 16) | 0,
          v = c == 'x' ? r : (r & 0x3) | 0x8;
        return v.toString(16);
      });
    }
    localStorage.setItem(STORAGE_KEY, id);
  }
  return id;
};

export const analyticsApi = {
  trackEvent: async (event: Omit<AnalyticsEvent, 'anonymous_id'>) => {
    try {
      const anonymous_id = getAnonymousId();

      // Safety check: Strip potential PII keys if they accidentally slip in
      const safeData = { ...event.event_data };
      const forbidden = ['password', 'token', 'secret', 'credit_card'];

      if (safeData) {
        Object.keys(safeData).forEach((key) => {
          if (forbidden.some((term) => key.toLowerCase().includes(term))) {
            delete safeData[key];
          }
        });
      }

      return await apiClient<void>('/analytics/events', {
        method: 'POST',
        body: JSON.stringify({
          ...event,
          event_data: safeData,
          anonymous_id,
        }),
      });
    } catch (error) {
      // Analytics should default to fail-silent in production
      console.error('Failed to log analytics event', error);
    }
  },
};
