'use client';

import { useState, useCallback } from 'react';
import { settingsApi, UserSettings, UpdateSettingsData } from '@/lib/api/settings';
import { useApi } from './useApi';

export interface UseSettingsResult {
  settings: UserSettings | null;
  isLoading: boolean;
  error: string | null;
  updateSettings: (data: UpdateSettingsData) => Promise<void>;
  syncSettings: () => Promise<void>;
  refetch: () => Promise<void>;
}

/**
 * Hook for managing user settings operations
 */
export function useSettings(): UseSettingsResult {
  const [updating, setUpdating] = useState(false);
  const [syncing, setSyncing] = useState(false);

  const {
    data: settings,
    loading,
    error,
    refetch,
  } = useApi({
    apiFn: () => settingsApi.getSettings(),
    deps: [],
  });

  const updateSettings = useCallback(async (data: UpdateSettingsData) => {
    setUpdating(true);
    try {
      await settingsApi.updateSettings(data);
      // Refetch settings data after successful update
      await refetch();
    } catch (err) {
      throw err;
    } finally {
      setUpdating(false);
    }
  }, [refetch]);

  const syncSettings = useCallback(async () => {
    setSyncing(true);
    try {
      await settingsApi.syncSettings();
      // Refetch settings to get updated last_synced timestamp
      await refetch();
    } catch (err) {
      throw err;
    } finally {
      setSyncing(false);
    }
  }, [refetch]);

  return {
    settings,
    isLoading: loading || updating || syncing,
    error,
    updateSettings,
    syncSettings,
    refetch,
  };
}