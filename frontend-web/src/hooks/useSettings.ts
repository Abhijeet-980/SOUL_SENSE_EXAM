import { useState, useEffect, useCallback } from 'react';
import { settingsApi, UserSettings } from '@/lib/api/settings';

export function useSettings() {
  const [settings, setSettings] = useState<UserSettings | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchSettings = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);
      const data = await settingsApi.getSettings();
      setSettings(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load settings');
    } finally {
      setIsLoading(false);
    }
  }, []);

  const updateSettings = useCallback(async (updates: Partial<UserSettings>) => {
    if (!settings) return;

    try {
      const updatedSettings = { ...settings, ...updates };
      setSettings(updatedSettings);
      await settingsApi.updateSettings(updates);
    } catch (err) {
      // Revert on error
      setSettings(settings);
      throw err;
    }
  }, [settings]);

  const syncSettings = useCallback(async () => {
    try {
      const data = await settingsApi.syncSettings();
      setSettings(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to sync settings');
    }
  }, []);

  useEffect(() => {
    fetchSettings();
  }, [fetchSettings]);

  return {
    settings,
    isLoading,
    error,
    updateSettings,
    syncSettings,
    refetch: fetchSettings,
  };
}
