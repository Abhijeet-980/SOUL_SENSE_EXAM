'use client';

import { useState, useCallback } from 'react';
import { profileApi, PersonalProfile, UpdatePersonalProfile } from '@/lib/api/profile';
import { useApi } from './useApi';

export interface UseProfileResult {
  profile: PersonalProfile | null;
  loading: boolean;
  error: string | null;
  updateProfile: (data: UpdatePersonalProfile) => Promise<void>;
  refetch: () => Promise<void>;
}

/**
 * Hook for managing user profile data
 */
export function useProfile(): UseProfileResult {
  const [updating, setUpdating] = useState(false);

  const {
    data: profile,
    loading,
    error,
    refetch,
  } = useApi({
    apiFn: () => profileApi.getPersonalProfile(),
    deps: [],
  });

  const updateProfile = useCallback(async (data: UpdatePersonalProfile) => {
    setUpdating(true);
    try {
      await profileApi.updatePersonalProfile(data);
      // Refetch profile data after successful update
      await refetch();
    } catch (err) {
      throw err;
    } finally {
      setUpdating(false);
    }
  }, [refetch]);

  return {
    profile,
    loading: loading || updating,
    error,
    updateProfile,
    refetch,
  };
}