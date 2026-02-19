'use client';

import { useState, useCallback } from 'react';
import { useApi } from './useApi';
import { profileApi, UserProfile, UpdateUserProfile } from '@/lib/api/profile';

interface UseProfileReturn {
  profile: UserProfile | null;
  isLoading: boolean;
  error: string | null;
  updateProfile: (data: UpdateUserProfile) => Promise<void>;
  uploadAvatar: (file: File) => Promise<void>;
  deleteAvatar: () => Promise<void>;
  refetch: () => void;
}

export function useProfile(): UseProfileReturn {
  const [updateLoading, setUpdateLoading] = useState(false);
  const [updateError, setUpdateError] = useState<string | null>(null);

  const {
    data: profile,
    loading: fetchLoading,
    error: fetchError,
    refetch,
  } = useApi({
    apiFn: () => profileApi.getUserProfile(),
    immediate: true,
  });

  const updateProfile = useCallback(async (data: UpdateUserProfile) => {
    setUpdateLoading(true);
    setUpdateError(null);

    try {
      await profileApi.updateUserProfile(data);
      // Refetch the profile after successful update
      await refetch();
    } catch (error: any) {
      const errorMessage = error?.message || 'Failed to update profile';
      setUpdateError(errorMessage);
      throw error;
    } finally {
      setUpdateLoading(false);
    }
  }, [refetch]);

  const uploadAvatar = useCallback(async (file: File) => {
    setUpdateLoading(true);
    setUpdateError(null);

    try {
      await profileApi.uploadAvatar(file);
      // Refetch the profile after successful upload
      await refetch();
    } catch (error: any) {
      const errorMessage = error?.message || 'Failed to upload avatar';
      setUpdateError(errorMessage);
      throw error;
    } finally {
      setUpdateLoading(false);
    }
  }, [refetch]);

  const deleteAvatar = useCallback(async () => {
    setUpdateLoading(true);
    setUpdateError(null);

    try {
      await profileApi.deleteAvatar();
      // Refetch the profile after successful deletion
      await refetch();
    } catch (error: any) {
      const errorMessage = error?.message || 'Failed to delete avatar';
      setUpdateError(errorMessage);
      throw error;
    } finally {
      setUpdateLoading(false);
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
    isLoading: fetchLoading || updateLoading,
    error: fetchError || updateError,
    updateProfile,
    uploadAvatar,
    deleteAvatar,
    refetch,
  };
}