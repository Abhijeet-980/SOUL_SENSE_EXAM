/**
 * Custom React hooks for secure data fetching with built-in error handling
 */

'use client';

import { useState, useEffect, useRef } from 'react';
import { sanitizeError, logError } from '../lib/utils/errorHandler';

export interface UseApiOptions<T> {
  /** Function that returns the API promise */
  apiFn: () => Promise<T>;
  /** Dependencies array (like useEffect) */
  deps?: any[];
  /** Whether to fetch immediately on mount */
  immediate?: boolean;
  /** Called when data is successfully fetched */
  onSuccess?: (data: T) => void;
  /** Called when error occurs */
  onError?: (error: any) => void;
}

export interface UseApiResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => Promise<void>;
}

/**
 * Hook for fetching API data with automatic error handling and memory leak prevention
 */
export function useApi<T>({
  apiFn,
  deps = [],
  immediate = true,
  onSuccess,
  onError,
}: UseApiOptions<T>): UseApiResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(immediate);
  const [error, setError] = useState<string | null>(null);
  const isMountedRef = useRef(false);

  // Track mount status for memory leak prevention
  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  const fetchData = async () => {
    setLoading(true);
    setError(null);

    try {
      const result = await apiFn();

      // Only update state if component is still mounted
      if (isMountedRef.current) {
        setData(result);
        onSuccess?.(result);
      }
    } catch (err) {
      logError(err, 'useApi');

      if (isMountedRef.current) {
        const errorMessage = sanitizeError(err);
        setError(errorMessage);
        onError?.(err);
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  };

  useEffect(() => {
    if (immediate) {
      fetchData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return {
    data,
    loading,
    error,
    refetch: fetchData,
  };
}

/**
 * Hook for monitoring user session and auto-logout on expiration
 */
export function useSession() {
  const [isAuthenticated, setIsAuthenticated] = useState<boolean>(false);
  const [user, setUser] = useState<any>(null);

  useEffect(() => {
    // Import getSession dynamically to avoid circular dependency
    const { getSession, clearSession } = require('../lib/utils/sessionStorage');

    // Check initial auth state from session storage
    const session = getSession();

    setIsAuthenticated(!!session);
    if (session?.user) {
      setUser(session.user);
    }

    // Listen for logout events from other tabs
    const handleStorageChange = (e: StorageEvent) => {
      if (e.key === 'logout-event') {
        clearSession();
        setIsAuthenticated(false);
        setUser(null);
        window.location.href = '/login';
      }

      // Check for session updates
      if (e.key === 'soul_sense_auth_session') {
        const updatedSession = getSession();
        setIsAuthenticated(!!updatedSession);
        setUser(updatedSession?.user || null);
      }
    };

    window.addEventListener('storage', handleStorageChange);

    return () => {
      window.removeEventListener('storage', handleStorageChange);
    };
  }, []);

  const logout = () => {
    const { clearSession } = require('../lib/utils/sessionStorage');
    clearSession();
    localStorage.setItem('logout-event', Date.now().toString());
    setIsAuthenticated(false);
    setUser(null);
    window.location.href = '/login';
  };

  return {
    isAuthenticated,
    user,
    logout,
  };
}

/**
 * Hook to detect online/offline status
 */
export function useOnlineStatus() {
  const [isOnline, setIsOnline] = useState(
    typeof navigator !== 'undefined' ? navigator.onLine : true
  );

  useEffect(() => {
    const handleOnline = () => setIsOnline(true);
    const handleOffline = () => setIsOnline(false);

    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);

    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);

  return isOnline;
}

/**
 * Hook to handle browser back/forward cache (bfcache)
 */
export function usePageVisibility(onVisible?: () => void) {
  useEffect(() => {
    const handlePageShow = (event: PageTransitionEvent) => {
      if (event.persisted) {
        // Page was restored from bfcache
        onVisible?.();
      }
    };

    window.addEventListener('pageshow', handlePageShow);

    return () => {
      window.removeEventListener('pageshow', handlePageShow);
    };
  }, [onVisible]);
}
