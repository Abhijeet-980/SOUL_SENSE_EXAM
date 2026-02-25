'use client';

import React, {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  ReactNode,
} from 'react';
import { useRouter, usePathname } from 'next/navigation';
import {
  UserSession,
  getSession,
  saveSession,
  clearSession,
  getExpiryTimestamp,
} from '@/lib/utils/sessionStorage';
import { authApi } from '@/lib/api/auth';
import { Loader } from '@/components/ui';
import { isValidCallbackUrl } from '@/lib/utils/url';
import { toast } from '@/lib/toast';

interface AuthContextType {
  user: UserSession['user'] | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  isMockMode: boolean;
  login: (
    data: {
      username: string;
      password: string;
      captcha_input?: string;
      session_id?: string;
    },
    rememberMe: boolean,
    shouldRedirect?: boolean,
    redirectTo?: string,
    stayLoadingOnSuccess?: boolean
  ) => Promise<any>;
  login2FA: (
    data: { pre_auth_token: string; code: string },
    rememberMe: boolean,
    shouldRedirect?: boolean,
    redirectTo?: string,
    stayLoadingOnSuccess?: boolean
  ) => Promise<any>;
  logout: () => void;
  setIsLoading: (loading: boolean) => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider = ({ children }: { children: ReactNode }) => {
  const [user, setUser] = useState<UserSession['user'] | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isMockMode, setIsMockMode] = useState(false);
  const router = useRouter();
  const pathname = usePathname();

  const [mounted, setMounted] = useState(false);

  // Clear lingering global loading state when transitioning away from auth pages
  useEffect(() => {
    if (mounted && isLoading && !!user && pathname !== '/login' && pathname !== '/register') {
      setIsLoading(false);
    }
  }, [pathname, isLoading, user, mounted]);

  useEffect(() => {
    setMounted(true);
    const initAuth = async () => {
      try {
        // 1. Check if server has restarted
        await checkServerInstance();

        // 2. Check for existing session
        const session = getSession();
        if (session) {
          // Critical: Verify the session isn't using the stale 'current' fallback
          if (session.user.id === 'current') {
            console.error(
              'Critical Auth Sync Error: Stale "current" ID fallback found in stored session.'
            );
            toast.error('Authentication session corrupted. Please log in again.');
            clearSession();
            setUser(null);
            router.push('/login');
          } else {
            setUser(session.user);
          }
        }

        // 3. Check if backend is in mock mode
        await checkMockMode();
      } catch (e) {
        console.warn('Auth initialization error:', e);
      } finally {
        // Small delay to ensure state propagates
        const timer = setTimeout(() => setIsLoading(false), 50);
        return () => clearTimeout(timer);
      }
    };

    initAuth();
  }, []);

  const checkServerInstance = async () => {
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
      const response = await fetch(`${apiUrl}/auth/server-id`, {
        method: 'GET',
      });

      if (response.ok) {
        const { server_id } = await response.json();
        const storedId = localStorage.getItem('soul_sense_server_instance_id');

        if (storedId && server_id && storedId !== server_id) {
          console.log('🔄 Server restart detected. Clearing stale session.');
          clearSession();
          setUser(null);
        }

        if (server_id) {
          localStorage.setItem('soul_sense_server_instance_id', server_id);
        }
      }
    } catch (error) {
      console.warn('Could not verify server instance:', error);
    }
  };

  const checkMockMode = async () => {
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
      // Health check is usually at the API root or base URL
      const response = await fetch(`${apiUrl.replace(/\/api\/v1\/?$/, '')}/health`, {
        method: 'GET',
      });

      if (response.ok) {
        const data = await response.json();
        setIsMockMode(data.mock_auth_mode || false);
      }
    } catch (error) {
      console.warn('Could not check mock mode status:', error);
    }
  };

  const login = async (
    loginData: {
      username: string;
      password: string;
      captcha_input?: string;
      session_id?: string;
    },
    rememberMe: boolean,
    shouldRedirect = true,
    redirectTo = '/',
    stayLoadingOnSuccess = false
  ) => {
    setIsLoading(true);
    try {
      const result = await authApi.login(loginData);

      if (result.pre_auth_token) {
        return result; // 2FA Required
      }

      if (!result.id) {
        console.error(
          'Critical Auth Sync Error: Valid session established but User ID missing from API payload.'
        );
        throw new Error('Invalid session state: Missing user identifier.');
      }

      const session: UserSession = {
        user: {
          id: result.id.toString(),
          email: (result.email ||
            (loginData.username.includes('@') ? loginData.username : '')) as string,
          name: result.username || loginData.username.split('@')[0],
          username: result.username,
          created_at: result.created_at,
        },
        token: result.access_token,
        expiresAt: getExpiryTimestamp(),
      };

      saveSession(session, rememberMe);
      setUser(session.user);

      if (shouldRedirect) {
        const finalRedirect = isValidCallbackUrl(redirectTo) ? redirectTo : '/';
        console.log(`useAuth: Navigation to ${finalRedirect} triggered`);
        router.push(finalRedirect);
      }

      // If we are redirecting and want to stay loading, we don't clear it here
      if (stayLoadingOnSuccess) return result;

      setIsLoading(false);
      return result;
    } catch (error) {
      setIsLoading(false);
      console.error('Login failed:', error);
      toast.error('Login failed. Please check your credentials and try again.');
      throw error;
    }
  };

  const login2FA = async (
    data: { pre_auth_token: string; code: string },
    rememberMe: boolean,
    shouldRedirect = true,
    redirectTo = '/',
    stayLoadingOnSuccess = false
  ) => {
    setIsLoading(true);
    try {
      const result = await authApi.login2FA(data);

      if (!result.id) {
        console.error(
          'Critical Auth Sync Error: Valid session established but User ID missing from API payload.'
        );
        throw new Error('Invalid session state: Missing user identifier.');
      }

      const session: UserSession = {
        user: {
          id: result.id.toString(),
          email: (result.email || '') as string,
          name: result.username || 'User',
          username: result.username,
          created_at: result.created_at,
        },
        token: result.access_token,
        expiresAt: getExpiryTimestamp(),
      };

      saveSession(session, rememberMe);
      setUser(session.user);

      if (shouldRedirect) {
        const finalRedirect = isValidCallbackUrl(redirectTo) ? redirectTo : '/';
        router.push(finalRedirect);
      }

      if (stayLoadingOnSuccess) return result;

      setIsLoading(false);
      return result;
    } catch (error) {
      console.error('2FA verification failed:', error);
      throw error;
    } finally {
      if (!stayLoadingOnSuccess) {
        setIsLoading(false);
      }
    }
  };

  const logout = useCallback(async () => {
    try {
      await authApi.logout();
    } catch (error) {
      console.error('Logout error:', error);
      toast.error('Logout failed. Your session may still be active on the server.');
    } finally {
      // Always clear local session even if backend call fails
      clearSession();
      setUser(null);
      router.push('/login');
    }
  }, [router]);

  // Listen for auth-failure events from API client
  useEffect(() => {
    const handleAuthFailure = () => {
      logout();
    };

    window.addEventListener('auth-failure', handleAuthFailure);

    return () => {
      window.removeEventListener('auth-failure', handleAuthFailure);
    };
  }, [logout]);

  // ... existing code ...

  return (
    <AuthContext.Provider
      value={{
        user,
        isAuthenticated: !!user,
        isLoading: !mounted || isLoading,
        isMockMode,
        login,
        login2FA,
        logout,
        setIsLoading,
      }}
    >
      {/* Always render children for Next.js router hydration, overlay loader if needed */}
      {(!mounted || isLoading) && (
        <Loader fullScreen text={!mounted ? 'Bootstrapping...' : 'Authenticating...'} />
      )}
      <div style={{ display: !mounted || isLoading ? 'none' : 'block', height: '100%' }}>
        {children}
      </div>
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
