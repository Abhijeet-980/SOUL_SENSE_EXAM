'use client';

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { useRouter } from 'next/navigation';
import {
  UserSession,
  getSession,
  saveSession,
  clearSession,
  getExpiryTimestamp,
} from '@/lib/utils/sessionStorage';
import { authApi } from '@/lib/api/auth';
import { Loader } from '@/components/ui';

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
    redirectTo?: string
  ) => Promise<any>;
  login2FA: (
    data: { pre_auth_token: string; code: string },
    rememberMe: boolean,
    shouldRedirect?: boolean,
    redirectTo?: string
  ) => Promise<any>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider = ({ children }: { children: ReactNode }) => {
  const [user, setUser] = useState<UserSession['user'] | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isMockMode, setIsMockMode] = useState(false);
  const router = useRouter();

  useEffect(() => {
    // Check for existing session on mount
    const session = getSession();
    if (session) {
      console.log('Session restored:', session.user.email);
      setUser(session.user);
    }

    // Check if backend is in mock mode (from main)
    checkMockMode();

    // Small delay to ensure state propagates before layout checks
    const timer = setTimeout(() => setIsLoading(false), 50);
    return () => clearTimeout(timer);
  }, []);

  const checkMockMode = async () => {
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
      const response = await fetch(`${apiUrl}/api/v1/health`, {
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
    redirectTo = '/dashboard'
  ) => {
    setIsLoading(true);
    try {
      const result = await authApi.login(loginData);

      if (result.pre_auth_token) {
        return result; // 2FA Required
      }

      const session: UserSession = {
        user: {
          id: 'current',
          email: (result.email ||
            (loginData.username.includes('@') ? loginData.username : '')) as string,
          name: result.username || loginData.username.split('@')[0],
        },
        token: result.access_token,
        expiresAt: getExpiryTimestamp(),
      };

      saveSession(session, rememberMe);
      setUser(session.user);

      if (shouldRedirect) {
        console.log(`useAuth: Navigation to ${redirectTo} triggered`);
        router.push(redirectTo);
      }

      return result;
    } catch (error) {
      console.error('Login failed:', error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  const login2FA = async (
    data: { pre_auth_token: string; code: string },
    rememberMe: boolean,
    shouldRedirect = true,
    redirectTo = '/dashboard'
  ) => {
    setIsLoading(true);
    try {
      const result = await authApi.login2FA(data);

      const session: UserSession = {
        user: {
          id: 'current',
          email: (result.email || '') as string,
          name: result.username || 'User',
        },
        token: result.access_token,
        expiresAt: getExpiryTimestamp(),
      };

      saveSession(session, rememberMe);
      setUser(session.user);

      if (shouldRedirect) {
        router.push(redirectTo);
      }

      return result;
    } catch (error) {
      console.error('2FA verification failed:', error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  const logout = async () => {
    // Integrate logout fetch from main
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000').replace(
        /\/api\/v1\/?$/,
        ''
      );
      await fetch(`${apiUrl}/api/v1/auth/logout`, {
        method: 'POST',
        credentials: 'include',
      });
    } catch (error) {
      console.error('Logout error:', error);
    }

    clearSession();
    setUser(null);
    router.push('/login');
  };

  // ... existing code ...

  return (
    <AuthContext.Provider
      value={{
        user,
        isAuthenticated: !!user,
        isLoading,
        isMockMode,
        login,
        login2FA,
        logout,
      }}
    >
      {isLoading ? <Loader fullScreen text="Authenticating..." /> : children}
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
