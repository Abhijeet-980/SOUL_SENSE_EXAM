'use client';

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { useRouter } from 'next/navigation';
import {
    UserSession,
    getSession,
    saveSession,
    clearSession,
    getExpiryTimestamp
} from '@/lib/utils/sessionStorage';

interface AuthContextType {
    user: UserSession['user'] | null;
    isAuthenticated: boolean;
    isLoading: boolean;
    isMockMode: boolean;
    login: (email: string, password: string, rememberMe: boolean) => Promise<void>;
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
            setUser(session.user);
        }
        
        // Check if backend is in mock mode
        checkMockMode();
        
        setIsLoading(false);
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

    const login = async (email: string, password: string, rememberMe: boolean) => {
        setIsLoading(true);
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
            
            // Try real API login first
            const formData = new URLSearchParams();
            formData.append('username', email);
            formData.append('password', password);
            
            const response = await fetch(`${apiUrl}/api/v1/auth/login`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                body: formData,
            });

            if (response.ok) {
                const data = await response.json();
                
                // Check if 2FA is required
                if (response.status === 202) {
                    // Handle 2FA flow
                    console.log('2FA required');
                    // TODO: Implement 2FA UI flow
                    return;
                }
                
                const mockUser = {
                    id: '1',
                    email,
                    name: email.split('@')[0],
                };

                const session: UserSession = {
                    user: mockUser,
                    token: data.access_token,
                    expiresAt: getExpiryTimestamp(),
                };

                saveSession(session, rememberMe);
                setUser(mockUser);
                router.push('/dashboard');
            } else {
                const error = await response.json();
                throw new Error(error.detail || 'Login failed');
            }
        } catch (error) {
            console.error('Login failed:', error);
            throw error;
        } finally {
            setIsLoading(false);
        }
    };

    const logout = async () => {
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
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

    return (
        <AuthContext.Provider value={{
            user,
            isAuthenticated: !!user,
            isLoading,
            isMockMode,
            login,
            logout
        }}>
            {!isLoading && children}
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
