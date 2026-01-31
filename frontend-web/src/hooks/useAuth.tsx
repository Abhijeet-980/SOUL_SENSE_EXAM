'use client';

import React, { createContext, useContext, useState, useEffect, useCallback, ReactNode } from 'react';
import {
    saveSession,
    getSession,
    clearSession,
    isSessionValid,
    SessionData
} from '@/lib/utils/sessionStorage';

interface User {
    email: string;
    userId?: string;
}

interface AuthContextType {
    user: User | null;
    isAuthenticated: boolean;
    isLoading: boolean;
    login: (email: string, password: string, rememberMe: boolean) => Promise<boolean>;
    logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

interface AuthProviderProps {
    children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
    const [user, setUser] = useState<User | null>(null);
    const [isLoading, setIsLoading] = useState(true);

    // Check for existing session on mount
    useEffect(() => {
        const checkSession = () => {
            if (isSessionValid()) {
                const session = getSession();
                if (session) {
                    setUser({
                        email: session.email,
                        userId: session.userId
                    });
                }
            }
            setIsLoading(false);
        };

        checkSession();
    }, []);

    const login = useCallback(async (email: string, password: string, rememberMe: boolean): Promise<boolean> => {
        try {
            // TODO: Replace with actual API call when backend is ready
            // Simulate API call
            await new Promise((resolve) => setTimeout(resolve, 1500));

            // For demo purposes, any valid email/password combination works
            // In production, this would validate against a real API
            console.log('Login attempt:', { email, rememberMe });

            // Create session data
            const sessionData: SessionData = {
                email,
                userId: `user_${Date.now()}`, // Mock user ID
                loginTime: Date.now(),
            };

            // Save session based on rememberMe preference
            saveSession(sessionData, rememberMe);

            // Update state
            setUser({ email, userId: sessionData.userId });

            return true;
        } catch (error) {
            console.error('Login error:', error);
            return false;
        }
    }, []);

    const logout = useCallback(() => {
        clearSession();
        setUser(null);
    }, []);

    const value: AuthContextType = {
        user,
        isAuthenticated: !!user,
        isLoading,
        login,
        logout,
    };

    return (
        <AuthContext.Provider value={value}>
            {children}
        </AuthContext.Provider>
    );
}

export function useAuth(): AuthContextType {
    const context = useContext(AuthContext);
    if (context === undefined) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
}
