/**
 * Session Storage Utilities
 * =========================
 * Manages session persistence for "Remember Me" functionality.
 * Uses localStorage for persistent sessions and sessionStorage for session-only storage.
 */

const SESSION_KEY = 'soulsense_session';
const REMEMBER_KEY = 'soulsense_remember_me';

export interface SessionData {
    email: string;
    userId?: string;
    token?: string;
    expiresAt?: number;
    loginTime: number;
}

/**
 * Saves session data to browser storage.
 * @param data - Session data to save
 * @param rememberMe - If true, saves to localStorage (persists across browser close)
 *                     If false, saves to sessionStorage (cleared when browser closes)
 */
export function saveSession(data: SessionData, rememberMe: boolean): void {
    const storage = rememberMe ? localStorage : sessionStorage;
    storage.setItem(SESSION_KEY, JSON.stringify(data));

    // Store the rememberMe preference
    if (rememberMe) {
        localStorage.setItem(REMEMBER_KEY, 'true');
    } else {
        localStorage.removeItem(REMEMBER_KEY);
    }
}

/**
 * Retrieves saved session data from storage.
 * Checks localStorage first (for remembered sessions), then sessionStorage.
 */
export function getSession(): SessionData | null {
    // Check localStorage first (persisted sessions)
    const persistedSession = localStorage.getItem(SESSION_KEY);
    if (persistedSession) {
        try {
            return JSON.parse(persistedSession) as SessionData;
        } catch {
            clearSession();
            return null;
        }
    }

    // Check sessionStorage (temporary sessions)
    const tempSession = sessionStorage.getItem(SESSION_KEY);
    if (tempSession) {
        try {
            return JSON.parse(tempSession) as SessionData;
        } catch {
            clearSession();
            return null;
        }
    }

    return null;
}

/**
 * Clears all session data from both storage types.
 */
export function clearSession(): void {
    localStorage.removeItem(SESSION_KEY);
    localStorage.removeItem(REMEMBER_KEY);
    sessionStorage.removeItem(SESSION_KEY);
}

/**
 * Checks if a valid session exists.
 * @param maxAge - Optional maximum session age in milliseconds (default: 30 days)
 */
export function isSessionValid(maxAge: number = 30 * 24 * 60 * 60 * 1000): boolean {
    const session = getSession();
    if (!session) return false;

    // Check if session has custom expiry
    if (session.expiresAt && Date.now() > session.expiresAt) {
        clearSession();
        return false;
    }

    // Check if session is older than maxAge
    if (Date.now() - session.loginTime > maxAge) {
        clearSession();
        return false;
    }

    return true;
}

/**
 * Checks if the user opted to be remembered.
 */
export function wasRemembered(): boolean {
    return localStorage.getItem(REMEMBER_KEY) === 'true';
}
