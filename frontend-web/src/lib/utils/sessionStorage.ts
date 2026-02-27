/**
 * Session storage utilities for handling user persistence
 */

const SESSION_KEY = 'soul_sense_auth_session';
const SESSION_EXPIRY_DAYS = 30;

export interface UserSession {
  user: {
    id: string;
    email: string;
    username?: string;
    name?: string;
    created_at?: string;
    onboarding_completed?: boolean;
  };
  token: string;
  expiresAt: number;
}

/**
 * Lightweight JWT parser to check if a token is expired
 * Does not require external libraries to stay edge-runtime compatible
 */
export const isTokenExpired = (token: string): boolean => {
  if (!token) return true;
  try {
    let payloadBase64 = token.split('.')[1];
    if (!payloadBase64) return true;

    // Convert base64url to base64
    payloadBase64 = payloadBase64.replace(/-/g, '+').replace(/_/g, '/');

    // Use atob for browser-native base64 decoding
    const decodedPayload = JSON.parse(atob(payloadBase64));
    const currentTime = Math.floor(Date.now() / 1000);

    // Return true if current time is past the exp claim
    return decodedPayload.exp < currentTime;
  } catch (e) {
    console.warn('Failed to parse JWT for expiry check:', e);
    return true; // Malformed tokens are considered fundamentally expired
  }
};

/**
 * Save session to storage
 * @param session User session data
 * @param rememberMe Whether to use localStorage (persistent) or sessionStorage (per-tab)
 */
export const saveSession = (session: UserSession, rememberMe: boolean): void => {
  const data = JSON.stringify(session);
  if (rememberMe) {
    localStorage.setItem(SESSION_KEY, data);
    sessionStorage.removeItem(SESSION_KEY); // Clear duplicate
  } else {
    sessionStorage.setItem(SESSION_KEY, data);
    localStorage.removeItem(SESSION_KEY); // Clear duplicate
  }
};

/**
 * Get session from storage
 * Checks both localStorage and sessionStorage
 */
export const getSession = (): UserSession | null => {
  const localData = localStorage.getItem(SESSION_KEY);
  const sessionData = sessionStorage.getItem(SESSION_KEY);

  const data = localData || sessionData;
  if (!data) return null;

  try {
    const session: UserSession = JSON.parse(data);

    // Validate expiry
    if (Date.now() > session.expiresAt) {
      clearSession();
      return null;
    }

    return session;
  } catch (error) {
    console.error('Failed to parse session data:', error);
    clearSession();
    return null;
  }
};

/**
 * Clear session from both storage types
 */
export const clearSession = (): void => {
  localStorage.removeItem(SESSION_KEY);
  sessionStorage.removeItem(SESSION_KEY);
};

/**
 * Calculate expiry timestamp
 */
export const getExpiryTimestamp = (): number => {
  const now = new Date();
  now.setDate(now.getDate() + SESSION_EXPIRY_DAYS);
  return now.getTime();
};
