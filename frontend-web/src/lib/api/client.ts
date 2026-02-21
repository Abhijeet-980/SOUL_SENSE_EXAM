import { ApiError } from './errors';
import { sanitizeError, logError, shouldLogout, isRetryableError } from '../utils/errorHandler';
import { retryRequest } from '../utils/requestUtils';
import { getSession, saveSession } from '../utils/sessionStorage';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:8000/api/v1';

interface RequestOptions extends RequestInit {
  timeout?: number;
  skipAuth?: boolean; // For public endpoints like login
  retry?: boolean; // Enable retry for this request
  maxRetries?: number;
  _isRetry?: boolean; // Internal flag for auth retry
}

/**
 * Get authentication token from storage
 * NOTE: This uses session storage. Production should use httpOnly cookies.
 * See implementation_plan.md for migration guide.
 */
function getAuthToken(): string | null {
  if (typeof window === 'undefined') return null;
  const session = getSession();
  return session?.token || null;
}

/**
 * Handle authentication failure
 */
function handleAuthFailure(): void {
  if (typeof window === 'undefined') return;

  // Clear token
  localStorage.removeItem('token');
  localStorage.removeItem('user');

  // Notify other tabs to logout
  localStorage.setItem('logout-event', Date.now().toString());

  // Redirect to login
  window.location.href = '/login';
}

export async function apiClient<T>(endpoint: string, options: RequestOptions = {}): Promise<T> {
  const {
    timeout = 10000,
    skipAuth = false,
    retry = false,
    maxRetries = 3,
    ...fetchOptions
  } = options;

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);

  const url = endpoint.startsWith('http') ? endpoint : `${API_BASE_URL}${endpoint}`;

  // Inject authentication token
  const token = skipAuth ? null : getAuthToken();
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    ...fetchOptions.headers,
  };

  if (token && !skipAuth) {
    (headers as Record<string, string>)['Authorization'] = `Bearer ${token}`;
  }

  const makeRequest = async (): Promise<T> => {
    try {
      const response = await fetch(url, {
        ...fetchOptions,
        headers,
        signal: controller.signal,
      });

      clearTimeout(id);

      if (!response.ok) {
        let errorData;
        try {
          errorData = await response.json();
        } catch {
          errorData = { message: `HTTP Error ${response.status}: ${response.statusText}` };
        }

        const apiError = new ApiError(response.status, errorData);

        // Handle 401 - Unauthorized
        if (response.status === 401) {
          if (!options._isRetry) {
            try {
              // Internal refresh fetch to avoid circular dependency
              const refreshRes = await fetch(`${API_BASE_URL}/auth/refresh`, {
                method: 'POST',
                // Include cookies for refresh token
                credentials: 'include',
              });

              if (refreshRes.ok) {
                const data = await refreshRes.json();

                // Update session
                const session = getSession();
                if (session) {
                  session.token = data.access_token;
                  if (data.refresh_token) {
                    // Cookie is HTTPOnly
                  }
                  // Persist update (keeping existing storage type)
                  saveSession(session, !!localStorage.getItem('soul_sense_auth_session'));
                }

                // Retry original request with new token
                return apiClient(endpoint, { ...options, _isRetry: true });
              }
            } catch (err) {
              console.error('Token refresh failed:', err);
            }
          }
          handleAuthFailure();
        }

        throw apiError;
      }

      // Handle empty response (204 No Content)
      if (response.status === 204) {
        return {} as T;
      }

      return await response.json();
    } catch (error: any) {
      clearTimeout(id);

      if (error.name === 'AbortError') {
        throw new ApiError(408, {
          message: 'Request timed out. Please check your internet connection.',
          isNetworkError: true,
        });
      }

      if (error instanceof ApiError) {
        throw error;
      }

      // Likely a network error (DNS, Connection Refused, etc.)
      const detailedMessage = `Network Error: [URL: ${url}] | Message: ${error.message || 'Unknown error'}`;
      throw new ApiError(0, {
        message: detailedMessage,
        isNetworkError: true,
        originalError: error.message,
      });
    }
  };

  // Retry if enabled and error is retryable
  if (retry) {
    return retryRequest(makeRequest, maxRetries, 1000, isRetryableError);
  }

  return makeRequest();
}
