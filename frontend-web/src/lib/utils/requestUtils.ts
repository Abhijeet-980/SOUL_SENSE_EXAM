/**
 * Request deduplication and caching utilities
 */

interface CachedRequest {
  promise: Promise<any>;
  timestamp: number;
}

const requestCache = new Map<string, CachedRequest>();
const CACHE_DURATION = 5000; // 5 seconds

/**
 * Deduplicate identical concurrent requests
 * Prevents multiple components from triggering the same API call
 */
export function deduplicateRequest<T>(key: string, requestFn: () => Promise<T>): Promise<T> {
  const now = Date.now();
  const cached = requestCache.get(key);

  // Return cached promise if request is in-flight or recent
  if (cached && now - cached.timestamp < CACHE_DURATION) {
    return cached.promise as Promise<T>;
  }

  // Make new request
  const promise = requestFn().finally(() => {
    // Clean up after cache duration expires
    setTimeout(() => {
      const current = requestCache.get(key);
      if (current && current.timestamp === now) {
        requestCache.delete(key);
      }
    }, CACHE_DURATION);
  });

  requestCache.set(key, { promise, timestamp: now });
  return promise;
}

/**
 * Clear all cached requests (useful for logout)
 */
export function clearRequestCache(): void {
  requestCache.clear();
}

/**
 * Clear specific request from cache
 */
export function invalidateRequest(key: string): void {
  requestCache.delete(key);
}

/**
 * Retry a request with exponential backoff
 */
export async function retryRequest<T>(
  requestFn: () => Promise<T>,
  maxRetries: number = 3,
  initialBackoff: number = 1000,
  shouldRetry: (error: any) => boolean = () => true
): Promise<T> {
  let lastError: any;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await requestFn();
    } catch (error) {
      lastError = error;

      // Don't retry if shouldn't retry or out of attempts
      if (!shouldRetry(error) || attempt === maxRetries) {
        throw error;
      }

      // Exponential backoff: 1s, 2s, 4s, 8s...
      const backoff = initialBackoff * Math.pow(2, attempt);
      await new Promise((resolve) => setTimeout(resolve, backoff));
    }
  }

  throw lastError;
}

/**
 * Debounce function to prevent rapid repeated calls
 */
export function debounce<T extends (...args: any[]) => any>(
  func: T,
  waitMs: number
): (...args: Parameters<T>) => void {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  return function (...args: Parameters<T>) {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }

    timeoutId = setTimeout(() => {
      func(...args);
    }, waitMs);
  };
}

/**
 * Throttle function to limit execution rate
 */
export function throttle<T extends (...args: any[]) => any>(
  func: T,
  limitMs: number
): (...args: Parameters<T>) => void {
  let inThrottle = false;

  return function (...args: Parameters<T>) {
    if (!inThrottle) {
      func(...args);
      inThrottle = true;
      setTimeout(() => {
        inThrottle = false;
      }, limitMs);
    }
  };
}
