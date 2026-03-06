/**
 * Error handling utilities for safe, user-friendly error messages
 */

import { ApiError } from '../api/errors';

/**
 * Sanitize errors to prevent sensitive information leakage
 * Never expose stack traces, tokens, or internal details to users
 */
export function sanitizeError(error: any): string {
  // Development mode - show more details
  if (process.env.NODE_ENV === 'development') {
    if (error instanceof ApiError) {
      return error.data?.message || `API Error ${error.status}`;
    }
    return error.message || 'Unknown error';
  }

  // Production mode - generic, safe messages only
  if (error instanceof ApiError) {
    switch (error.status) {
      case 400:
        return 'Invalid request. Please check your input and try again.';
      case 401:
        return 'Please log in to continue.';
      case 403:
        return 'You do not have permission to access this resource.';
      case 404:
        return 'The requested resource was not found.';
      case 408:
        return 'Request timed out. Please check your connection and try again.';
      case 429:
        return 'Too many requests. Please wait a moment and try again.';
      case 500:
      case 502:
      case 503:
        return 'Server error. Please try again later.';
      default:
        if (error.status >= 500) {
          return 'Server error. Please try again later.';
        }
        return 'Something went wrong. Please try again.';
    }
  }

  // Network errors
  if (error.name === 'AbortError' || error.message?.includes('network')) {
    return 'Connection failed. Please check your internet and try again.';
  }

  // Generic fallback
  return 'Something went wrong. Please try again.';
}

/**
 * Log errors safely (only in development)
 */
export function logError(error: any, context?: string): void {
  if (process.env.NODE_ENV === 'development') {
    console.error(`[Error${context ? ` - ${context}` : ''}]:`, error);
  }

  // In production, you would send to error tracking service (Sentry, etc.)
  // But never log sensitive data like tokens or user PII
}

/**
 * Check if error should trigger logout
 */
export function shouldLogout(error: any): boolean {
  if (error instanceof ApiError) {
    return error.status === 401;
  }
  return false;
}

/**
 * Get retry-able status
 */
export function isRetryableError(error: any): boolean {
  if (error instanceof ApiError) {
    // Don't retry client errors (4xx except 408, 429)
    if (error.status >= 400 && error.status < 500) {
      return error.status === 408 || error.status === 429;
    }
    // Retry server errors (5xx)
    return error.status >= 500;
  }

  // Retry network errors
  return error.name === 'AbortError' || error.message?.includes('network');
}
