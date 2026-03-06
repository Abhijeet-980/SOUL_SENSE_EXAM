import { useState, useEffect, useCallback } from 'react';

interface RateLimitErrorWithDetails {
  detail?: {
    code?: string;
    details?: {
      wait_seconds?: number;
    };
    message?: string;
  };
}

export const useRateLimiter = () => {
  const [lockoutTime, setLockoutTime] = useState<number>(0);

  useEffect(() => {
    let timer: NodeJS.Timeout;
    if (lockoutTime > 0) {
      timer = setInterval(() => {
        setLockoutTime((prev) => {
          if (prev <= 1) {
            clearInterval(timer);
            return 0;
          }
          return prev - 1;
        });
      }, 1000);
    }
    return () => clearInterval(timer);
  }, [lockoutTime]);

  const handleRateLimitError = useCallback(
    (error: any, setErrorCallback?: (message: string) => void) => {
      // Parse possible error structures
      // 1. New GLB_RATE_LIMIT (429) structure
      // 2. AUTH002 (Account Locked) structure

      let waitSeconds = 0;
      let message = 'Too many requests. Please try again later.';

      // Check for standard 429 status or specific error objects
      if (
        error?.status === 429 ||
        error?.detail?.code === 'GLB004' ||
        error?.detail?.code === 'AUTH002'
      ) {
        waitSeconds = error?.detail?.details?.wait_seconds;
        message = error?.detail?.message || message;

        // Fallback if wait_seconds not found but message contains it (legacy/fallback)
        if (!waitSeconds && typeof message === 'string') {
          const match = message.match(/(\d+)s/);
          if (match) waitSeconds = parseInt(match[1]);
        }
      }

      if (waitSeconds > 0) {
        setLockoutTime(waitSeconds);
        if (setErrorCallback) {
          setErrorCallback(`Too many attempts. Please try again in ${waitSeconds}s`);
        }
        return true; // Handled
      }

      return false; // Not a rate limit error
    },
    []
  );

  return {
    lockoutTime,
    setLockoutTime,
    isLocked: lockoutTime > 0,
    handleRateLimitError,
  };
};
