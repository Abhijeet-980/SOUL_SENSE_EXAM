'use client';

import { useEffect } from 'react';
import { initWebVitals } from '@/lib/web-vitals';

/**
 * Web Vitals monitoring component
 * Initializes performance tracking on mount
 */
export function WebVitalsMonitor() {
  useEffect(() => {
    // Only track in production or when explicitly enabled
    const shouldTrack =
      process.env.NEXT_PUBLIC_ENABLE_WEB_VITALS === 'true' ||
      process.env.NODE_ENV === 'production';

    if (!shouldTrack) return;

    // Initialize Web Vitals tracking
    initWebVitals((metric) => {
      // Send to custom analytics endpoint
      if (typeof window !== 'undefined' && navigator.onLine) {
        fetch('/api/v1/analytics/web-vitals', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: metric.name,
            value: metric.value,
            rating: metric.rating,
            timestamp: metric.timestamp,
            url: window.location.href,
            userAgent: navigator.userAgent,
          }),
          keepalive: true,
        }).catch(() => {
          // Silently fail to not disrupt user experience
        });
      }
    });
  }, []);

  return null;
}
