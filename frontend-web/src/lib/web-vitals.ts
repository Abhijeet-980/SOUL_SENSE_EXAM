/**
 * Web Vitals monitoring utility
 * Tracks Core Web Vitals metrics: FCP, LCP, CLS, FID/INP, TTFB
 */

export interface Metric {
  name: string;
  value: number;
  rating: 'good' | 'needs-improvement' | 'poor';
  timestamp: number;
}

export interface WebVitalsReport {
  fcp?: Metric;
  lcp?: Metric;
  cls?: Metric;
  fid?: Metric;
  inp?: Metric;
  ttfb?: Metric;
  tti?: Metric;
}

// Thresholds for Core Web Vitals
const THRESHOLDS = {
  FCP: { good: 1800, poor: 3000 },
  LCP: { good: 2500, poor: 4000 },
  CLS: { good: 0.1, poor: 0.25 },
  FID: { good: 100, poor: 300 },
  INP: { good: 200, poor: 500 },
  TTFB: { good: 800, poor: 1800 },
  TTI: { good: 3000, poor: 5000 },
};

function getRating(name: string, value: number): 'good' | 'needs-improvement' | 'poor' {
  const threshold = THRESHOLDS[name as keyof typeof THRESHOLDS];
  if (!threshold) return 'good';

  if (value <= threshold.good) return 'good';
  if (value <= threshold.poor) return 'needs-improvement';
  return 'poor';
}

function logMetric(metric: Metric) {
  const rating = metric.rating;
  const emoji = rating === 'good' ? '✓' : rating === 'needs-improvement' ? '⚠' : '✗';

  console.log(
    `[Web Vitals] ${emoji} ${metric.name}: ${metric.value.toFixed(0)}ms (${rating})`
  );

  // Send to analytics service if configured
  if (typeof window !== 'undefined' && (window as any).gtag) {
    (window as any).gtag('event', metric.name, {
      event_category: 'Web Vitals',
      event_label: metric.rating,
      value: Math.round(metric.name === 'CLS' ? metric.value * 1000 : metric.value),
      non_interaction: true,
    });
  }
}

/**
 * Measure First Contentful Paint
 */
function measureFCP() {
  if (typeof window === 'undefined' || !('PerformanceObserver' in window)) return;

  const observer = new PerformanceObserver((list) => {
    const entries = list.getEntries();
    const fcpEntry = entries.find((entry) => entry.name === 'first-contentful-paint');

    if (fcpEntry) {
      const metric: Metric = {
        name: 'FCP',
        value: fcpEntry.startTime,
        rating: getRating('FCP', fcpEntry.startTime),
        timestamp: Date.now(),
      };
      logMetric(metric);
      observer.disconnect();
    }
  });

  try {
    observer.observe({ entryTypes: ['paint'] });
  } catch (e) {
    console.warn('FCP measurement not supported');
  }
}

/**
 * Measure Largest Contentful Paint
 */
function measureLCP(onMetric: (metric: Metric) => void) {
  if (typeof window === 'undefined' || !('PerformanceObserver' in window)) return;

  let lcpValue = 0;

  const observer = new PerformanceObserver((list) => {
    const entries = list.getEntries();
    const lastEntry = entries[entries.length - 1] as any;

    lcpValue = lastEntry.renderTime || lastEntry.loadTime;

    const metric: Metric = {
      name: 'LCP',
      value: lcpValue,
      rating: getRating('LCP', lcpValue),
      timestamp: Date.now(),
    };

    onMetric(metric);
  });

  try {
    observer.observe({ entryTypes: ['largest-contentful-paint'] });
  } catch (e) {
    console.warn('LCP measurement not supported');
  }

  // Stop observing after page load
  if (typeof document !== 'undefined') {
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'hidden') {
        observer.takeRecords();
        observer.disconnect();
      }
    });
  }
}

/**
 * Measure Cumulative Layout Shift
 */
function measureCLS(onMetric: (metric: Metric) => void) {
  if (typeof window === 'undefined' || !('PerformanceObserver' in window)) return;

  let clsValue = 0;
  let sessionValue = 0;
  let sessionEntries: any[] = [];

  const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries() as any[]) {
      if (!entry.hadRecentInput) {
        sessionValue += entry.value;
        sessionEntries.push(entry);

        if (sessionValue > clsValue) {
          clsValue = sessionValue;

          const metric: Metric = {
            name: 'CLS',
            value: clsValue,
            rating: getRating('CLS', clsValue),
            timestamp: Date.now(),
          };

          onMetric(metric);
        }
      }
    }
  });

  try {
    observer.observe({ entryTypes: ['layout-shift'] });
  } catch (e) {
    console.warn('CLS measurement not supported');
  }

  // Reset session value on user interaction
  if (typeof document !== 'undefined') {
    document.addEventListener('click', () => {
      sessionValue = 0;
      sessionEntries = [];
    }, { once: true });
  }
}

/**
 * Measure First Input Delay
 */
function measureFID(onMetric: (metric: Metric) => void) {
  if (typeof window === 'undefined' || !('PerformanceObserver' in window)) return;

  const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries() as any[]) {
      const metric: Metric = {
        name: 'FID',
        value: entry.processingStart - entry.startTime,
        rating: getRating('FID', entry.processingStart - entry.startTime),
        timestamp: Date.now(),
      };

      logMetric(metric);
      onMetric(metric);
      observer.disconnect();
    }
  });

  try {
    observer.observe({ entryTypes: ['first-input'] });
  } catch (e) {
    console.warn('FID measurement not supported');
  }
}

/**
 * Measure Interaction to Next Paint
 */
function measureINP(onMetric: (metric: Metric) => void) {
  if (typeof window === 'undefined' || !('PerformanceObserver' in window)) return;

  let inpValue = Infinity;
  let entries: any[] = [];

  const observer = new PerformanceObserver((list) => {
    entries = [...entries, ...list.getEntries()];

    for (const entry of entries as any[]) {
      if (entry.interactionId) {
        const processingDuration = entry.processingEnd - entry.startTime;
        inpValue = Math.min(inpValue, processingDuration);

        const metric: Metric = {
          name: 'INP',
          value: inpValue,
          rating: getRating('INP', inpValue),
          timestamp: Date.now(),
        };

        onMetric(metric);
      }
    }
  });

  try {
    observer.observe({ entryTypes: ['event'] });
  } catch (e) {
    console.warn('INP measurement not supported');
  }
}

/**
 * Measure Time to First Byte
 */
function measureTTFB() {
  if (typeof window === 'undefined' || !window.performance) return;

  const navigation = performance.getEntriesByType('navigation')[0] as any;

  if (navigation && navigation.responseStart) {
    const ttfb = navigation.responseStart;

    const metric: Metric = {
      name: 'TTFB',
      value: ttfb,
      rating: getRating('TTFB', ttfb),
      timestamp: Date.now(),
    };

    logMetric(metric);
  }
}

/**
 * Estimate Time to Interactive
 */
function measureTTI(onMetric: (metric: Metric) => void) {
  if (typeof window === 'undefined' || !('PerformanceObserver' in window)) return;

  // Use FCP as a fallback for TTI
  const observer = new PerformanceObserver((list) => {
    const entries = list.getEntries();
    const fcpEntry = entries.find((entry) => entry.name === 'first-contentful-paint');

    if (fcpEntry) {
      // Estimate TTI as FCP + 1s (conservative estimate)
      const estimatedTTI = fcpEntry.startTime + 1000;

      const metric: Metric = {
        name: 'TTI',
        value: estimatedTTI,
        rating: getRating('TTI', estimatedTTI),
        timestamp: Date.now(),
      };

      onMetric(metric);
      observer.disconnect();
    }
  });

  try {
    observer.observe({ entryTypes: ['paint'] });
  } catch (e) {
    console.warn('TTI estimation not supported');
  }
}

/**
 * Initialize all Web Vitals measurements
 */
export function initWebVitals(onMetric?: (metric: Metric) => void): WebVitalsReport {
  const report: WebVitalsReport = {};

  const metricCallback = onMetric || logMetric;

  // Run immediately
  measureTTFB();
  measureFCP();

  // Run with callback
  measureLCP((metric) => {
    report.lcp = metric;
    metricCallback(metric);
  });

  measureCLS((metric) => {
    report.cls = metric;
    metricCallback(metric);
  });

  measureFID((metric) => {
    report.fid = metric;
    metricCallback(metric);
  });

  measureINP((metric) => {
    report.inp = metric;
    metricCallback(metric);
  });

  measureTTI((metric) => {
    report.tti = metric;
    metricCallback(metric);
  });

  return report;
}

/**
 * Get performance summary for analytics
 */
export function getPerformanceSummary(): any {
  if (typeof window === 'undefined' || !window.performance) {
    return null;
  }

  const navigation = performance.getEntriesByType('navigation')[0] as any;
  const paint = performance.getEntriesByType('paint');

  return {
    navigation: {
      domContentLoaded: navigation?.domContentLoadedEventEnd - navigation?.domContentLoadedEventStart,
      loadComplete: navigation?.loadEventEnd - navigation?.loadEventStart,
      domInteractive: navigation?.domInteractive,
    },
    paint: paint.reduce((acc: any, entry: any) => {
      acc[entry.name.replace(/-/g, '')] = entry.startTime;
      return acc;
    }, {}),
    memory: (performance as any).memory ? {
      usedJSHeapSize: (performance as any).memory.usedJSHeapSize,
      totalJSHeapSize: (performance as any).memory.totalJSHeapSize,
    } : null,
  };
}
