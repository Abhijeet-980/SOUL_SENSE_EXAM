# Performance Optimization Guide

This guide documents the performance optimization strategies implemented in the Soul Sense EQ Test platform.

## Table of Contents

- [Overview](#overview)
- [Frontend Optimizations](#frontend-optimizations)
- [Backend Optimizations](#backend-optimizations)
- [Performance Monitoring](#performance-monitoring)
- [Performance Budgets](#performance-budgets)
- [Best Practices](#best-practices)

---

## Overview

The performance optimization implementation focuses on:

1. **Frontend**: Code splitting, lazy loading, bundle optimization
2. **Backend**: Response compression, query optimization, caching
3. **Monitoring**: Web Vitals tracking, API response time logging

Target metrics:
- Initial bundle size: < 200KB
- First Contentful Paint (FCP): < 1.5s
- Time to Interactive (TTI): < 3s
- API response time (p95): < 500ms
- Database query time: < 100ms

---

## Frontend Optimizations

### Dynamic Imports for Heavy Components

Heavy components are lazy-loaded using Next.js dynamic imports to reduce initial bundle size.

**Location**: `/frontend-web/src/lib/dynamic-imports.ts`

**Optimized Components**:
- Chart components (Recharts)
- PDF export (jsPDF, html2canvas)
- Force-directed graphs
- History charts and gauges

**Usage Example**:

```typescript
import { ActivityAreaChart, ExportPDF } from '@/lib/dynamic-imports';

// Components are loaded on-demand with loading states
<ActivityAreaChart data={chartData} />
<ExportPDF resultId={123} />
```

### Web Vitals Monitoring

Real-time Core Web Vitals tracking integrated into the application.

**Location**: `/frontend-web/src/lib/web-vitals.ts`

**Metrics Tracked**:
- **FCP** (First Contentful Paint): Time to first content render
- **LCP** (Largest Contentful Paint): Time to largest element render
- **CLS** (Cumulative Layout Shift): Visual stability score
- **FID** (First Input Delay): Interactivity responsiveness
- **INP** (Interaction to Next Paint): Overall interactivity
- **TTFB** (Time to First Byte): Server response time
- **TTI** (Time to Interactive): Estimated interactive time

**Usage**:

```typescript
import { initWebVitals } from '@/lib/web-vitals';

// Initialize with callback
initWebVitals((metric) => {
  console.log(`${metric.name}: ${metric.value}ms (${metric.rating})`);

  // Send to analytics
  fetch('/api/v1/analytics/web-vitals', {
    method: 'POST',
    body: JSON.stringify(metric),
  });
});
```

**Monitoring Component**:

The `WebVitalsMonitor` component automatically tracks metrics in production:

```tsx
import { WebVitalsMonitor } from '@/components/monitoring/WebVitalsMonitor';

// Added in root layout
<WebVitalsMonitor />
```

### Next.js Configuration Optimizations

**Location**: `/frontend-web/next.config.js`

**Optimizations**:
- SWC minification enabled
- Console removal in production
- CSS optimization
- Static asset caching headers
- Font display optimization (swap)

```javascript
module.exports = {
  swcMinify: true,
  compiler: {
    removeConsole: process.env.NODE_ENV === 'production',
  },
  experimental: {
    optimizeCss: true,
  },
  // Cache headers for static assets
  async headers() {
    return [
      {
        source: '/:all*(svg|jpg|jpeg|png|ico|webp)',
        headers: [{
          key: 'Cache-Control',
          value: 'public, max-age=31536000, immutable',
        }],
      },
    ];
  },
};
```

### Font Optimization

Fonts use `display: 'swap'` to prevent FOIT (Flash of Invisible Text):

```typescript
const inter = Inter({
  subsets: ['latin'],
  variable: '--font-sans',
  display: 'swap',  // Show fallback font immediately
});
```

---

## Backend Optimizations

### GZip Compression Middleware

All API responses are compressed using GZip middleware.

**Location**: `/backend/fastapi/api/main.py`

```python
from fastapi.middleware.gzip import GZipMiddleware

app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=6)
```

**Configuration**:
- `minimum_size=1000`: Only compress responses > 1KB
- `compresslevel=6`: Balance between speed and compression ratio

### Performance Monitoring Middleware

Tracks API response times and logs slow requests.

**Location**: `/backend/fastapi/api/main.py`

```python
class PerformanceMonitoringMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000

        # Add performance header
        response.headers["X-Process-Time"] = f"{process_time:.2f}ms"

        # Log slow requests
        if process_time > 500:
            logger.warning(f"Slow request: {request.url.path} took {process_time:.2f}ms")

        return response
```

### Caching Utilities

Simple in-memory cache with TTL for expensive operations.

**Location**: `/backend/fastapi/api/utils/performance.py`

**Usage**:

```python
from api.utils.performance import cached, SimpleCache

# Using decorator
@cached(ttl=300, key_prefix="questions")
def get_questions(age: int):
    # Expensive database query
    return query_questions(age)

# Using cache directly
cache = SimpleCache()
cache.set("key", data, ttl=300)
data = cache.get("key")
```

### Query Optimization

Pagination helper to reduce database load:

```python
from api.utils.performance import QueryOptimizer

items, total_count, page_info = QueryOptimizer.paginate(
    query,
    page=1,
    page_size=20
)
```

Field selection optimization:

```python
query = QueryOptimizer.optimize_select_fields(
    query,
    User,
    fields=['id', 'username', 'email']  # Only select needed fields
)
```

### Web Vitals Analytics Endpoint

Accepts Web Vitals metrics from frontend.

**Location**: `/backend/fastapi/api/routers/analytics.py`

```python
@router.post("/analytics/web-vitals")
async def track_web_vitals(metric: WebVitalsMetric):
    logger.info(f"Web Vitals - {metric.name}: {metric.value:.2f}ms")
    return {"status": "success"}
```

---

## Performance Monitoring

### Frontend Monitoring

1. **Web Vitals**: Tracked automatically in production
2. **Performance Summary**: Navigation timing, paint timings
3. **Memory Usage**: JS heap size tracking

### Backend Monitoring

1. **API Response Times**: Logged for every request
2. **Slow Request Alerts**: Requests > 500ms are logged as warnings
3. **Database Query Performance**: Can be enabled via `log_query_performance()`

### Viewing Performance Data

**In Development**:

Check browser console for Web Vitals output:

```
[Web Vitals] ✓ FCP: 1234ms (good)
[Web Vitals] ✓ LCP: 2100ms (good)
[Web Vitals] ⚠ CLS: 0.15 (needs-improvement)
```

**API Performance**:

Check the `X-Process-Time` header in API responses:

```bash
curl -I http://localhost:8000/api/v1/questions
# X-Process-Time: 45.23ms
```

**Server Logs**:

```bash
# View performance logs
tail -f logs/api.log | grep "performance"

# Slow request warnings
tail -f logs/api.log | grep "Slow request"
```

---

## Performance Budgets

### Bundle Size Budgets

- **Initial Bundle**: < 200KB gzipped
- **Any Single Route**: < 300KB gzipped
- **Total JavaScript**: < 1MB gzipped

### Runtime Performance Budgets

| Metric | Good | Needs Improvement | Poor |
|--------|------|-------------------|------|
| FCP | < 1.8s | < 3.0s | > 3.0s |
| LCP | < 2.5s | < 4.0s | > 4.0s |
| CLS | < 0.1 | < 0.25 | > 0.25 |
| FID | < 100ms | < 300ms | > 300ms |
| INP | < 200ms | < 500ms | > 500ms |
| TTFB | < 800ms | < 1.8s | > 1.8s |
| TTI | < 3.0s | < 5.0s | > 5.0s |

### API Performance Budgets

- **p50 Response Time**: < 200ms
- **p95 Response Time**: < 500ms
- **p99 Response Time**: < 1000ms
- **Database Query Time**: < 100ms

---

## Best Practices

### When Adding New Features

1. **Frontend**:
   - Use dynamic imports for components with heavy libraries
   - Keep page-specific code in route-specific files
   - Use React.memo for expensive pure components
   - Implement proper loading states

2. **Backend**:
   - Add pagination to list endpoints
   - Use caching for frequently accessed data
   - Optimize database queries with proper indexes
   - Use `yield` for database dependencies

3. **Monitoring**:
   - Test performance locally before deploying
   - Check browser console for Web Vitals warnings
   - Monitor API response times in logs

### Code Splitting Guidelines

**Use dynamic imports for**:
- Chart libraries (Recharts, D3.js)
- PDF generators (jsPDF, html2canvas)
- Rich text editors
- Data visualization components

**Keep in main bundle**:
- Authentication components
- Core UI components
- Navigation and layout

### Performance Checklist

Before committing code:

- [ ] Heavy components use dynamic imports
- [ ] No console logs in production code
- [ ] Loading states implemented
- [ ] Images optimized (WebP/AVIF formats)
- [ ] API calls use pagination
- [ ] Database queries optimized
- [ ] No memory leaks (cleanup effects)
- [ ] Web Vitals metrics pass budgets

---

## Troubleshooting

### Bundle Size Issues

To analyze bundle size:

```bash
cd frontend-web
ANALYZE=true npm run build
```

This generates a bundle analysis report at `.next/analyze/client.html`.

### Slow API Responses

1. Check the `X-Process-Time` header
2. Enable query performance logging:

```python
from api.utils.performance import log_query_performance

with get_db() as db:
    log_stats = log_query_performance(db, "operation_name")
    # ... your code ...
    log_stats()  # Print stats
```

### Web Vitals Warnings

Common issues and fixes:

| Issue | Fix |
|-------|-----|
| High CLS | Reserve space for images and dynamic content |
| High LCP | Optimize images, use lazy loading |
| High FID/INP | Reduce JavaScript execution time |
| High TTFB | Optimize server, use CDN |

---

## Additional Resources

- [Web.dev Performance](https://web.dev/performance/)
- [Next.js Performance](https://nextjs.org/docs/app/building-your-application/optimizing)
- [FastAPI Performance](https://fastapi.tiangolo.com/tutorial/)
- [Core Web Vitals](https://web.dev/vitals/)

---

**Last Updated**: 2026-02-21
