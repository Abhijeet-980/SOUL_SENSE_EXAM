# Offline-First Implementation

This directory contains the core offline-first functionality for the Soul Sense EQ Test application.

## Features

- **Offline Database**: IndexedDB storage using Dexie.js
- **Network Detection**: Real-time online/offline status monitoring
- **Sync Queue**: Automatic retry of failed requests when back online
- **Conflict Resolution**: Automatic resolution of conflicting data changes
- **Service Worker**: Background sync and asset caching
- **PWA Support**: Progressive Web App features for installation

## File Structure

```
offline/
├── db.ts                    # IndexedDB schema and operations
├── network.ts               # Network status monitoring
├── syncQueue.ts             # Request queue management
├── apiClient.ts             # Offline-first API wrapper
├── conflictResolution.ts    # Conflict resolution strategies
├── serviceWorker.ts         # Service worker utilities
└── index.ts                 # Public API exports
```

## Quick Start

### 1. Basic Usage

```typescript
import { offlineClient, networkMonitor } from '@/lib/offline';

// Check if online
if (networkMonitor.isOnline()) {
  // Make API call
} else {
  // Work offline
}

// Save data offline
await offlineClient.saveAssessmentOffline(data);
```

### 2. Monitor Network Status

```typescript
import { networkMonitor } from '@/lib/offline/network';

const unsubscribe = networkMonitor.subscribe((state) => {
  console.log('Online:', state.isOnline);
  console.log('Connection type:', state.effectiveConnectionType);
});
```

### 3. Manual Sync

```typescript
import { syncQueue } from '@/lib/offline/syncQueue';

// Process all pending requests
const results = await syncQueue.processQueue();

// Get queue statistics
const stats = await syncQueue.getStats();
console.log('Pending items:', stats.totalPending);
```

## Database Schema

### Assessments Table
```typescript
{
  id: number;
  username: string;
  assessmentId: string;
  answers: Record<string, number>;
  score: number;
  categoryScores: Record<string, number>;
  completedAt: Date;
  synced: boolean;
  createdAt: Date;
  updatedAt: Date;
}
```

### Journals Table
```typescript
{
  id: number;
  username: string;
  journalId: string;
  content: string;
  mood?: string;
  tags?: string[];
  sentiment?: number;
  createdAt: Date;
  updatedAt: Date;
  synced: boolean;
}
```

### Sync Queue Table
```typescript
{
  id: number;
  url: string;
  method: string;
  headers?: Record<string, string>;
  body?: any;
  retryCount: number;
  priority: 'high' | 'medium' | 'low';
  createdAt: Date;
  lastAttempt?: Date;
  error?: string;
}
```

## Conflict Resolution

The application uses automatic conflict resolution based on:

1. **Timestamps**: Last-write-wins for most fields
2. **Server Authority**: Server data takes precedence for critical fields
3. **Merge Strategies**: Smart merging for journal entries
4. **User Prompts**: Manual resolution for complex conflicts (future)

## Testing

### Test Offline Mode

1. Open Chrome DevTools
2. Go to Network tab
3. Select "Offline" from throttling
4. Use the application
5. Select "Online" to trigger sync

### Test Service Worker

```javascript
// In browser console
navigator.serviceWorker.getRegistration().then(reg => {
  reg.sync.register('sync-data');
});
```

### View IndexedDB Data

1. Open Chrome DevTools
2. Go to Application tab
3. Expand IndexedDB
4. View SoulSenseDB tables

## Best Practices

1. **Always check network status** before critical operations
2. **Handle offline errors** gracefully with user feedback
3. **Use appropriate priorities** for sync queue items
4. **Monitor storage usage** and implement cleanup
5. **Test thoroughly** on actual devices with poor connectivity

## Troubleshooting

### Service Worker Not Registering
- Check browser console for errors
- Verify sw.js path is correct
- Ensure serving over HTTPS (or localhost)

### Sync Not Working
- Check network connection
- Verify IndexedDB is accessible
- Review sync queue for errors
- Check API endpoints are accessible

### IndexedDB Quota Exceeded
- Implement data cleanup for old entries
- Use compression for large data
- Clear cache periodically

## Dependencies

- `dexie`: IndexedDB wrapper
- Browser APIs: Service Worker, Background Sync, IndexedDB

## Documentation

See `/docs/OFFLINE_FIRST_ARCHITECTURE.md` for detailed architecture documentation.
