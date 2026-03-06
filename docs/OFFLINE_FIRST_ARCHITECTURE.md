# Offline-First Architecture Documentation

This document describes the implementation of the offline-first architecture in Soul Sense EQ Test.

## Overview

The application now supports full offline functionality, allowing users to:
- Take EQ assessments without internet connection
- Write and edit journal entries offline
- View past assessment results
- Read previous journal entries
- Update profile information
- View insights and trends

All data is automatically synchronized when connectivity is restored.

## Architecture Components

### 1. IndexedDB Database (Dexie.js)

**Location:** `frontend-web/src/lib/offline/db.ts`

The application uses Dexie.js to manage IndexedDB storage with the following schema:

```typescript
- assessments: Stores completed EQ assessments with sync status
- journals: Stores journal entries with sync status
- questionsCache: Caches questions for offline access
- syncQueue: Queue of API requests to retry when online
- userSettings: Stores user preferences with sync status
```

Each record includes:
- `synced`: Boolean flag indicating if data has been synced with server
- `createdAt`/`updatedAt`: Timestamps for conflict resolution
- User-specific data for isolation

### 2. Network Status Monitoring

**Location:** `frontend-web/src/lib/offline/network.ts`

The `networkMonitor` provides:
- Real-time online/offline status detection
- Connection quality monitoring (via Network Information API)
- Event subscription system for UI updates
- Automatic triggering of sync when connection restored

```typescript
// Subscribe to network changes
const unsubscribe = networkMonitor.subscribe((state) => {
  console.log(state.isOnline ? 'Online' : 'Offline');
});
```

### 3. Sync Queue System

**Location:** `frontend-web/src/lib/offline/syncQueue.ts`

The sync queue manages offline API requests with:

**Features:**
- Priority-based queuing (high, medium, low)
- Automatic retry with exponential backoff
- Max retry limits (default: 3)
- Background processing when connection restored

**Usage:**
```typescript
await syncQueue.addItem(
  '/api/v1/assessments',
  'POST',
  { assessmentData },
  { 'Authorization': 'Bearer token' },
  'high' // priority
);
```

### 4. Offline-First API Client

**Location:** `frontend-web/src/lib/offline/apiClient.ts`

The `offlineClient` wraps all API calls with offline logic:

**Behavior:**
- **Online Mode**: Direct API calls, success saves to cache
- **Offline Mode**:
  - GET requests: Return cached data if available
  - POST/PUT/PATCH: Queue for sync, save to IndexedDB
  - DELETE: Queue for sync

**Special Methods:**
```typescript
// Save assessment offline
await offlineClient.saveAssessmentOffline(data);

// Save journal offline
await offlineClient.saveJournalOffline(data);

// Sync all pending data
await offlineClient.syncPendingData();
```

### 5. Service Worker

**Location:** `frontend-web/public/sw.js`

The service worker provides:
- Static asset caching (PWA functionality)
- API response caching with stale-while-revalidate
- Offline fallback pages
- Background sync API integration

**Cache Strategy:**
- Static assets: Cache-first
- API requests: Network-first with cache fallback
- Offline page: Cache-first for /offline

### 6. Conflict Resolution

**Location:** `frontend-web/src/lib/offline/conflictResolution.ts`

Automatic conflict resolution based on timestamps:

**Strategies:**
- **Last-write-wins**: For most fields (using timestamps)
- **Server-authority**: For critical data
- **Merge**: For journal entries (content concatenation)
- **User prompts**: For unresolvable conflicts (future enhancement)

```typescript
const result = await conflictResolver.resolveAssessment(local, remote);
if (result.resolved) {
  // Apply resolved data
}
```

### 7. UI Components

**Location:** `frontend-web/src/components/offline/`

**OfflineBanner:**
- Shows online/offline status
- Displays sync progress
- Indicates pending operations count
- Auto-hides when all synced

**SyncButton:**
- Manual sync trigger
- Shows pending items count
- Priority breakdown dropdown
- Last sync time display

## State Management

**Location:** `frontend-web/src/stores/syncStore.ts`

Zustand store with persistence for:
- Current online status
- Pending operations count
- Last sync time
- Active conflicts
- Sync state

## Integration Guide

### Using Offline Storage

```typescript
import { db } from '@/lib/offline';

// Save assessment
await db.assessments.add({
  username: 'user',
  assessmentId: 'id',
  answers: {},
  score: 85,
  categoryScores: {},
  completedAt: new Date(),
  synced: false,
  createdAt: new Date(),
  updatedAt: new Date(),
});

// Get pending items
const pending = await db.getPendingAssessments();

// Mark as synced
await db.markAsSynced('assessments', id);
```

### Monitoring Network Status

```typescript
import { networkMonitor } from '@/lib/offline/network';

// Check current status
if (networkMonitor.isOnline()) {
  // Perform online-only operation
}

// Subscribe to changes
const unsubscribe = networkMonitor.subscribe((state) => {
  updateUI(state);
});
```

### Manual Sync Trigger

```typescript
import { syncQueue } from '@/lib/offline/syncQueue';

// Process queue
const results = await syncQueue.processQueue();

// Get stats
const stats = await syncQueue.getStats();
console.log(`${stats.totalPending} items pending`);
```

## Testing Offline Functionality

### Using Chrome DevTools

1. Open DevTools (F12)
2. Go to Network tab
3. Select "Offline" from throttling dropdown
4. Test application features
5. Select "Online" to trigger sync

### Test Scenarios

**1. Offline Assessment:**
- Go offline
- Take an assessment
- Verify it saves to IndexedDB
- Go online
- Verify automatic sync

**2. Offline Journal:**
- Go offline
- Write journal entry
- Verify it saves locally
- Go online
- Verify sync completion

**3. Conflict Resolution:**
- Modify data offline
- Modify same data on another device
- Go online
- Verify conflict resolution

## PWA Features

### Installation

The app can be installed as a PWA:
- Progressive Web App manifest included
- Service worker registered
- Icons and theme configured

### Offline Support

- Dedicated offline page at `/offline`
- Cached assets for instant loading
- Queued operations sync automatically

## Performance Considerations

**IndexedDB Limits:**
- Storage quota varies by browser (typically 50-80% of disk space)
- Implement cleanup for old cached data
- Monitor storage usage

**Sync Optimization:**
- Batch multiple requests when possible
- Use incremental sync (only changed data)
- Implement delta updates for large datasets

**Cache Invalidation:**
- Version-based cache invalidation
- Time-based expiration for cached questions
- Manual cache clear option in settings

## Security Considerations

**Data Encryption:**
- Consider encrypting sensitive data in IndexedDB
- Use HTTPS for all API communications
- Implement proper auth token refresh

**Conflict Detection:**
- Use checksums for data integrity
- Validate synced data on server
- Log all conflict resolutions

## Future Enhancements

1. **Advanced Conflict Resolution:**
   - User prompts for unresolvable conflicts
   - Operational transformation for text fields
   - Conflict history and audit logs

2. **Sync Optimization:**
   - Incremental sync with server-side change tracking
   - Compression for large payloads
   - WebSocket-based real-time sync

3. **Enhanced Caching:**
   - Smart cache preloading based on usage patterns
   - Predictive data fetching
   - Adaptive cache sizing

4. **Monitoring:**
   - Sync success/failure metrics
   - Offline usage analytics
   - Performance monitoring

## Troubleshooting

**Sync not working:**
- Check browser console for errors
- Verify IndexedDB is accessible
- Check network connection
- Clear cache and retry

**Conflicts not resolving:**
- Check timestamp formats
- Verify server response format
- Review conflict resolution logs

**Service worker not updating:**
- Unregister and re-register SW
- Clear browser cache
- Check for multiple SW instances

## Related Files

- `frontend-web/src/lib/offline/*` - Core offline functionality
- `frontend-web/src/components/offline/*` - UI components
- `frontend-web/src/stores/syncStore.ts` - State management
- `frontend-web/public/sw.js` - Service worker
- `frontend-web/public/manifest.json` - PWA manifest
- `frontend-web/src/app/offline/page.tsx` - Offline page

## References

- [Dexie.js Documentation](https://dexie.org/)
- [Service Worker API](https://developer.mozilla.org/en-US/docs/Web/API/Service_Worker_API)
- [IndexedDB API](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API)
- [Background Sync API](https://developer.mozilla.org/en-US/docs/Web/API/Background_Sync_API)
