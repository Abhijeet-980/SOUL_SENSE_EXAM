'use client';

import { useState, useEffect } from 'react';
import { Button } from '@/components/ui';
import { RefreshCw, CheckCircle2, XCircle } from 'lucide-react';
import { syncQueue } from '@/lib/offline/syncQueue';
import { networkMonitor } from '@/lib/offline/network';

export function SyncButton() {
  const [stats, setStats] = useState({
    totalPending: 0,
    highPriority: 0,
    mediumPriority: 0,
    lowPriority: 0,
  });
  const [isOnline, setIsOnline] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [lastSync, setLastSync] = useState<Date | null>(null);
  const [showTooltip, setShowTooltip] = useState(false);

  useEffect(() => {
    const updateStats = async () => {
      const newStats = await syncQueue.getStats();
      setStats(newStats);
      setIsOnline(networkMonitor.isOnline());
      setSyncing(syncQueue.isProcessingQueue());
    };

    updateStats();

    const unsubscribe = networkMonitor.subscribe(() => {
      setIsOnline(networkMonitor.isOnline());
    });

    const interval = setInterval(updateStats, 3000);

    return () => {
      unsubscribe();
      clearInterval(interval);
    };
  }, []);

  const handleSync = async () => {
    if (!isOnline || syncing) return;

    setSyncing(true);

    try {
      const results = await syncQueue.processQueue();

      const successful = results.filter(r => r.success).length;
      const failed = results.filter(r => !r.success).length;

      if (failed > 0) {
        console.error(`Sync completed: ${successful} succeeded, ${failed} failed`);
      }

      setLastSync(new Date());

      const newStats = await syncQueue.getStats();
      setStats(newStats);
    } catch (error) {
      console.error('Sync failed:', error);
    } finally {
      setSyncing(false);
    }
  };

  if (stats.totalPending === 0 && isOnline) {
    return (
      <Button variant="ghost" size="sm" className="gap-2" disabled>
        <CheckCircle2 className="w-4 h-4 text-green-500" />
        <span className="hidden sm:inline">All synced</span>
      </Button>
    );
  }

  return (
    <div className="relative">
      <Button
        variant={isOnline ? 'default' : 'secondary'}
        size="sm"
        className="gap-2"
        onClick={isOnline ? handleSync : undefined}
        disabled={!isOnline || syncing}
        onMouseEnter={() => setShowTooltip(true)}
        onMouseLeave={() => setShowTooltip(false)}
      >
        <RefreshCw className={`w-4 h-4 ${syncing ? 'animate-spin' : ''}`} />
        <span className="hidden sm:inline">
          {syncing ? 'Syncing...' : `Sync (${stats.totalPending})`}
        </span>
      </Button>

      {showTooltip && (
        <div className="absolute right-0 top-full mt-2 w-48 bg-popover border rounded-lg shadow-lg p-3 z-50">
          <div className="space-y-1 text-xs">
            <div className="flex justify-between">
              <span>Status:</span>
              <span className={isOnline ? 'text-green-600' : 'text-orange-600'}>
                {isOnline ? 'Online' : 'Offline'}
              </span>
            </div>
            <div className="flex justify-between">
              <span>Pending:</span>
              <span className="font-medium">{stats.totalPending}</span>
            </div>
            <div className="flex justify-between text-red-600">
              <span>High:</span>
              <span>{stats.highPriority}</span>
            </div>
            <div className="flex justify-between text-yellow-600">
              <span>Medium:</span>
              <span>{stats.mediumPriority}</span>
            </div>
            <div className="flex justify-between text-gray-600">
              <span>Low:</span>
              <span>{stats.lowPriority}</span>
            </div>
            {lastSync && (
              <div className="pt-1 border-t text-muted-foreground">
                Last: {lastSync.toLocaleTimeString()}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
