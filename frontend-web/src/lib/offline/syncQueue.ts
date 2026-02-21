import { db, SyncQueueItem } from './db';
import { networkMonitor } from './network';

export interface SyncResult {
  success: boolean;
  item: SyncQueueItem;
  error?: string;
}

export interface SyncStats {
  totalPending: number;
  highPriority: number;
  mediumPriority: number;
  lowPriority: number;
  lastSyncTime?: Date;
}

class SyncQueue {
  private isProcessing = false;
  private maxRetries = 3;
  private retryDelay = 1000;
  private syncInProgress = false;

  async addItem(
    url: string,
    method: string,
    body?: any,
    headers?: Record<string, string>,
    priority: 'high' | 'medium' | 'low' = 'medium'
  ): Promise<number> {
    const id = await db.syncQueue.add({
      url,
      method,
      headers,
      body,
      retryCount: 0,
      priority,
      createdAt: new Date(),
    });

    if (networkMonitor.isOnline() && !this.syncInProgress) {
      this.processQueue();
    }

    return id;
  }

  async getStats(): Promise<SyncStats> {
    const items = await db.syncQueue.toArray();

    const stats: SyncStats = {
      totalPending: items.length,
      highPriority: items.filter(i => i.priority === 'high').length,
      mediumPriority: items.filter(i => i.priority === 'medium').length,
      lowPriority: items.filter(i => i.priority === 'low').length,
    };

    return stats;
  }

  async processQueue(): Promise<SyncResult[]> {
    if (this.syncInProgress) {
      return [];
    }

    this.syncInProgress = true;
    const results: SyncResult[] = [];

    try {
      const items = await db.syncQueue.orderBy('priority').toArray();

      for (const item of items) {
        if (!networkMonitor.isOnline()) {
          break;
        }

        const result = await this.processItem(item);
        results.push(result);

        if (result.success) {
          await db.deleteSyncQueueItem(item.id!);
        } else if (item.retryCount! >= this.maxRetries) {
          await db.deleteSyncQueueItem(item.id!);
        } else {
          await db.syncQueue.update(item.id!, {
            retryCount: item.retryCount! + 1,
            lastAttempt: new Date(),
            error: result.error,
          });

          await this.delay(this.retryDelay * Math.pow(2, item.retryCount!));
        }
      }
    } finally {
      this.syncInProgress = false;
    }

    return results;
  }

  private async processItem(item: SyncQueueItem): Promise<SyncResult> {
    try {
      const options: RequestInit = {
        method: item.method,
        headers: {
          'Content-Type': 'application/json',
          ...item.headers,
        },
      };

      if (item.body && ['POST', 'PUT', 'PATCH'].includes(item.method)) {
        options.body = JSON.stringify(item.body);
      }

      const response = await fetch(item.url, options);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return {
        success: true,
        item,
      };
    } catch (error) {
      return {
        success: false,
        item,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async clearQueue(): Promise<void> {
    await db.syncQueue.clear();
  }

  async removeItem(id: number): Promise<void> {
    await db.syncQueue.delete(id);
  }

  isProcessingQueue(): boolean {
    return this.syncInProgress;
  }
}

export const syncQueue = new SyncQueue();
