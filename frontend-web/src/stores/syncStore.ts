import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { networkMonitor, NetworkState } from '@/lib/offline/network';
import { syncQueue, SyncStats } from '@/lib/offline/syncQueue';
import { db } from '@/lib/offline/db';

export interface Conflict {
  id: string;
  type: 'assessment' | 'journal' | 'settings';
  localData: any;
  serverData: any;
  timestamp: Date;
  resolved?: boolean;
}

interface SyncState {
  isOnline: boolean;
  pendingOperations: number;
  lastSyncTime: Date | null;
  conflicts: Conflict[];
  isSyncing: boolean;

  setIsOnline: (online: boolean) => void;
  setPendingOperations: (count: number) => void;
  setLastSyncTime: (time: Date | null) => void;
  setIsSyncing: (syncing: boolean) => void;
  addConflict: (conflict: Conflict) => void;
  resolveConflict: (id: string) => void;
  clearConflicts: () => void;

  sync: () => Promise<void>;
  refreshStats: () => Promise<void>;
}

export const useSyncStore = create<SyncState>()(
  persist(
    (set, get) => ({
      isOnline: typeof navigator !== 'undefined' ? navigator.onLine : true,
      pendingOperations: 0,
      lastSyncTime: null,
      conflicts: [],
      isSyncing: false,

      setIsOnline: (online) => set({ isOnline: online }),

      setPendingOperations: (count) => set({ pendingOperations: count }),

      setLastSyncTime: (time) => set({ lastSyncTime: time }),

      setIsSyncing: (syncing) => set({ isSyncing: syncing }),

      addConflict: (conflict) =>
        set((state) => ({
          conflicts: [...state.conflicts, conflict],
        })),

      resolveConflict: (id) =>
        set((state) => ({
          conflicts: state.conflicts.map((c) =>
            c.id === id ? { ...c, resolved: true } : c
          ),
        })),

      clearConflicts: () => set({ conflicts: [] }),

      sync: async () => {
        const { isSyncing } = get();

        if (isSyncing) {
          return;
        }

        set({ isSyncing: true });

        try {
          await syncQueue.processQueue();

          const stats = await syncQueue.getStats();

          set({
            pendingOperations: stats.totalPending,
            lastSyncTime: new Date(),
            isSyncing: false,
          });
        } catch (error) {
          console.error('Sync failed:', error);
          set({ isSyncing: false });
        }
      },

      refreshStats: async () => {
        const stats = await syncQueue.getStats();
        const isOnline = networkMonitor.isOnline();

        set({
          pendingOperations: stats.totalPending,
          isOnline,
        });
      },
    }),
    {
      name: 'soulsense-sync-storage',
      partialize: (state) => ({
        lastSyncTime: state.lastSyncTime,
        conflicts: state.conflicts,
      }),
    }
  )
);

networkMonitor.subscribe((state) => {
  useSyncStore.getState().setIsOnline(state.isOnline);

  if (state.isOnline) {
    useSyncStore.getState().sync();
  }
});
