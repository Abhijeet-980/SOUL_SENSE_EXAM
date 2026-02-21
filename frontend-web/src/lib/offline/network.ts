export type NetworkStatus = 'online' | 'offline' | 'unknown';

export interface NetworkState {
  isOnline: boolean;
  status: NetworkStatus;
  lastOnlineTime?: Date;
  lastOfflineTime?: Date;
  effectiveConnectionType?: string;
}

class NetworkMonitor {
  private listeners: Set<(state: NetworkState) => void> = new Set();
  private currentState: NetworkState = {
    isOnline: typeof navigator !== 'undefined' ? navigator.onLine : true,
    status: 'unknown',
  };

  constructor() {
    if (typeof window !== 'undefined') {
      this.initialize();
    }
  }

  private initialize() {
    this.updateStatus();

    window.addEventListener('online', () => {
      this.currentState.isOnline = true;
      this.currentState.lastOnlineTime = new Date();
      this.currentState.status = 'online';
      this.notifyListeners();
    });

    window.addEventListener('offline', () => {
      this.currentState.isOnline = false;
      this.currentState.lastOfflineTime = new Date();
      this.currentState.status = 'offline';
      this.notifyListeners();
    });

    if ('connection' in navigator) {
      const connection = (navigator as any).connection;
      this.currentState.effectiveConnectionType = connection.effectiveType;

      connection.addEventListener('change', () => {
        this.currentState.effectiveConnectionType = connection.effectiveType;
        this.notifyListeners();
      });
    }
  }

  private updateStatus() {
    this.currentState.isOnline = navigator.onLine;
    this.currentState.status = navigator.onLine ? 'online' : 'offline';
  }

  private notifyListeners() {
    this.listeners.forEach(listener => listener({ ...this.currentState }));
  }

  subscribe(listener: (state: NetworkState) => void): () => void {
    this.listeners.add(listener);

    listener({ ...this.currentState });

    return () => {
      this.listeners.delete(listener);
    };
  }

  getCurrentState(): NetworkState {
    return { ...this.currentState };
  }

  isOnline(): boolean {
    return this.currentState.isOnline;
  }
}

export const networkMonitor = new NetworkMonitor();
