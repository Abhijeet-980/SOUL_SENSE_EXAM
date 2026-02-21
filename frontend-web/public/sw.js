const CACHE_NAME = 'soulsense-v1';
const OFFLINE_URL = '/offline';

const ASSETS_TO_CACHE = [
  '/',
  '/offline',
  '/manifest.json',
  '/icon.png',
];

const API_CACHE_PATTERN = /\/api\/.*/;

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(ASSETS_TO_CACHE);
    })
  );

  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (cacheName !== CACHE_NAME) {
            return caches.delete(cacheName);
          }
        })
      );
    })
  );

  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  if (url.origin === location.origin) {
    if (API_CACHE_PATTERN.test(url.pathname)) {
      event.respondWith(handleApiRequest(request));
    } else {
      event.respondWith(handleAssetRequest(request));
    }
  }
});

async function handleApiRequest(request) {
  try {
    const networkResponse = await fetch(request);

    if (networkResponse.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, networkResponse.clone());
    }

    return networkResponse;
  } catch (error) {
    const cachedResponse = await caches.match(request);

    if (cachedResponse) {
      return cachedResponse;
    }

    const offlineResponse = {
      status: 503,
      statusText: 'Service Unavailable',
      headers: new Headers({ 'Content-Type': 'application/json' }),
    };

    const body = JSON.stringify({
      error: 'Offline - Request queued for sync',
      queued: true,
    });

    return new Response(body, offlineResponse);
  }
}

async function handleAssetRequest(request) {
  const cachedResponse = await caches.match(request);

  if (cachedResponse) {
    return cachedResponse;
  }

  try {
    const networkResponse = await fetch(request);

    if (networkResponse.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, networkResponse.clone());
    }

    return networkResponse;
  } catch (error) {
    if (request.destination === 'document') {
      return caches.match(OFFLINE_URL);
    }

    throw error;
  }
}

self.addEventListener('sync', (event) => {
  if (event.tag === 'sync-data') {
    event.waitUntil(syncOfflineData());
  }
});

async function syncOfflineData() {
  try {
    const clients = await self.clients.matchAll();

    clients.forEach((client) => {
      client.postMessage({
        type: 'SYNC_START',
      });
    });

    const response = await fetch('/api/sync', {
      method: 'POST',
    });

    if (response.ok) {
      clients.forEach((client) => {
        client.postMessage({
          type: 'SYNC_COMPLETE',
        });
      });
    }
  } catch (error) {
    console.error('Sync failed:', error);
  }
}

self.addEventListener('push', (event) => {
  if (event.data) {
    const data = event.data.json();

    const options = {
      body: data.body,
      icon: data.icon || '/icon.png',
      badge: '/badge.png',
      vibrate: [100, 50, 100],
      data: {
        dateOfArrival: Date.now(),
        primaryKey: '1',
      },
    };

    event.waitUntil(self.registration.showNotification(data.title, options));
  }
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  event.waitUntil(
    clients.openWindow('/').catch(() => {
      return clients.openWindow('/');
    })
  );
});
