'use client';

import { useState } from 'react';
import { PremiumNotificationSettings, NotificationSettingsObject } from '@/components/settings';

export default function NotificationsDemoPage() {
  const [settings, setSettings] = useState<NotificationSettingsObject>({
    masterEnabled: true,
    emailEnabled: true,
    pushEnabled: false,
    frequency: 'daily',
    quietHours: {
      start: '22:00',
      end: '07:00',
    },
  });

  const handleSettingsChange = (updated: Partial<NotificationSettingsObject>) => {
    setSettings((prev: NotificationSettingsObject) => ({ ...prev, ...updated }));
  };

  return (
    <div className="min-h-screen bg-background py-12 px-4">
      <div className="max-w-4xl mx-auto space-y-12">
        <div className="text-center space-y-4">
          <h1 className="text-4xl font-extrabold tracking-tight">Notification Settings Demo</h1>
          <p className="text-muted-foreground text-lg">
            Testing the Notification Preferences interface
          </p>
        </div>

        <div className="bg-card p-6 rounded-3xl border shadow-2xl">
          <PremiumNotificationSettings settings={settings} onChange={handleSettingsChange} />
        </div>

        <div className="bg-muted p-6 rounded-2xl border">
          <h3 className="font-bold mb-4">Current State Debug:</h3>
          <pre className="text-xs bg-black text-green-400 p-4 rounded overflow-auto">
            {JSON.stringify(settings, null, 2)}
          </pre>
        </div>
      </div>
    </div>
  );
}
