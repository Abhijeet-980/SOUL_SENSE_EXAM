'use client';

import { useState, useEffect } from 'react';
import { useSettings } from '@/hooks/useSettings';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui';
import { Button } from '@/components/ui';
import { Skeleton } from '@/components/ui';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui';
import {
  ThemeToggle,
  NotificationSettings,
  PrivacySettings,
  AccountSettings,
  AboutSettings,
} from '@/components/settings';
import { cn } from '@/lib/utils';
import { CheckCircle, AlertCircle, Settings as SettingsIcon } from 'lucide-react';

const tabs = [
  { id: 'appearance', label: 'Appearance', icon: '🎨' },
  { id: 'notifications', label: 'Notifications', icon: '🔔' },
  { id: 'privacy', label: 'Privacy & Data', icon: '🔒' },
  { id: 'account', label: 'Account', icon: '👤' },
  { id: 'about', label: 'About', icon: 'ℹ️' },
];

export default function SettingsPage() {
  const { settings, isLoading, error, updateSettings, syncSettings } = useSettings();
  const [activeTab, setActiveTab] = useState('appearance');
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const [isMobile, setIsMobile] = useState(false);

  // Handle URL hash for direct tab links
  useEffect(() => {
    const hash = window.location.hash.replace('#', '');
    if (hash && tabs.some((tab) => tab.id === hash)) {
      setActiveTab(hash);
    }
  }, []);

  // Update URL hash when tab changes
  useEffect(() => {
    window.location.hash = activeTab;
  }, [activeTab]);

  // Check if mobile
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768);
    };
    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, []);

  const handleSettingChange = async (updates: any) => {
    setSaveStatus('saving');
    try {
      await updateSettings(updates);
      setSaveStatus('saved');
      setTimeout(() => setSaveStatus('idle'), 2000);
    } catch (err) {
      setSaveStatus('error');
      setTimeout(() => setSaveStatus('idle'), 3000);
    }
  };

  const handleSync = async () => {
    try {
      await syncSettings();
    } catch (err) {
      console.error('Failed to sync settings:', err);
    }
  };

  if (isLoading) {
    return (
      <div className="max-w-7xl mx-auto p-6 space-y-6">
        <div className="flex items-center gap-3 mb-8">
          <SettingsIcon className="h-8 w-8" />
          <h1 className="text-3xl font-bold">Settings</h1>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          <Skeleton className="h-96 lg:col-span-1" />
          <div className="lg:col-span-3 space-y-6">
            <Skeleton className="h-64" />
            <Skeleton className="h-64" />
            <Skeleton className="h-64" />
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-7xl mx-auto p-6">
        <Card className="border-red-200">
          <CardContent className="p-6">
            <div className="flex items-center gap-3 text-red-600">
              <AlertCircle className="h-5 w-5" />
              <p className="font-medium">Error loading settings</p>
            </div>
            <p className="text-red-500 mt-2">{error}</p>
            <Button onClick={() => window.location.reload()} className="mt-4" variant="outline">
              Try Again
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (!settings) {
    return (
      <div className="max-w-7xl mx-auto p-6">
        <Card>
          <CardContent className="p-6">
            <p className="text-muted-foreground">No settings available</p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <SettingsIcon className="h-8 w-8" />
          <h1 className="text-3xl font-bold">Settings</h1>
        </div>

        {/* Save Status */}
        <div className="flex items-center gap-4">
          {saveStatus === 'saving' && (
            <div className="flex items-center gap-2 text-blue-600">
              <div className="animate-spin h-4 w-4 border-2 border-blue-600 border-t-transparent rounded-full" />
              <span className="text-sm">Saving...</span>
            </div>
          )}
          {saveStatus === 'saved' && (
            <div className="flex items-center gap-2 text-green-600">
              <CheckCircle className="h-4 w-4" />
              <span className="text-sm">Saved</span>
            </div>
          )}
          {saveStatus === 'error' && (
            <div className="flex items-center gap-2 text-red-600">
              <AlertCircle className="h-4 w-4" />
              <span className="text-sm">Save failed</span>
            </div>
          )}

          <Button onClick={handleSync} variant="outline" size="sm">
            Sync Settings
          </Button>
        </div>
      </div>

      {/* Settings Content */}
      <div className={cn('grid gap-6', isMobile ? 'grid-cols-1' : 'grid-cols-1 lg:grid-cols-4')}>
        <Tabs
          value={activeTab}
          onValueChange={setActiveTab}
          orientation={isMobile ? 'horizontal' : 'vertical'}
          className={cn('w-full', isMobile ? '' : 'lg:col-span-1')}
        >
          <TabsList
            className={cn(
              'grid w-full',
              isMobile
                ? 'grid-cols-5 h-auto p-1'
                : 'grid-cols-1 h-auto p-2 space-y-1 bg-transparent'
            )}
          >
            {tabs.map((tab) => (
              <TabsTrigger
                key={tab.id}
                value={tab.id}
                className={cn(
                  'flex items-center gap-3 justify-start p-3 h-auto',
                  isMobile ? 'flex-col text-xs' : 'text-left',
                  activeTab === tab.id && 'bg-primary text-primary-foreground'
                )}
              >
                <span className="text-lg">{tab.icon}</span>
                <span className={cn(isMobile ? 'text-xs' : 'text-sm')}>{tab.label}</span>
              </TabsTrigger>
            ))}
          </TabsList>

          <div className={cn('lg:col-span-3', isMobile ? 'mt-6' : '')}>
            <TabsContent value="appearance" className="mt-0">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">🎨 Appearance</CardTitle>
                </CardHeader>
                <CardContent>
                  <ThemeToggle settings={settings} onChange={handleSettingChange} />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="notifications" className="mt-0">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">🔔 Notifications</CardTitle>
                </CardHeader>
                <CardContent>
                  <NotificationSettings settings={settings} onChange={handleSettingChange} />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="privacy" className="mt-0">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">🔒 Privacy & Data</CardTitle>
                </CardHeader>
                <CardContent>
                  <PrivacySettings settings={settings} onChange={handleSettingChange} />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="account" className="mt-0">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">👤 Account</CardTitle>
                </CardHeader>
                <CardContent>
                  <AccountSettings settings={settings} onChange={handleSettingChange} />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="about" className="mt-0">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">ℹ️ About</CardTitle>
                </CardHeader>
                <CardContent>
                  <AboutSettings />
                </CardContent>
              </Card>
            </TabsContent>
          </div>
        </Tabs>
      </div>
    </div>
  );
}
