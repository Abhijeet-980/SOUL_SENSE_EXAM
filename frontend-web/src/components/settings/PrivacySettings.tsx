'use client';

import { UserSettings } from '@/lib/api/settings';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui';
import { Checkbox } from '@/components/ui';
import { Button } from '@/components/ui';
import { useDebounce } from '@/hooks/useDebounce';
import { AlertTriangle, Download, Trash2 } from 'lucide-react';

interface PrivacySettingsProps {
  settings: UserSettings;
  onChange: (updates: Partial<UserSettings>) => void;
}

export function PrivacySettings({ settings, onChange }: PrivacySettingsProps) {
  const debouncedOnChange = useDebounce(onChange, 500);

  const handlePrivacyChange = (key: 'data_collection' | 'analytics', value: boolean) => {
    debouncedOnChange({
      privacy: {
        ...settings.privacy,
        [key]: value,
      },
    });
  };

  const handleRetentionChange = (days: number) => {
    debouncedOnChange({
      privacy: {
        ...settings.privacy,
        data_retention_days: days,
      },
    });
  };

  const handleVisibilityChange = (visibility: 'public' | 'private' | 'friends') => {
    debouncedOnChange({
      privacy: {
        ...settings.privacy,
        profile_visibility: visibility,
      },
    });
  };

  const handleExportData = () => {
    // TODO: Implement data export functionality
    console.log('Exporting user data...');
  };

  const handleDeleteAccount = () => {
    // TODO: Implement account deletion with confirmation
    console.log('Deleting account...');
  };

  return (
    <div className="space-y-6">
      {/* Data Collection */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">Data Collection</h3>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Analytics</p>
            <p className="text-xs text-muted-foreground">Help improve the app by sharing usage data</p>
          </div>
          <Checkbox
            checked={settings.privacy.analytics}
            onChange={(e) => handlePrivacyChange('analytics', e.target.checked)}
          />
        </div>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Data Collection</p>
            <p className="text-xs text-muted-foreground">Allow collection of personal data for personalization</p>
          </div>
          <Checkbox
            checked={settings.privacy.data_collection}
            onChange={(e) => handlePrivacyChange('data_collection', e.target.checked)}
          />
        </div>
      </div>

      {/* Data Retention */}
      <div className="space-y-3">
        <div>
          <h3 className="text-sm font-medium">Data Retention</h3>
          <p className="text-xs text-muted-foreground">How long to keep your data</p>
        </div>
        <Select value={settings.privacy.data_retention_days.toString()} onValueChange={(value) => handleRetentionChange(parseInt(value))}>
          <SelectTrigger className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="30">30 days</SelectItem>
            <SelectItem value="90">90 days</SelectItem>
            <SelectItem value="365">1 year</SelectItem>
            <SelectItem value="730">2 years</SelectItem>
            <SelectItem value="1825">5 years</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Profile Visibility */}
      <div className="space-y-3">
        <div>
          <h3 className="text-sm font-medium">Profile Visibility</h3>
          <p className="text-xs text-muted-foreground">Control who can see your profile</p>
        </div>
        <Select value={settings.privacy.profile_visibility} onValueChange={handleVisibilityChange}>
          <SelectTrigger className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="private">Private</SelectItem>
            <SelectItem value="friends">Friends Only</SelectItem>
            <SelectItem value="public">Public</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Data Management */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">Data Management</h3>

        <div className="flex items-center gap-3">
          <Button variant="outline" onClick={handleExportData} className="flex items-center gap-2">
            <Download className="h-4 w-4" />
            Export Data
          </Button>
          <p className="text-xs text-muted-foreground">Download a copy of all your data</p>
        </div>
      </div>

      {/* Danger Zone */}
      <div className="space-y-4 p-4 border border-destructive/20 rounded-lg bg-destructive/5">
        <div className="flex items-center gap-2">
          <AlertTriangle className="h-4 w-4 text-destructive" />
          <h3 className="text-sm font-medium text-destructive">Danger Zone</h3>
        </div>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Delete Account</p>
            <p className="text-xs text-muted-foreground">Permanently delete your account and all data</p>
          </div>
          <Button variant="destructive" onClick={handleDeleteAccount} className="flex items-center gap-2">
            <Trash2 className="h-4 w-4" />
            Delete
          </Button>
        </div>
      </div>
    </div>
  );
}