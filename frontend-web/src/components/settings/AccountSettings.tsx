'use client';

import { UserSettings } from '@/lib/api/settings';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui';
import { Button } from '@/components/ui';
import { Input } from '@/components/ui';
import { Label } from '@/components/ui';
import { useDebounce } from '@/hooks/useDebounce';
import { useState } from 'react';

interface AccountSettingsProps {
  settings: UserSettings;
  onChange: (updates: Partial<UserSettings>) => void;
}

export function AccountSettings({ settings, onChange }: AccountSettingsProps) {
  const debouncedOnChange = useDebounce(onChange, 500);
  const [passwordData, setPasswordData] = useState({
    current: '',
    new: '',
    confirm: '',
  });

  const handleAccountChange = (key: 'language' | 'timezone' | 'date_format', value: string) => {
    debouncedOnChange({
      account: {
        ...settings.account,
        [key]: value,
      },
    });
  };

  const handlePasswordChange = (field: 'current' | 'new' | 'confirm', value: string) => {
    setPasswordData(prev => ({ ...prev, [field]: value }));
  };

  const handlePasswordSubmit = () => {
    if (passwordData.new !== passwordData.confirm) {
      // TODO: Show error message
      return;
    }
    // TODO: Implement password change API call
    console.log('Changing password...');
    setPasswordData({ current: '', new: '', confirm: '' });
  };

  const handleConnectGoogle = () => {
    // TODO: Implement Google OAuth connection
    console.log('Connecting Google account...');
  };

  const handleDisconnectGoogle = () => {
    // TODO: Implement Google OAuth disconnection
    console.log('Disconnecting Google account...');
  };

  return (
    <div className="space-y-6">
      {/* Account Preferences */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">Preferences</h3>

        <div className="space-y-3">
          <div>
            <Label htmlFor="language">Language</Label>
            <Select value={settings.account.language} onValueChange={(value) => handleAccountChange('language', value)}>
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="en">English</SelectItem>
                <SelectItem value="es">Español</SelectItem>
                <SelectItem value="fr">Français</SelectItem>
                <SelectItem value="de">Deutsch</SelectItem>
                <SelectItem value="it">Italiano</SelectItem>
                <SelectItem value="pt">Português</SelectItem>
                <SelectItem value="ru">Русский</SelectItem>
                <SelectItem value="ja">日本語</SelectItem>
                <SelectItem value="ko">한국어</SelectItem>
                <SelectItem value="zh">中文</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div>
            <Label htmlFor="timezone">Timezone</Label>
            <Select value={settings.account.timezone} onValueChange={(value) => handleAccountChange('timezone', value)}>
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="UTC">UTC</SelectItem>
                <SelectItem value="America/New_York">Eastern Time</SelectItem>
                <SelectItem value="America/Chicago">Central Time</SelectItem>
                <SelectItem value="America/Denver">Mountain Time</SelectItem>
                <SelectItem value="America/Los_Angeles">Pacific Time</SelectItem>
                <SelectItem value="Europe/London">London</SelectItem>
                <SelectItem value="Europe/Paris">Paris</SelectItem>
                <SelectItem value="Europe/Berlin">Berlin</SelectItem>
                <SelectItem value="Asia/Tokyo">Tokyo</SelectItem>
                <SelectItem value="Asia/Shanghai">Shanghai</SelectItem>
                <SelectItem value="Australia/Sydney">Sydney</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div>
            <Label htmlFor="date-format">Date Format</Label>
            <Select value={settings.account.date_format} onValueChange={(value) => handleAccountChange('date_format', value)}>
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="MM/DD/YYYY">MM/DD/YYYY</SelectItem>
                <SelectItem value="DD/MM/YYYY">DD/MM/YYYY</SelectItem>
                <SelectItem value="YYYY-MM-DD">YYYY-MM-DD</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      {/* Password Change */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">Change Password</h3>

        <div className="space-y-3">
          <div>
            <Label htmlFor="current-password">Current Password</Label>
            <Input
              id="current-password"
              type="password"
              value={passwordData.current}
              onChange={(e) => handlePasswordChange('current', e.target.value)}
              placeholder="Enter current password"
            />
          </div>

          <div>
            <Label htmlFor="new-password">New Password</Label>
            <Input
              id="new-password"
              type="password"
              value={passwordData.new}
              onChange={(e) => handlePasswordChange('new', e.target.value)}
              placeholder="Enter new password"
            />
          </div>

          <div>
            <Label htmlFor="confirm-password">Confirm New Password</Label>
            <Input
              id="confirm-password"
              type="password"
              value={passwordData.confirm}
              onChange={(e) => handlePasswordChange('confirm', e.target.value)}
              placeholder="Confirm new password"
            />
          </div>

          <Button onClick={handlePasswordSubmit} disabled={!passwordData.current || !passwordData.new || !passwordData.confirm}>
            Change Password
          </Button>
        </div>
      </div>

      {/* Connected Accounts */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">Connected Accounts</h3>

        <div className="flex items-center justify-between p-4 border rounded-lg">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-red-500 rounded-full flex items-center justify-center text-white font-bold">
              G
            </div>
            <div>
              <p className="text-sm font-medium">Google</p>
              <p className="text-xs text-muted-foreground">Connect your Google account for easier sign-in</p>
            </div>
          </div>
          <Button variant="outline" onClick={handleConnectGoogle}>
            Connect
          </Button>
        </div>
      </div>
    </div>
  );
}