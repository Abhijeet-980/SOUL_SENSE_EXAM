'use client';

import { UserSettings } from '../../lib/api/settings';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui';
import { Checkbox } from '@/components/ui';
import { useDebounce } from '@/hooks/useDebounce';

interface NotificationSettingsProps {
  settings: UserSettings;
  onChange: (updates: Partial<UserSettings>) => void;
}

export function NotificationSettings({ settings, onChange }: NotificationSettingsProps) {
  const debouncedOnChange = useDebounce(onChange, 500);

  const handleNotificationChange = (key: 'email' | 'push', value: boolean) => {
    debouncedOnChange({
      notifications: {
        ...settings.notifications,
        [key]: value,
      },
    });
  };

  const handleFrequencyChange = (frequency: 'immediate' | 'daily' | 'weekly') => {
    debouncedOnChange({
      notifications: {
        ...settings.notifications,
        frequency,
      },
    });
  };

  const handleTypeChange = (type: keyof UserSettings['notifications']['types'], value: boolean) => {
    debouncedOnChange({
      notifications: {
        ...settings.notifications,
        types: {
          ...settings.notifications.types,
          [type]: value,
        },
      },
    });
  };

  return (
    <div className="space-y-6">
      {/* General Notification Settings */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">General</h3>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Email Notifications</p>
            <p className="text-xs text-muted-foreground">Receive notifications via email</p>
          </div>
          <Checkbox
            checked={settings.notifications.email}
            onChange={(e) => handleNotificationChange('email', e.target.checked)}
          />
        </div>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Push Notifications</p>
            <p className="text-xs text-muted-foreground">Receive push notifications in your browser</p>
          </div>
          <Checkbox
            checked={settings.notifications.push}
            onChange={(e) => handleNotificationChange('push', e.target.checked)}
          />
        </div>

        <div className="space-y-3">
          <div>
            <p className="text-sm font-medium">Notification Frequency</p>
            <p className="text-xs text-muted-foreground">How often to receive notifications</p>
          </div>
          <Select value={settings.notifications.frequency} onValueChange={handleFrequencyChange}>
            <SelectTrigger className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="immediate">Immediate</SelectItem>
              <SelectItem value="daily">Daily Digest</SelectItem>
              <SelectItem value="weekly">Weekly Summary</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* Notification Types */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">Notification Types</h3>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Exam Reminders</p>
            <p className="text-xs text-muted-foreground">Reminders for upcoming exams</p>
          </div>
          <Checkbox
            checked={settings.notifications.types.exam_reminders}
            onChange={(e) => handleTypeChange('exam_reminders', e.target.checked)}
          />
        </div>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Journal Prompts</p>
            <p className="text-xs text-muted-foreground">Daily journal writing prompts</p>
          </div>
          <Checkbox
            checked={settings.notifications.types.journal_prompts}
            onChange={(e) => handleTypeChange('journal_prompts', e.target.checked)}
          />
        </div>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Progress Updates</p>
            <p className="text-xs text-muted-foreground">Updates on your emotional intelligence progress</p>
          </div>
          <Checkbox
            checked={settings.notifications.types.progress_updates}
            onChange={(e) => handleTypeChange('progress_updates', e.target.checked)}
          />
        </div>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">System Updates</p>
            <p className="text-xs text-muted-foreground">New features and system announcements</p>
          </div>
          <Checkbox
            checked={settings.notifications.types.system_updates}
            onChange={(e) => handleTypeChange('system_updates', e.target.checked)}
          />
        </div>
      </div>
    </div>
  );
}