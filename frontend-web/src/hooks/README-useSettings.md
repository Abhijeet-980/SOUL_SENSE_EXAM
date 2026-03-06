# useSettings Hook

A React hook for managing user settings operations with full CRUD functionality and cloud synchronization.

## Features

- ✅ Load user settings from API
- ✅ Update settings with optimistic updates
- ✅ Sync settings to cloud
- ✅ Error handling and loading states
- ✅ TypeScript support

## Usage

```typescript
import { useSettings } from '@/hooks/useSettings';

function SettingsComponent() {
  const { settings, isLoading, error, updateSettings, syncSettings } = useSettings();

  const handleThemeChange = async (theme: 'light' | 'dark' | 'system') => {
    try {
      await updateSettings({ theme });
    } catch (err) {
      console.error('Failed to update theme:', err);
    }
  };

  const handleSync = async () => {
    try {
      await syncSettings();
    } catch (err) {
      console.error('Failed to sync:', err);
    }
  };

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div>
      <p>Current theme: {settings?.theme}</p>
      <button onClick={() => handleThemeChange('dark')}>Set Dark Theme</button>
      <button onClick={handleSync}>Sync Settings</button>
    </div>
  );
}
```

## API

### Return Values

- `settings`: Current user settings object or `null`
- `isLoading`: Boolean indicating if any operation is in progress
- `error`: Error message string or `null`
- `updateSettings(data)`: Function to update settings
- `syncSettings()`: Function to sync settings to cloud
- `refetch()`: Function to manually refetch settings

### Settings Structure

```typescript
interface UserSettings {
  theme: 'light' | 'dark' | 'system';
  notifications: {
    enabled: boolean;
    email: boolean;
    push: boolean;
    frequency: 'immediate' | 'daily' | 'weekly' | 'never';
  };
  privacy: {
    share_analytics: boolean;
    data_retention_days: number;
  };
  accessibility: {
    high_contrast: boolean;
    reduced_motion: boolean;
    font_size: 'small' | 'medium' | 'large';
  };
  sync: {
    enabled: boolean;
    last_synced: string | null;
  };
}
```

## API Endpoints

- `GET /api/v1/settings` - Fetch user settings
- `PUT /api/v1/settings` - Update user settings
- `POST /api/v1/settings/sync` - Sync settings to cloud

## Error Handling

The hook provides comprehensive error handling:
- Network errors are caught and displayed
- Failed updates don't break the UI
- Default settings are provided when API fails
- All operations are properly typed

## Test Page

Visit `/settings-test` to see a working example of the hook in action.