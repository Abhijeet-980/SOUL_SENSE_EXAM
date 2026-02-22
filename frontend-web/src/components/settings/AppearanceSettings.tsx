'use client';

import { UserSettings } from '../../lib/api/settings';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui';
import { Checkbox } from '../ui';
import { useDebounce } from '../../hooks/useDebounce';
import { ThemeToggle } from './theme-toggle';

interface AppearanceSettingsProps {
  settings: UserSettings;
  onChange: (updates: Partial<UserSettings>) => void;
}

export function AppearanceSettings({ settings, onChange }: AppearanceSettingsProps) {
  const debouncedOnChange = useDebounce(onChange, 500);

  const handleThemeChange = (theme: 'light' | 'dark' | 'system') => {
    debouncedOnChange({ theme });
  };

  const handleAccessibilityChange = (key: 'high_contrast' | 'reduced_motion', value: boolean) => {
    debouncedOnChange({
      accessibility: {
        ...settings.accessibility,
        [key]: value,
      },
    });
  };

  const handleFontSizeChange = (fontSize: 'small' | 'medium' | 'large') => {
    debouncedOnChange({
      accessibility: {
        ...settings.accessibility,
        font_size: fontSize,
      },
    });
  };

  return (
    <div className="space-y-6">
      {/* Theme Selection */}
      <ThemeToggle
        value={settings.theme as 'light' | 'dark' | 'system'}
        onChange={handleThemeChange}
      />

      {/* Font Size */}
      <div className="space-y-3">
        <div>
          <h3 className="text-sm font-medium">Font Size</h3>
          <p className="text-xs text-muted-foreground">Adjust text size for better readability</p>
        </div>
        <Select value={settings.accessibility.font_size} onValueChange={handleFontSizeChange}>
          <SelectTrigger className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="small">Small</SelectItem>
            <SelectItem value="medium">Medium</SelectItem>
            <SelectItem value="large">Large</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Accessibility Options */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">Accessibility</h3>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">High Contrast</p>
            <p className="text-xs text-muted-foreground">Increase contrast for better visibility</p>
          </div>
          <Checkbox
            checked={settings.accessibility.high_contrast}
            onChange={(e) => handleAccessibilityChange('high_contrast', e.target.checked)}
          />
        </div>

        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-medium">Reduced Motion</p>
            <p className="text-xs text-muted-foreground">Minimize animations and transitions</p>
          </div>
          <Checkbox
            checked={settings.accessibility.reduced_motion}
            onChange={(e) => handleAccessibilityChange('reduced_motion', e.target.checked)}
          />
        </div>
      </div>
    </div>
  );
}