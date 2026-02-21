'use client';

import { UserSettings } from '@/lib/api/settings';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui';
import { Checkbox } from '@/components/ui';
import { useDebounce } from '@/hooks/useDebounce';
import { Sun, Moon, Monitor, Type, Eye } from 'lucide-react';

interface ThemeToggleProps {
  settings: UserSettings;
  onChange: (updates: Partial<UserSettings>) => void;
}

export function ThemeToggle({ settings, onChange }: ThemeToggleProps) {
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
    <div className="space-y-10">
      {/* Theme Selection */}
      <div className="space-y-4">
        <div className="flex items-center gap-2 text-muted-foreground/60">
          <Monitor className="h-3.5 w-3.5" />
          <h3 className="text-[10px] uppercase tracking-widest font-black">Interface Theme</h3>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          {(['light', 'dark', 'system'] as const).map((t) => (
            <button
              key={t}
              onClick={() => handleThemeChange(t)}
              className={`p-4 rounded-2xl border transition-all flex flex-col items-center gap-3 ${
                settings.theme === t
                  ? 'bg-primary/5 border-primary text-primary shadow-sm'
                  : 'bg-muted/10 border-border/40 text-muted-foreground hover:bg-muted/20'
              }`}
            >
              {t === 'light' && <Sun className="h-5 w-5" />}
              {t === 'dark' && <Moon className="h-5 w-5" />}
              {t === 'system' && <Monitor className="h-5 w-5" />}
              <span className="text-xs font-bold capitalize">{t}</span>
            </button>
          ))}
        </div>
      </div>

      {/* Font Size */}
      <div className="space-y-4">
        <div className="flex items-center gap-2 text-muted-foreground/60">
          <Type className="h-3.5 w-3.5" />
          <h3 className="text-[10px] uppercase tracking-widest font-black">Content Typography</h3>
        </div>
        <Select value={settings.accessibility.font_size} onValueChange={handleFontSizeChange}>
          <SelectTrigger className="w-full h-12 rounded-xl bg-muted/10 border-border/40">
            <SelectValue />
          </SelectTrigger>
          <SelectContent className="rounded-xl border-border/40">
            <SelectItem value="small">Comfortable (Small)</SelectItem>
            <SelectItem value="medium">Standard (Medium)</SelectItem>
            <SelectItem value="large">Spacious (Large)</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Accessibility Options */}
      <div className="space-y-4">
        <div className="flex items-center gap-2 text-muted-foreground/60">
          <Eye className="h-3.5 w-3.5" />
          <h3 className="text-[10px] uppercase tracking-widest font-black">Visual Accessibility</h3>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="flex items-center justify-between p-4 rounded-2xl bg-muted/10 border border-border/40 group hover:border-border transition-colors">
            <div className="space-y-0.5">
              <p className="text-sm font-bold">High Contrast</p>
              <p className="text-[10px] text-muted-foreground font-medium">
                Prioritize legibility over aesthetics
              </p>
            </div>
            <Checkbox
              checked={settings.accessibility.high_contrast}
              onChange={(e) => handleAccessibilityChange('high_contrast', e.target.checked)}
              className="h-5 w-5 rounded-lg border-2 border-border/60"
            />
          </div>

          <div className="flex items-center justify-between p-4 rounded-2xl bg-muted/10 border border-border/40 group hover:border-border transition-colors">
            <div className="space-y-0.5">
              <p className="text-sm font-bold">Reduced Motion</p>
              <p className="text-[10px] text-muted-foreground font-medium">
                Minimize animations and transitions
              </p>
            </div>
            <Checkbox
              checked={settings.accessibility.reduced_motion}
              onChange={(e) => handleAccessibilityChange('reduced_motion', e.target.checked)}
              className="h-5 w-5 rounded-lg border-2 border-border/60"
            />
          </div>
        </div>
      </div>
    </div>
  );
}
