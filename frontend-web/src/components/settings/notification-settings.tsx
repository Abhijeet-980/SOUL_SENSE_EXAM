'use client';

import {
    Bell,
    Mail,
    Smartphone,
    Clock,
    ShieldAlert,
    Settings2,
    CalendarDays
} from 'lucide-react';
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
    Switch,
    Select,
    Input
} from '@/components/ui';
import { motion } from 'framer-motion';
import { cn } from '@/lib/utils';

export interface NotificationSettingsObject {
    masterEnabled: boolean;
    emailEnabled: boolean;
    pushEnabled: boolean;
    frequency: 'instant' | 'daily' | 'weekly';
    quietHours: {
        start: string;
        end: string;
    };
}

export interface NotificationSettingsProps {
    settings: NotificationSettingsObject;
    onChange: (updatedSettings: Partial<NotificationSettingsObject>) => void;
}

/**
 * NotificationSettings Component
 * 
 * Provides a premium interface for managing notification preferences,
 * including master toggle, email/push controls, frequency, and quiet hours.
 */
export function NotificationSettings({
    settings,
    onChange
}: NotificationSettingsProps) {

    const handleToggle = (key: keyof Pick<NotificationSettingsObject, 'masterEnabled' | 'emailEnabled' | 'pushEnabled'>) => {
        onChange({ [key]: !settings[key] });
    };

    const handleFrequencyChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
        onChange({ frequency: e.target.value as NotificationSettingsObject['frequency'] });
    };

    const handleQuietHoursChange = (key: 'start' | 'end', value: string) => {
        onChange({
            quietHours: {
                ...settings.quietHours,
                [key]: value
            }
        });
    };

    // Push support check (simulated / generic)
    const isPushSupported = typeof window !== 'undefined' && 'Notification' in window;

    return (
        <div className="max-w-4xl mx-auto space-y-8 p-1">
            {/* Header Section */}
            <motion.div
                initial={{ opacity: 0, y: -20 }}
                animate={{ opacity: 1, y: 0 }}
                className="flex flex-col gap-2"
            >
                <div className="flex items-center gap-3">
                    <div className="p-3 rounded-2xl bg-primary/10 text-primary">
                        <Bell className="h-8 w-8" />
                    </div>
                    <div>
                        <h2 className="text-3xl font-bold tracking-tight bg-gradient-to-r from-primary to-primary/60 bg-clip-text text-transparent flex items-center gap-2">
                            Notification Preferences
                        </h2>
                        <p className="text-muted-foreground">
                            Choose how and when you want to be notified about your progress and alerts.
                        </p>
                    </div>
                </div>
            </motion.div>

            {/* Master Control Card */}
            <motion.div
                initial={{ opacity: 0, scale: 0.98 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.1 }}
            >
                <Card className={cn(
                    "overflow-hidden border-primary/10 transition-all duration-300",
                    !settings.masterEnabled && "bg-muted/30 opacity-80"
                )}>
                    <CardHeader className="bg-primary/5">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                                <div className="p-2 rounded-lg bg-primary/10 text-primary">
                                    <ShieldAlert className="h-5 w-5" />
                                </div>
                                <div>
                                    <CardTitle className="text-lg">Master Notifications</CardTitle>
                                    <CardDescription>Global toggle for all platform alerts</CardDescription>
                                </div>
                            </div>
                            <Switch
                                checked={settings.masterEnabled}
                                onCheckedChange={() => handleToggle('masterEnabled')}
                                className="scale-110"
                            />
                        </div>
                    </CardHeader>
                    <CardContent className="pt-6">
                        <div className="space-y-8">
                            {/* Channels Section */}
                            <div className="space-y-4">
                                <h4 className="text-sm font-bold uppercase tracking-widest text-muted-foreground flex items-center gap-2">
                                    <Settings2 className="h-4 w-4" />
                                    Delivery Channels
                                </h4>

                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <div className={cn(
                                        "flex items-start justify-between p-4 rounded-xl border border-border transition-all",
                                        !settings.masterEnabled && "opacity-50 pointer-events-none grayscale-[0.5]"
                                    )}>
                                        <div className="flex gap-3">
                                            <div className="mt-1 p-2 rounded-md bg-sky-500/10 text-sky-500">
                                                <Mail className="h-4 w-4" />
                                            </div>
                                            <div className="space-y-1">
                                                <p className="text-sm font-semibold">Email Notifications</p>
                                                <p className="text-xs text-muted-foreground">Receive weekly summaries and important alerts via email.</p>
                                            </div>
                                        </div>
                                        <Switch
                                            checked={settings.emailEnabled}
                                            onCheckedChange={() => handleToggle('emailEnabled')}
                                            disabled={!settings.masterEnabled}
                                        />
                                    </div>

                                    <div className={cn(
                                        "flex items-start justify-between p-4 rounded-xl border border-border transition-all",
                                        !settings.masterEnabled && "opacity-50 pointer-events-none grayscale-[0.5]"
                                    )}>
                                        <div className="flex gap-3">
                                            <div className="mt-1 p-2 rounded-md bg-indigo-500/10 text-indigo-500">
                                                <Smartphone className="h-4 w-4" />
                                            </div>
                                            <div className="space-y-1">
                                                <div className="flex items-center gap-2">
                                                    <p className="text-sm font-semibold">Push Notifications</p>
                                                    {!isPushSupported && (
                                                        <span className="text-[10px] bg-muted px-1.5 py-0.5 rounded text-muted-foreground">Unsupported</span>
                                                    )}
                                                </div>
                                                <p className="text-xs text-muted-foreground">Instant alerts on your browser or mobile device.</p>
                                            </div>
                                        </div>
                                        <Switch
                                            checked={settings.pushEnabled}
                                            onCheckedChange={() => handleToggle('pushEnabled')}
                                            disabled={!settings.masterEnabled || !isPushSupported}
                                        />
                                    </div>
                                </div>
                            </div>

                            {/* Frequency Section */}
                            <div className={cn(
                                "space-y-4 transition-all",
                                !settings.masterEnabled && "opacity-50 pointer-events-none"
                            )}>
                                <h4 className="text-sm font-bold uppercase tracking-widest text-muted-foreground flex items-center gap-2">
                                    <Clock className="h-4 w-4" />
                                    Notification Frequency
                                </h4>
                                <div className="max-w-xs">
                                    <Select
                                        value={settings.frequency}
                                        onChange={handleFrequencyChange}
                                        disabled={!settings.masterEnabled}
                                    >
                                        <option value="instant">Instant</option>
                                        <option value="daily">Daily Digest</option>
                                        <option value="weekly">Weekly Summary</option>
                                    </Select>
                                    <p className="text-[11px] text-muted-foreground mt-2 px-1">
                                        Note: Critical security alerts are always sent instantly regardless of this setting.
                                    </p>
                                </div>
                            </div>

                            {/* Quiet Hours Section */}
                            <div className={cn(
                                "space-y-4 pt-4 border-t border-border transition-all",
                                !settings.masterEnabled && "opacity-50 pointer-events-none"
                            )}>
                                <h4 className="text-sm font-bold uppercase tracking-widest text-muted-foreground flex items-center gap-2">
                                    <CalendarDays className="h-4 w-4" />
                                    Quiet Hours
                                </h4>
                                <p className="text-xs text-muted-foreground max-w-lg">
                                    We won't send any non-critical notifications during this time period.
                                </p>

                                <div className="flex flex-col sm:flex-row items-center gap-4 pt-2">
                                    <div className="w-full sm:w-48 space-y-1.5">
                                        <label className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider">Start Time</label>
                                        <div className="relative group">
                                            <Input
                                                type="time"
                                                value={settings.quietHours.start}
                                                onChange={(e) => handleQuietHoursChange('start', e.target.value)}
                                                disabled={!settings.masterEnabled}
                                                className="bg-muted/20 focus:bg-background transition-colors"
                                            />
                                        </div>
                                    </div>
                                    <div className="hidden sm:block text-muted-foreground mt-6 font-bold">to</div>
                                    <div className="w-full sm:w-48 space-y-1.5">
                                        <label className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider">End Time</label>
                                        <div className="relative group">
                                            <Input
                                                type="time"
                                                value={settings.quietHours.end}
                                                onChange={(e) => handleQuietHoursChange('end', e.target.value)}
                                                disabled={!settings.masterEnabled}
                                                className="bg-muted/20 focus:bg-background transition-colors"
                                            />
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </motion.div>

            {/* Information box */}
            <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.5 }}
                className="p-4 rounded-xl bg-primary/5 border border-primary/10 text-xs text-muted-foreground text-center"
            >
                Changes are saved automatically to your profile.
            </motion.div>
        </div>
    );
}
