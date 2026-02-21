'use client';

import { useEffect, useState } from 'react';
import { BentoGridItem } from './bento-grid';
import { User, Sun, Moon, Sunrise, Sunset, Sparkles } from 'lucide-react';
import { cn } from '@/lib/utils';

export interface WelcomeCardProps {
  userName?: string;
  lastActivity?: string | Date;
}

const MESSAGES = [
  'Ready to check in today?',
  'Your personal growth journey continues here.',
  'Take a moment to reflect on your progress.',
  'Consistency is the key to mindfulness.',
  'Every step counts toward better self-awareness.',
  "Let's make today a great day for growth.",
  'Your emotional wellbeing matters.',
];

export const WelcomeCard = ({ userName, lastActivity }: WelcomeCardProps) => {
  const [mounted, setMounted] = useState(false);
  const [greeting, setGreeting] = useState('');
  const [message, setMessage] = useState('');
  const [daysSince, setDaysSince] = useState<number | null>(null);

  useEffect(() => {
    // 1. Calculate time of day for greeting
    const hour = new Date().getHours();
    let timeGreeting = 'Good evening';
    if (hour >= 5 && hour < 12) timeGreeting = 'Good morning';
    else if (hour >= 12 && hour < 17) timeGreeting = 'Good afternoon';

    // Fallback if no user name
    const namePart = userName ? `, ${userName}` : '';
    setGreeting(`${timeGreeting}${namePart}!`);

    // 2. Select consistent rotating message based on day of year
    const start = new Date(new Date().getFullYear(), 0, 0);
    const diff = new Date().getTime() - start.getTime();
    const oneDay = 1000 * 60 * 60 * 24;
    const dayOfYear = Math.floor(diff / oneDay);
    setMessage(MESSAGES[dayOfYear % MESSAGES.length]);

    // 3. Calculate days since last activity securely
    if (lastActivity) {
      const lastDate = new Date(lastActivity);
      if (!isNaN(lastDate.getTime())) {
        const now = new Date();
        now.setHours(0, 0, 0, 0);
        lastDate.setHours(0, 0, 0, 0);

        const dayDiff = Math.floor((now.getTime() - lastDate.getTime()) / oneDay);
        // Ensure no negative days due to timezones
        if (dayDiff >= 0) {
          setDaysSince(dayDiff);
        }
      }
    }

    setMounted(true);
  }, [userName, lastActivity]);

  const renderIcon = () => {
    if (!mounted) return <User className="h-12 w-12 text-blue-500" />;

    const hour = new Date().getHours();
    if (hour >= 5 && hour < 12) return <Sunrise className="h-12 w-12 text-amber-500" />;
    if (hour >= 12 && hour < 17) return <Sun className="h-12 w-12 text-orange-500" />;
    if (hour >= 17 && hour < 21) return <Sunset className="h-12 w-12 text-rose-500" />;
    return <Moon className="h-12 w-12 text-indigo-500" />;
  };

  return (
    <BentoGridItem
      title={<span className="truncate block max-w-full">{mounted ? greeting : 'Welcome!'}</span>}
      description={
        <div className="space-y-4">
          <p className="text-muted-foreground">{mounted ? message : 'Loading your journey...'}</p>

          {mounted && daysSince !== null && (
            <div className="flex items-center gap-2 text-sm font-medium">
              <Sparkles className="h-4 w-4 text-primary" />
              <span
                className={cn(
                  'px-2 py-1 rounded-full bg-primary/10 text-primary',
                  daysSince === 0 && 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400'
                )}
              >
                {daysSince === 0
                  ? 'You checked in today!'
                  : daysSince === 1
                    ? 'You checked in yesterday!'
                    : `Active ${daysSince} days ago`}
              </span>
            </div>
          )}
        </div>
      }
      header={
        <div className="flex flex-1 w-full h-full min-h-[6rem] rounded-xl bg-gradient-to-br from-blue-500/20 via-purple-500/10 to-purple-500/20 items-center justify-center transition-all duration-500 hover:scale-[1.02]">
          {renderIcon()}
        </div>
      }
      icon={<User className="h-4 w-4" />}
      className="md:col-span-2 shadow-sm hover:shadow-md transition-shadow duration-300"
    />
  );
};
