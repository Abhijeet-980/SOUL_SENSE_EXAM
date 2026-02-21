'use client';

import { useEffect, useState } from 'react';
import { BentoGridItem } from './bento-grid';
import { User, Sun, Moon, Sunrise, Sunset, Sparkles, Clock } from 'lucide-react';
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
    const hour = new Date().getHours();
    let timeGreeting = 'Good evening';
    if (hour >= 5 && hour < 12) timeGreeting = 'Good morning';
    else if (hour >= 12 && hour < 17) timeGreeting = 'Good afternoon';

    const namePart = userName ? `, ${userName}` : '';
    setGreeting(`${timeGreeting}${namePart}`);

    // Select message based on day of year
    const start = new Date(new Date().getFullYear(), 0, 0);
    const diff = new Date().getTime() - start.getTime();
    const oneDay = 1000 * 60 * 60 * 24;
    const dayOfYear = Math.floor(diff / oneDay);
    setMessage(MESSAGES[dayOfYear % MESSAGES.length]);

    if (lastActivity) {
      const lastDate = new Date(lastActivity);
      if (!isNaN(lastDate.getTime())) {
        const now = new Date();
        now.setHours(0, 0, 0, 0);
        lastDate.setHours(0, 0, 0, 0);

        const dayDiff = Math.floor((now.getTime() - lastDate.getTime()) / oneDay);
        if (dayDiff >= 0) {
          setDaysSince(dayDiff);
        }
      }
    }

    setMounted(true);
  }, [userName, lastActivity]);

  const renderIcon = () => {
    if (!mounted) return <User className="h-8 w-8 text-primary/40" />;

    const hour = new Date().getHours();
    if (hour >= 5 && hour < 12) return <Sunrise className="h-8 w-8 text-orange-500/60" />;
    if (hour >= 12 && hour < 17) return <Sun className="h-8 w-8 text-yellow-500/60" />;
    if (hour >= 17 && hour < 21) return <Sunset className="h-8 w-8 text-orange-400/60" />;
    return <Moon className="h-8 w-8 text-indigo-400/60" />;
  };

  return (
    <BentoGridItem
      title={
        <div className="flex items-center gap-2">
          {renderIcon()}
          <span className="truncate font-bold tracking-tight text-2xl">
            {mounted ? greeting : 'Welcome'}
          </span>
        </div>
      }
      description={
        <div className="space-y-4 mt-2">
          <p className="text-muted-foreground text-lg leading-relaxed font-medium">
            {mounted ? message : 'Loading...'}
          </p>

          {mounted && daysSince !== null && (
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-primary/70">
              <Clock className="h-3.5 w-3.5" />
              <span>
                {daysSince === 0
                  ? 'Last activity: Today'
                  : daysSince === 1
                    ? 'Last activity: Yesterday'
                    : `Last activity: ${daysSince} days ago`}
              </span>
            </div>
          )}
        </div>
      }
      header={
        <div className="flex flex-1 w-full h-full min-h-[6rem] rounded-2xl bg-muted/20 border border-border/50 items-center justify-center relative overflow-hidden group">
          <div className="absolute inset-0 bg-gradient-to-br from-primary/5 to-transparent opacity-50 group-hover:opacity-100 transition-opacity duration-500" />
          <Sparkles className="h-12 w-12 text-primary/10 group-hover:text-primary/20 transition-colors duration-500" />
        </div>
      }
      className="md:col-span-2 bg-background/60 backdrop-blur-md border border-border/40 shadow-sm"
    />
  );
};
