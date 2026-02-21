'use client';

import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { motion, AnimatePresence } from 'framer-motion';
import { Area, AreaChart, ResponsiveContainer } from 'recharts';
import { cn } from '@/lib/utils';
import Link from 'next/link';
import { ChevronRight } from 'lucide-react';

// Types
export type MoodRating = 1 | 2 | 3 | 4 | 5;

export interface DailyMood {
    date: string;
    score: MoodRating;
}

interface MoodWidgetProps {
    hasLoggedToday?: boolean;
    todaysMood?: MoodRating;
    recentMoods?: DailyMood[]; // Expecting sorted array, last 7 days
    onQuickLog?: (mood: MoodRating) => void;
    className?: string;
}

// Configuration for Moods
const MOOD_OPTIONS: { score: MoodRating; emoji: string; label: string; color: string }[] = [
    { score: 1, emoji: '😢', label: 'Terrible', color: 'text-red-500' },
    { score: 2, emoji: '😕', label: 'Bad', color: 'text-orange-500' },
    { score: 3, emoji: '😐', label: 'Okay', color: 'text-yellow-500' },
    { score: 4, emoji: '🙂', label: 'Good', color: 'text-green-500' },
    { score: 5, emoji: '😄', label: 'Great', color: 'text-emerald-500' },
];

export function MoodWidget({
    hasLoggedToday = false,
    todaysMood,
    recentMoods = [],
    onQuickLog,
    className,
}: MoodWidgetProps) {
    // Local state to handle optimistic updates or animation states if needed
    const [isHovering, setIsHovering] = useState<MoodRating | null>(null);

    // Prepare data for the mini trend (last 7 days)
    // Ensure we have data for the chart, fallback to simple placeholder if empty
    const chartData = recentMoods.map((m, i) => ({ index: i, score: m.score }));

    const currentMoodConfig = todaysMood ? MOOD_OPTIONS.find((m) => m.score === todaysMood) : null;

    return (
        <Card className={cn('h-full w-full overflow-hidden shadow-md transition-shadow hover:shadow-lg', className)}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Daily Mood</CardTitle>
                {/* Mini Trend Indicator - show if we have history */}
                {recentMoods.length > 0 && (
                    <div className="h-8 w-16">
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={chartData}>
                                <defs>
                                    <linearGradient id="colorScore" x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                                        <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                                    </linearGradient>
                                </defs>
                                <Area
                                    type="monotone"
                                    dataKey="score"
                                    stroke="#10b981"
                                    strokeWidth={2}
                                    fillOpacity={1}
                                    fill="url(#colorScore)"
                                    isAnimationActive={false} // clean render
                                />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </CardHeader>
            <CardContent className="p-6 pt-2">
                <AnimatePresence mode="wait">
                    {!hasLoggedToday ? (
                        <motion.div
                            key="selector"
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, y: -10 }}
                            transition={{ duration: 0.3 }}
                            className="flex flex-col items-center space-y-4"
                        >
                            <h3 className="text-lg font-semibold text-foreground/80">How are you feeling?</h3>
                            <div className="flex w-full justify-between gap-1 sm:justify-center sm:gap-4">
                                {MOOD_OPTIONS.map((mood) => (
                                    <motion.button
                                        key={mood.score}
                                        whileHover={{ scale: 1.2, transition: { duration: 0.2 } }}
                                        whileTap={{ scale: 0.9 }}
                                        onClick={() => onQuickLog?.(mood.score)}
                                        onMouseEnter={() => setIsHovering(mood.score)}
                                        onMouseLeave={() => setIsHovering(null)}
                                        className="flex flex-col items-center justify-center p-2 focus:outline-none"
                                        aria-label={`Log mood: ${mood.label}`}
                                    >
                                        <span className="text-4xl leading-none filter drop-shadow-sm transition-all hover:drop-shadow-md">
                                            {mood.emoji}
                                        </span>
                                        <span
                                            className={cn(
                                                'mt-1 text-[10px] font-medium opacity-0 transition-opacity',
                                                isHovering === mood.score ? 'opacity-100' : 'opacity-0'
                                            )}
                                        >
                                            {mood.label}
                                        </span>
                                    </motion.button>
                                ))}
                            </div>
                        </motion.div>
                    ) : (
                        <motion.div
                            key="logged"
                            initial={{ opacity: 0, scale: 0.95 }}
                            animate={{ opacity: 1, scale: 1 }}
                            transition={{ type: 'spring', stiffness: 300, damping: 20 }}
                            className="flex flex-col items-center text-center"
                        >
                            <div className="relative mb-2">
                                <motion.div
                                    initial={{ scale: 0 }}
                                    animate={{ scale: 1 }}
                                    transition={{ delay: 0.2, type: 'spring' }}
                                    className="flex h-20 w-20 items-center justify-center rounded-full bg-secondary/30 text-6xl shadow-inner"
                                >
                                    {currentMoodConfig?.emoji || '😐'}
                                </motion.div>
                                <div className="absolute -bottom-1 -right-1 flex h-6 w-6 items-center justify-center rounded-full bg-primary text-xs text-primary-foreground shadow-sm">
                                    <ChevronRight className="h-3 w-3" />
                                </div>
                            </div>

                            <div className="mb-4">
                                <p className="text-sm font-medium text-muted-foreground">Today's Mood</p>
                                <p className={cn('text-2xl font-bold', currentMoodConfig?.color || 'text-foreground')}>
                                    {currentMoodConfig?.label || 'Logged'}
                                </p>
                            </div>

                            <Button variant="outline" className="w-full gap-2 text-xs" asChild>
                                <Link href="/journal">
                                    View Journal
                                    <ChevronRight className="h-3 w-3" />
                                </Link>
                            </Button>
                        </motion.div>
                    )}
                </AnimatePresence>
            </CardContent>
        </Card>
    );
}
import { motion, AnimatePresence } from 'framer-motion';
import { BentoGridItem } from './bento-grid';
import { History, ArrowRight } from 'lucide-react';
import { cn } from '@/lib/utils';
import Link from 'next/link';

const EMOJIS = [
  { char: '😢', label: 'Very Low', rating: 1, color: 'text-red-500 bg-red-500/10' },
  { char: '😕', label: 'Low', rating: 2, color: 'text-orange-500 bg-orange-500/10' },
  { char: '😐', label: 'Neutral', rating: 3, color: 'text-yellow-500 bg-yellow-500/10' },
  { char: '🙂', label: 'Good', rating: 4, color: 'text-blue-500 bg-blue-500/10' },
  { char: '😄', label: 'Great', rating: 5, color: 'text-green-500 bg-green-500/10' },
];

export const MoodWidget = () => {
  const [loggedMood, setLoggedMood] = useState<number | null>(null);

  // Mock trend data for last 7 days (1-5 scale)
  const trendData = [3, 4, 3, 5, 4, 2, 4];

  const handleMoodSelect = (rating: number) => {
    // Simulate logging
    setLoggedMood(rating);
  };

  const selectedEmoji = EMOJIS.find((e) => e.rating === loggedMood);

  return (
    <BentoGridItem
      title="Daily Check-in"
      description={
        loggedMood ? "Good to know how you're feeling." : 'How are you feeling right now?'
      }
      className="md:col-span-1 border-none shadow-none p-0 group overflow-hidden"
    >
      <div className="flex flex-col h-full bg-white/60 dark:bg-black/40 backdrop-blur-xl rounded-3xl p-6 border border-white/20 transition-all group-hover:shadow-2xl">
        <div className="flex-1 flex flex-col justify-center">
          <AnimatePresence mode="wait">
            {!loggedMood ? (
              <motion.div
                key="selector"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className="flex justify-between items-center gap-2"
              >
                {EMOJIS.map((mood) => (
                  <motion.button
                    key={mood.rating}
                    whileHover={{ scale: 1.2, y: -5 }}
                    whileTap={{ scale: 0.9 }}
                    onClick={() => handleMoodSelect(mood.rating)}
                    className={cn(
                      'flex-1 aspect-square rounded-2xl flex items-center justify-center text-2xl transition-all border border-transparent hover:border-white/40 shadow-sm',
                      'bg-neutral-100/50 dark:bg-neutral-800/50 hover:bg-white dark:hover:bg-neutral-700'
                    )}
                    title={mood.label}
                  >
                    {mood.char}
                  </motion.button>
                ))}
              </motion.div>
            ) : (
              <motion.div
                key="logged"
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                className="flex flex-col items-center py-4"
              >
                <div className="text-6xl mb-3 drop-shadow-xl">{selectedEmoji?.char}</div>
                <div
                  className={cn(
                    'px-4 py-1 rounded-full text-xs font-bold border mb-4',
                    selectedEmoji?.color,
                    'border-current/20'
                  )}
                >
                  {selectedEmoji?.label}
                </div>
                <Link
                  href="/journal"
                  className="group/link flex items-center gap-1 text-sm text-blue-600 dark:text-blue-400 font-semibold hover:underline"
                >
                  Complete Journal
                  <ArrowRight className="h-3.5 w-3.5 group-hover/link:translate-x-1 transition-transform" />
                </Link>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Mini Trend */}
        <div className="mt-6 pt-4 border-t border-neutral-200/50 dark:border-neutral-800/50">
          <div className="flex items-center justify-between mb-2">
            <span className="text-[10px] uppercase tracking-wider font-bold text-neutral-400">
              Past 7 Days
            </span>
            <History className="h-3 w-3 text-neutral-400" />
          </div>
          <div className="flex items-end justify-between h-8 gap-1 px-1">
            {trendData.map((val, i) => (
              <motion.div
                key={i}
                initial={{ height: 0 }}
                animate={{ height: `${(val / 5) * 100}%` }}
                className={cn(
                  'flex-1 rounded-full min-h-[4px]',
                  val >= 4 ? 'bg-green-500/40' : val <= 2 ? 'bg-red-500/40' : 'bg-yellow-500/40'
                )}
              />
            ))}
          </div>
        </div>
      </div>
    </BentoGridItem>
  );
};
