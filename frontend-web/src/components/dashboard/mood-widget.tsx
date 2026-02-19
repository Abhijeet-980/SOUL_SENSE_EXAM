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
