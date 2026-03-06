"use client";

import React from 'react';
import { motion } from 'framer-motion';
import { UserStreak } from '@/types/gamification';
import { Flame, Trophy } from 'lucide-react';

interface StreakDisplayProps {
    streaks: UserStreak[];
    className?: string;
}

export const StreakDisplay: React.FC<StreakDisplayProps> = ({ streaks, className = "" }) => {
    const combinedStreak = streaks.find(s => s.activity_type === 'combined') || streaks[0];

    if (!combinedStreak) return null;

    return (
        <div className={`flex items-center gap-6 bg-white dark:bg-slate-800 rounded-2xl p-6 shadow-sm border border-slate-100 dark:border-slate-700 ${className}`}>
            <div className="flex items-center gap-4">
                <div className={`p-3 rounded-xl ${combinedStreak.is_active_today ? 'bg-orange-50 dark:bg-orange-900/20' : 'bg-slate-100 dark:bg-slate-700'}`}>
                    <Flame className={`w-8 h-8 ${combinedStreak.is_active_today ? 'text-orange-500 fill-orange-500' : 'text-slate-400'}`} />
                </div>
                <div>
                    <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Current Streak</div>
                    <div className="text-2xl font-bold text-slate-900 dark:text-white">
                        {combinedStreak.current_streak} Days
                    </div>
                </div>
            </div>

            <div className="h-10 w-px bg-slate-100 dark:bg-slate-700 mx-2" />

            <div className="flex items-center gap-4">
                <div className="p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded-xl">
                    <Trophy className="w-8 h-8 text-yellow-500" />
                </div>
                <div>
                    <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Longest Streak</div>
                    <div className="text-2xl font-bold text-slate-900 dark:text-white">
                        {combinedStreak.longest_streak} Days
                    </div>
                </div>
            </div>
        </div>
    );
};
