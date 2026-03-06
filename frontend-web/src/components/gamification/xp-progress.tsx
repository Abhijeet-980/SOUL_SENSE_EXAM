"use client";

import React from 'react';
import { motion } from 'framer-motion';
import { UserXP } from '@/types/gamification';

interface XPProgressProps {
    xp: UserXP;
    className?: string;
}

export const XPProgress: React.FC<XPProgressProps> = ({ xp, className = "" }) => {
    return (
        <div className={`bg-white dark:bg-slate-800 rounded-2xl p-6 shadow-sm border border-slate-100 dark:border-slate-700 ${className}`}>
            <div className="flex justify-between items-center mb-4">
                <div>
                    <h3 className="text-sm font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Level</h3>
                    <div className="flex items-baseline gap-2">
                        <span className="text-3xl font-bold text-slate-900 dark:text-white">{xp.current_level}</span>
                        <span className="text-sm text-slate-500">Tier {Math.floor(xp.current_level / 10) + 1}</span>
                    </div>
                </div>
                <div className="text-right">
                    <h3 className="text-sm font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Total XP</h3>
                    <span className="text-2xl font-semibold text-indigo-600 dark:text-indigo-400">{xp.total_xp.toLocaleString()}</span>
                </div>
            </div>

            <div className="relative h-4 w-full bg-slate-100 dark:bg-slate-700 rounded-full overflow-hidden mb-2">
                <motion.div
                    initial={{ width: 0 }}
                    animate={{ width: `${xp.level_progress * 100}%` }}
                    transition={{ duration: 1, ease: "easeOut" }}
                    className="absolute top-0 left-0 h-full bg-gradient-to-r from-indigo-500 to-purple-600"
                />
            </div>

            <div className="flex justify-between text-xs font-medium text-slate-400">
                <span>{xp.total_xp} / {xp.xp_to_next_level} XP</span>
                <span>{xp.xp_to_next_level - xp.total_xp} XP to Level {xp.current_level + 1}</span>
            </div>
        </div>
    );
};
