"use client";

import React from 'react';
import { motion } from 'framer-motion';
import { Achievement } from '@/types/gamification';
import { Lock } from 'lucide-react';

interface AchievementGalleryProps {
    achievements: Achievement[];
    className?: string;
}

export const AchievementGallery: React.FC<AchievementGalleryProps> = ({ achievements, className = "" }) => {
    return (
        <div className={`grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4 ${className}`}>
            {achievements.map((achievement, index) => (
                <motion.div
                    key={achievement.achievement_id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.05 }}
                    className={`relative p-5 rounded-2xl border transition-all duration-300 ${achievement.unlocked
                            ? 'bg-white dark:bg-slate-800 border-indigo-100 dark:border-indigo-900/50 shadow-sm'
                            : 'bg-slate-50 dark:bg-slate-800/40 border-slate-100 dark:border-slate-800 grayscale opacity-60'
                        }`}
                >
                    {!achievement.unlocked && (
                        <div className="absolute top-3 right-3 text-slate-400">
                            <Lock className="w-4 h-4" />
                        </div>
                    )}

                    <div className="text-4xl mb-3">{achievement.icon}</div>
                    <h4 className="font-bold text-slate-900 dark:text-white mb-1 line-clamp-1">{achievement.name}</h4>
                    <p className="text-xs text-slate-500 dark:text-slate-400 line-clamp-2">{achievement.description}</p>

                    <div className="mt-4">
                        <div className="flex justify-between text-[10px] text-slate-400 font-bold uppercase tracking-wider mb-1">
                            <span>{achievement.category}</span>
                            <span>+{achievement.points_reward} XP</span>
                        </div>
                        {achievement.unlocked ? (
                            <div className="h-1.5 w-full bg-green-500 rounded-full" />
                        ) : (
                            <div className="h-1.5 w-full bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden">
                                <div
                                    className="h-full bg-slate-300 dark:bg-slate-600"
                                    style={{ width: `${achievement.progress}%` }}
                                />
                            </div>
                        )}
                    </div>
                </motion.div>
            ))}
        </div>
    );
};
