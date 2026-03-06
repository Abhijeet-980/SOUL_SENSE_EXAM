"use client";

import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Achievement } from '@/types/gamification';
import { Trophy, Star, X } from 'lucide-react';

interface AchievementToastProps {
    achievement: Achievement;
    onClose: () => void;
    isVisible: boolean;
}

export const AchievementToast: React.FC<AchievementToastProps> = ({ achievement, onClose, isVisible }) => {
    return (
        <AnimatePresence>
            {isVisible && (
                <motion.div
                    initial={{ opacity: 0, scale: 0.9, y: 100 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.9, y: 100 }}
                    className="fixed bottom-8 left-1/2 -translate-x-1/2 z-[100] w-full max-w-sm px-4"
                >
                    <div className="bg-slate-900 text-white rounded-2xl p-4 shadow-2xl border border-white/10 flex gap-4 relative overflow-hidden">
                        {/* Background Sparkles Effect */}
                        <div className="absolute top-0 right-0 p-4 opacity-10 pointer-events-none">
                            <Star className="w-16 h-16 text-yellow-500 fill-yellow-500" />
                        </div>

                        <div className="bg-yellow-500/20 rounded-xl p-3 flex items-center justify-center shrink-0">
                            <Trophy className="w-8 h-8 text-yellow-500" />
                        </div>

                        <div className="flex-1 pr-6">
                            <div className="text-xs font-bold text-yellow-500 uppercase tracking-widest mb-1">New Achievement Unlocked!</div>
                            <h3 className="text-base font-bold text-white leading-tight">{achievement.name}</h3>
                            <p className="text-xs text-slate-400 mt-1">{achievement.description}</p>
                            <div className="mt-2 text-xs font-semibold bg-white/10 inline-block px-2 py-0.5 rounded-full">
                                +{achievement.points_reward} XP
                            </div>
                        </div>

                        <button
                            onClick={onClose}
                            className="absolute top-3 right-3 text-slate-500 hover:text-white transition-colors"
                        >
                            <X className="w-4 h-4" />
                        </button>
                    </div>
                </motion.div>
            )}
        </AnimatePresence>
    );
};
