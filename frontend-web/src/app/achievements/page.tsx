"use client";

import React, { useState } from 'react';
import { useApi } from '@/hooks/useApi';
import { gamificationApi } from '@/lib/api/gamification';
import { XPProgress, StreakDisplay, AchievementGallery, AchievementToast } from '@/components/gamification';
import { Trophy, Users, Zap, Calendar } from 'lucide-react';
import { motion } from 'framer-motion';

export default function AchievementsPage() {
    const { data: summary, loading, error, refetch } = useApi({
        apiFn: () => gamificationApi.getSummary()
    });

    const { data: leaderboard, loading: lbLoading } = useApi({
        apiFn: () => gamificationApi.getLeaderboard(5)
    });

    const [testAchievement, setTestAchievement] = useState(false);

    if (loading) {
        return (
            <div className="flex items-center justify-center min-h-screen">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-600"></div>
            </div>
        );
    }

    if (error || !summary) {
        return (
            <div className="max-w-7xl mx-auto px-4 py-12 text-center">
                <h1 className="text-2xl font-bold text-slate-900 dark:text-white mb-4">Oops! Something went wrong</h1>
                <p className="text-slate-500 mb-8">{error || "Could not load achievements"}</p>
                <button
                    onClick={() => refetch()}
                    className="bg-indigo-600 text-white px-6 py-2 rounded-xl font-medium"
                >
                    Try Again
                </button>
            </div>
        );
    }

    return (
        <div className="max-w-7xl mx-auto px-4 py-12 bg-slate-50/50 dark:bg-transparent min-h-screen">
            <header className="mb-12">
                <div className="flex items-center gap-3 mb-2">
                    <Trophy className="w-8 h-8 text-yellow-500" />
                    <h1 className="text-4xl font-black text-slate-900 dark:text-white tracking-tight">Your Progress</h1>
                </div>
                <p className="text-slate-500 dark:text-slate-400 text-lg">Tracks your emotional health journey and consistency.</p>
            </header>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 mb-12">
                <div className="lg:col-span-2 space-y-8">
                    {/* XP and Level card */}
                    <XPProgress xp={summary.xp} />

                    {/* Streak Card */}
                    <StreakDisplay streaks={summary.streaks} />

                    {/* Challenges Section (Placeholder) */}
                    <div className="bg-gradient-to-br from-indigo-600 to-purple-700 rounded-3xl p-8 text-white relative overflow-hidden shadow-xl shadow-indigo-200 dark:shadow-none">
                        <div className="absolute -top-10 -right-10 w-40 h-40 bg-white/10 rounded-full blur-3xl" />
                        <div className="relative z-10">
                            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-white/20 text-xs font-bold uppercase tracking-wider mb-4 border border-white/20">
                                <Zap className="w-3 h-3 fill-white" />
                                Featured Challenge
                            </div>
                            <h2 className="text-3xl font-bold mb-2">Mindfulness Week</h2>
                            <p className="text-indigo-100 max-w-md mb-6">Complete 7 journal entries this week to earn a Streak Freeze and +500 XP bonus!</p>
                            <button className="bg-white text-indigo-600 px-8 py-3 rounded-2xl font-bold hover:bg-slate-50 transition-colors">
                                View All Challenges
                            </button>
                        </div>
                        <Calendar className="absolute bottom-[-20px] right-[-20px] w-48 h-48 opacity-10 pointer-events-none rotate-12" />
                    </div>
                </div>

                <div className="space-y-8">
                    {/* Mini Leaderboard */}
                    <div className="bg-white dark:bg-slate-800 rounded-3xl p-6 shadow-sm border border-slate-100 dark:border-slate-700">
                        <div className="flex items-center justify-between mb-6">
                            <div className="flex items-center gap-2">
                                <Users className="w-5 h-5 text-indigo-500" />
                                <h3 className="font-bold text-slate-900 dark:text-white">Leaderboard</h3>
                            </div>
                            <span className="text-xs font-bold text-indigo-600 dark:text-indigo-400 uppercase">Top 5</span>
                        </div>

                        <div className="space-y-4">
                            {leaderboard?.map((entry) => (
                                <div key={entry.rank} className="flex items-center justify-between">
                                    <div className="flex items-center gap-3">
                                        <span className={`w-6 h-6 flex items-center justify-center rounded-full text-xs font-bold ${entry.rank === 1 ? 'bg-yellow-100 text-yellow-700' :
                                                entry.rank === 2 ? 'bg-slate-100 text-slate-700' :
                                                    'text-slate-400'
                                            }`}>
                                            {entry.rank}
                                        </span>
                                        <span className="font-medium text-slate-700 dark:text-slate-300">{entry.username}</span>
                                    </div>
                                    <div className="text-right">
                                        <div className="text-sm font-bold text-slate-900 dark:text-slate-100">{entry.total_xp} XP</div>
                                        <div className="text-[10px] text-slate-400">Level {entry.current_level}</div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Quick Actions */}
                    <div className="bg-white dark:bg-slate-800 rounded-3xl p-6 shadow-sm border border-slate-100 dark:border-slate-700">
                        <h3 className="font-bold text-slate-900 dark:text-white mb-4">Rewards Shop</h3>
                        <p className="text-sm text-slate-500 mb-6">Use your XP to unlock new themes, avatars, and features.</p>
                        <button disabled className="w-full bg-slate-100 dark:bg-slate-700 text-slate-400 py-3 rounded-2xl font-bold cursor-not-allowed">
                            Coming Level 5
                        </button>
                    </div>
                </div>
            </div>

            <section>
                <div className="flex items-center justify-between mb-8">
                    <h2 className="text-2xl font-bold text-slate-900 dark:text-white">Achievements</h2>
                    <button
                        onClick={() => setTestAchievement(true)}
                        className="text-xs text-indigo-600 font-bold uppercase"
                    >
                        Show Unlocks
                    </button>
                </div>

                <AchievementGallery achievements={summary.recent_achievements} />
            </section>

            {/* Test Toast */}
            {summary.recent_achievements.length > 0 && (
                <AchievementToast
                    achievement={summary.recent_achievements[0]}
                    isVisible={testAchievement}
                    onClose={() => setTestAchievement(false)}
                />
            )}
        </div>
    );
}
