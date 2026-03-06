import { apiClient } from './client';
import { GamificationSummary, Achievement, LeaderboardEntry, UserXP, UserStreak } from '@/types/gamification';

/**
 * Gamification API Service
 */
export const gamificationApi = {
    /**
     * Get complete gamification summary
     */
    getSummary: async (): Promise<GamificationSummary> => {
        return apiClient('/gamification/summary');
    },

    /**
     * Get all achievements
     */
    getAchievements: async (): Promise<Achievement[]> => {
        return apiClient('/gamification/achievements');
    },

    /**
     * Get current streaks
     */
    getStreaks: async (): Promise<UserStreak[]> => {
        return apiClient('/gamification/streak');
    },

    /**
     * Get XP and level info
     */
    getXP: async (): Promise<UserXP> => {
        return apiClient('/gamification/xp');
    },

    /**
     * Get global leaderboard
     */
    getLeaderboard: async (limit: number = 10): Promise<LeaderboardEntry[]> => {
        return apiClient(`/gamification/leaderboard?limit=${limit}`);
    },

    /**
     * Seed initial achievements (Development only)
     */
    seedAchievements: async (): Promise<void> => {
        return apiClient('/gamification/seed', {
            method: 'POST'
        });
    }
};
