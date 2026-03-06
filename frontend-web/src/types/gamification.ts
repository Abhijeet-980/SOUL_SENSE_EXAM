export interface Achievement {
  achievement_id: string;
  name: string;
  description: string;
  icon?: string;
  category: 'consistency' | 'awareness' | 'intelligence';
  rarity: 'common' | 'rare' | 'epic' | 'legendary';
  points_reward: number;
  unlocked: boolean;
  progress: number;
  unlocked_at?: string;
}

export interface UserXP {
  total_xp: number;
  current_level: number;
  xp_to_next_level: number;
  level_progress: number; // 0.0 to 1.0
}

export interface UserStreak {
  activity_type: string;
  current_streak: number;
  longest_streak: number;
  last_activity_date?: string;
  is_active_today: boolean;
}

export interface LeaderboardEntry {
  rank: number;
  username: string;
  total_xp: number;
  current_level: number;
  avatar_path?: string;
}

export interface Challenge {
  id: number;
  title: string;
  description: string;
  challenge_type: 'weekly' | 'monthly' | 'special';
  start_date: string;
  end_date: string;
  reward_xp: number;
  status: 'available' | 'joined' | 'completed' | 'failed';
  progress?: Record<string, any>;
}

export interface GamificationSummary {
  xp: UserXP;
  streaks: UserStreak[];
  recent_achievements: Achievement[];
}
