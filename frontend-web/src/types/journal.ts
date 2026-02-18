/**
 * Journal Entry Types
 * ====================
 * Type definitions for journal entries, filters and payloads.
 */

export type MoodLevel = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10;

export const PRESET_TAGS = [
    "work",
    "family",
    "health",
    "relationships",
    "personal",
    "gratitude",
    "goals",
    "stress",
    "achievement",
    "learning",
] as const;

export type PresetTag = (typeof PRESET_TAGS)[number];

export interface JournalEntry {
    id: number;
    content: string;
    mood_rating: number; // 1-10
    energy_level?: number; // 1-10
    stress_level?: number; // 1-10
    tags: string[];
    sentiment_score?: number; // -1 to 1
    patterns?: string[]; // detected emotional patterns
    created_at: string;
    updated_at: string;
}

export interface JournalEntryCreate {
    content: string;
    mood_rating: number;
    energy_level?: number;
    stress_level?: number;
    tags?: string[];
}

export interface JournalEntryUpdate extends Partial<JournalEntryCreate> { }

export interface JournalFilters {
    startDate?: string;
    endDate?: string;
    moodMin?: number;
    moodMax?: number;
    tags?: string[];
    search?: string;
}

export type TimeRange = '7d' | '14d' | '30d';
