'use client';

import React from 'react';
import { format, parseISO } from 'date-fns';
import { motion } from 'framer-motion';
import { ChevronRight } from 'lucide-react';
import { JournalEntry } from '@/types/journal';
import { cn } from '@/lib/utils';
import styles from './entry-card.module.css';

interface EntryCardProps {
    entry: JournalEntry;
    onClick?: (entry: JournalEntry) => void;
    variant?: 'compact' | 'expanded';
    className?: string;
}

const getMoodEmoji = (score: number | undefined): string => {
    if (score === undefined) return '😐';
    if (score <= 2) return '😢';
    if (score <= 4) return '😕';
    if (score <= 6) return '😐';
    if (score <= 8) return '🙂';
    return '😄';
};

const getSentimentColorClass = (score: number | undefined): string => {
    if (score === undefined) return styles['sentiment-neutral'];
    if (score > 0.3) return styles['sentiment-positive'];
    if (score < -0.3) return styles['sentiment-negative'];
    return styles['sentiment-neutral'];
};

export const JournalEntryCard: React.FC<EntryCardProps> = ({
    entry,
    onClick,
    variant = 'compact',
    className,
}) => {
    const {
        created_at,
        content,
        mood_rating,
        tags = [],
        sentiment_score,
    } = entry;

    const formattedDate = React.useMemo(() => {
        try {
            return format(parseISO(created_at), 'MMMM d, yyyy');
        } catch (e) {
            return created_at;
        }
    }, [created_at]);

    const moodEmoji = getMoodEmoji(mood_rating);
    const sentimentClass = getSentimentColorClass(sentiment_score);

    return (
        <motion.div
            layout
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            whileHover={{ scale: 1.01 }}
            whileTap={{ scale: 0.99 }}
            onClick={() => onClick?.(entry)}
            className={cn(
                styles.card,
                variant === 'compact' ? styles.compact : styles.expanded,
                className
            )}
        >
            <div className={cn(styles.sentimentIndicator, sentimentClass)} />

            <div className={styles.header}>
                <span className={styles.date}>{formattedDate}</span>
                <div className={styles.moodContainer}>
                    <span className={styles.moodEmoji}>{moodEmoji}</span>
                    {mood_rating !== undefined && (
                        <span className={styles.moodRating}>{mood_rating}/10</span>
                    )}
                </div>
            </div>

            <p className={styles.content}>
                {content}
            </p>

            {tags.length > 0 && (
                <div className={styles.tags}>
                    {tags.slice(0, variant === 'compact' ? 3 : 6).map((tag, idx) => (
                        <span key={idx} className={styles.tag}>
                            {tag}
                        </span>
                    ))}
                    {variant === 'compact' && tags.length > 3 && (
                        <span className={styles.tag}>+{tags.length - 3}</span>
                    )}
                </div>
            )}

            <div className={styles.footer}>
                <ChevronRight className={styles.arrow} size={18} />
            </div>
        </motion.div>
    );
};
