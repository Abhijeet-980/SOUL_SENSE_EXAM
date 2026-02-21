'use client';

import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { BentoGridItem } from './bento-grid';
import { History, ArrowRight, CloudRain, Cloud, CloudSun, Sun, Sparkles } from 'lucide-react';
import { cn } from '@/lib/utils';
import Link from 'next/link';

const MOODS = [
  {
    icon: CloudRain,
    label: 'Very Low',
    rating: 1,
    color: 'text-secondary bg-secondary/10 border-secondary/20',
  },
  {
    icon: Cloud,
    label: 'Low',
    rating: 2,
    color: 'text-secondary/80 bg-secondary/5 border-secondary/10',
  },
  {
    icon: CloudSun,
    label: 'Neutral',
    rating: 3,
    color: 'text-muted-foreground bg-muted border-border',
  },
  { icon: Sun, label: 'Good', rating: 4, color: 'text-primary/80 bg-primary/5 border-primary/10' },
  {
    icon: Sparkles,
    label: 'Great',
    rating: 5,
    color: 'text-primary bg-primary/10 border-primary/20',
  },
];

export const MoodWidget = () => {
  const [loggedMood, setLoggedMood] = useState<number | null>(null);

  // Mock trend data for last 7 days (1-5 scale)
  const trendData = [3, 4, 3, 5, 4, 2, 4];

  const handleMoodSelect = (rating: number) => {
    // Simulate logging
    setLoggedMood(rating);
  };

  const selectedMood = MOODS.find((m) => m.rating === loggedMood);

  return (
    <BentoGridItem
      title="Daily Check-in"
      description={
        loggedMood ? "Good to know how you're feeling." : 'How are you feeling right now?'
      }
      className="md:col-span-1 border-none shadow-none p-0 group overflow-hidden"
    >
      <div className="flex flex-col h-full bg-white/60 dark:bg-black/40 backdrop-blur-xl rounded-3xl p-6 border border-white/20 transition-all group-hover:shadow-2xl">
        <div className="flex-1 flex flex-col justify-center">
          <AnimatePresence mode="wait">
            {!loggedMood ? (
              <motion.div
                key="selector"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className="flex justify-between items-center gap-2"
              >
                {MOODS.map((mood) => (
                  <motion.button
                    key={mood.rating}
                    whileHover={{ scale: 1.05, y: -2 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={() => handleMoodSelect(mood.rating)}
                    className={cn(
                      'flex-1 aspect-square rounded-2xl flex items-center justify-center transition-all border shadow-sm',
                      'bg-background/40 hover:bg-background border-border/40 hover:border-primary/30',
                      mood.color.split(' ')[0] // Extract just the text color for the icon idle state
                    )}
                    title={mood.label}
                  >
                    <mood.icon className="w-6 h-6 sm:w-8 sm:h-8" strokeWidth={1.5} />
                  </motion.button>
                ))}
              </motion.div>
            ) : (
              <motion.div
                key="logged"
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                className="flex flex-col items-center py-4"
              >
                <div className="mb-4">
                  {selectedMood && (
                    <selectedMood.icon className="w-16 h-16 drop-shadow-md" strokeWidth={1} />
                  )}
                </div>
                <div
                  className={cn(
                    'px-4 py-1 rounded-full text-xs font-semibold border mb-4 backdrop-blur-sm',
                    selectedMood?.color
                  )}
                >
                  {selectedMood?.label}
                </div>
                <Link
                  href="/journal"
                  className="group/link flex items-center gap-1 text-sm text-primary font-medium hover:underline"
                >
                  Complete Journal
                  <ArrowRight className="h-3.5 w-3.5 group-hover/link:translate-x-1 transition-transform" />
                </Link>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Mini Trend */}
        <div className="mt-6 pt-4 border-t border-neutral-200/50 dark:border-neutral-800/50">
          <div className="flex items-center justify-between mb-2">
            <span className="text-[10px] uppercase tracking-wider font-bold text-neutral-400">
              Past 7 Days
            </span>
            <History className="h-3 w-3 text-neutral-400" />
          </div>
          <div className="flex items-end justify-between h-8 gap-1 px-1">
            {trendData.map((val, i) => (
              <motion.div
                key={i}
                initial={{ height: 0 }}
                animate={{ height: `${(val / 5) * 100}%` }}
                className={cn(
                  'flex-1 rounded-full min-h-[4px]',
                  val >= 4 ? 'bg-green-500/40' : val <= 2 ? 'bg-red-500/40' : 'bg-yellow-500/40'
                )}
              />
            ))}
          </div>
        </div>
      </div>
    </BentoGridItem>
  );
};
