'use client';

import React, { useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';

interface MoodSliderProps {
    value: number;
    onChange: (value: number) => void;
    label?: string;
    showEmoji?: boolean;
}

const moodConfig = [
    { range: [1, 2], emoji: '😢', label: 'Very Sad', color: 'rgb(239, 68, 68)' },    // red-500
    { range: [3, 4], emoji: '😕', label: 'Sad', color: 'rgb(249, 115, 22)' },      // orange-500
    { range: [5, 6], emoji: '😐', label: 'Neutral', color: 'rgb(234, 179, 8)' },   // yellow-500
    { range: [7, 8], emoji: '🙂', label: 'Happy', color: 'rgb(132, 204, 22)' },    // lime-500
    { range: [9, 10], emoji: '😄', label: 'Very Happy', color: 'rgb(34, 197, 94)' }, // green-500
];

export function MoodSlider({
    value,
    onChange,
    label,
    showEmoji = true,
}: MoodSliderProps) {
    const currentMood = useMemo(() => {
        return moodConfig.find(
            (m) => value >= m.range[0] && value <= m.range[1]
        ) || moodConfig[2];
    }, [value]);

    // Calculate track background with dynamic color transition
    const getTrackBackground = () => {
        const percentage = ((value - 1) / 9) * 100;
        return `linear-gradient(to right, ${currentMood.color} ${percentage}%, rgb(var(--secondary) / 0.5) ${percentage}%)`;
    };

    return (
        <div className="w-full space-y-6 py-4">
            <div className="flex items-center justify-between px-1">
                {label && (
                    <label className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">
                        {label}
                    </label>
                )}
                <div className="flex items-center gap-2">
                    <span className="text-2xl font-black tabular-nums text-foreground">
                        {value}
                    </span>
                    <span className="text-xs font-medium text-muted-foreground">/ 10</span>
                </div>
            </div>

            <div className="relative group px-1">
                {showEmoji && (
                    <div className="flex justify-center mb-8">
                        <AnimatePresence mode="wait">
                            <motion.div
                                key={currentMood.emoji}
                                initial={{ scale: 0.5, opacity: 0, y: 10 }}
                                animate={{ scale: 1, opacity: 1, y: 0 }}
                                exit={{ scale: 0.5, opacity: 0, y: -10 }}
                                transition={{ type: 'spring', stiffness: 300, damping: 20 }}
                                className="flex flex-col items-center"
                            >
                                <span className="text-6xl drop-shadow-xl select-none" role="img" aria-label={currentMood.label}>
                                    {currentMood.emoji}
                                </span>
                                <motion.span
                                    initial={{ opacity: 0 }}
                                    animate={{ opacity: 1 }}
                                    className="text-sm font-bold mt-2"
                                    style={{ color: currentMood.color }}
                                >
                                    {currentMood.label}
                                </motion.span>
                            </motion.div>
                        </AnimatePresence>
                    </div>
                )}

                <div className="relative h-6 flex items-center">
                    <input
                        type="range"
                        min="1"
                        max="10"
                        step="1"
                        value={value}
                        onChange={(e) => onChange(parseInt(e.target.value))}
                        className="absolute w-full h-3 bg-secondary rounded-full appearance-none cursor-pointer z-10 opacity-0"
                        style={{ WebkitAppearance: 'none' }}
                    />

                    {/* Custom Styled Track */}
                    <div
                        className="absolute w-full h-3 rounded-full transition-all duration-300 shadow-inner overflow-hidden"
                        style={{ background: getTrackBackground() }}
                    />

                    {/* Animated Thumb Proxy */}
                    <motion.div
                        className="absolute h-7 w-7 bg-white rounded-full shadow-2xl border-2 z-20 pointer-events-none"
                        style={{
                            left: `calc(${((value - 1) / 9) * 100}% - 14px)`,
                            borderColor: currentMood.color,
                            boxShadow: `0 0 20px ${currentMood.color}40`
                        }}
                        transition={{ type: 'spring', stiffness: 500, damping: 30 }}
                    />
                </div>

                <div className="flex justify-between mt-4 px-1">
                    {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map((num) => (
                        <button
                            key={num}
                            onClick={() => onChange(num)}
                            className={cn(
                                "w-1 h-1 rounded-full transition-all duration-300",
                                value >= num ? "bg-foreground/20 scale-125" : "bg-muted"
                            )}
                        />
                    ))}
                </div>
            </div>
        </div>
    );
}
