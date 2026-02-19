'use client';

import React, { useState, KeyboardEvent } from 'react';
import { X, Plus, Tag as TagIcon } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';
import { PRESET_TAGS } from '@/lib/constants/journal';

interface TagSelectorProps {
    selected: string[];
    onChange: (tags: string[]) => void;
    presets?: string[] | readonly string[];
    allowCustom?: boolean;
    maxTags?: number;
}

export function TagSelector({
    selected,
    onChange,
    presets = PRESET_TAGS,
    allowCustom = true,
    maxTags = 10,
}: TagSelectorProps) {
    const [inputValue, setInputValue] = useState('');
    const [isFocused, setIsFocused] = useState(false);

    const addTag = (tag: string) => {
        const trimmedTag = tag.trim();
        if (
            trimmedTag &&
            !selected.includes(trimmedTag) &&
            (maxTags === undefined || selected.length < maxTags)
        ) {
            onChange([...selected, trimmedTag]);
        }
        setInputValue('');
    };

    const removeTag = (tagToRemove: string) => {
        onChange(selected.filter((tag) => tag !== tagToRemove));
    };

    const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter' && allowCustom) {
            e.preventDefault();
            addTag(inputValue);
        } else if (e.key === 'Backspace' && !inputValue && selected.length > 0) {
            removeTag(selected[selected.length - 1]);
        }
    };

    const availablePresets = presets.filter((tag) => !selected.includes(tag));

    return (
        <div className="space-y-4 w-full">
            <div
                className={cn(
                    "min-h-[50px] p-2 rounded-xl border transition-all duration-200 bg-background/50 backdrop-blur-sm",
                    isFocused ? "border-primary ring-2 ring-primary/20 shadow-lg" : "border-border shadow-sm"
                )}
            >
                <div className="flex flex-wrap gap-2 items-center">
                    <AnimatePresence>
                        {selected.map((tag) => (
                            <motion.span
                                key={tag}
                                initial={{ scale: 0.8, opacity: 0 }}
                                animate={{ scale: 1, opacity: 1 }}
                                exit={{ scale: 0.8, opacity: 0 }}
                                className="inline-flex items-center gap-1 px-3 py-1 bg-primary/10 text-primary rounded-full text-sm font-medium border border-primary/20 hover:bg-primary/20 transition-colors"
                                layout
                            >
                                {tag}
                                <button
                                    onClick={() => removeTag(tag)}
                                    className="hover:text-destructive transition-colors rounded-full hover:bg-destructive/10 p-0.5"
                                    aria-label={`Remove ${tag}`}
                                >
                                    <X className="w-3.5 h-3.5" />
                                </button>
                            </motion.span>
                        ))}
                    </AnimatePresence>

                    {allowCustom && (selected.length < (maxTags || Infinity)) && (
                        <div className="flex-1 min-w-[120px]">
                            <input
                                type="text"
                                value={inputValue}
                                onChange={(e) => setInputValue(e.target.value)}
                                onKeyDown={handleKeyDown}
                                onFocus={() => setIsFocused(true)}
                                onBlur={() => setIsFocused(false)}
                                placeholder={selected.length === 0 ? "Add tags..." : ""}
                                className="w-full bg-transparent border-none outline-none text-sm placeholder:text-muted-foreground focus:ring-0 p-1"
                            />
                        </div>
                    )}
                </div>
            </div>

            <div className="space-y-2">
                <label className="text-xs font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1.5 px-1">
                    <TagIcon className="w-3 h-3" />
                    Suggested Tags
                </label>
                <div className="flex flex-wrap gap-2">
                    {availablePresets.map((tag) => (
                        <button
                            key={tag}
                            onClick={() => addTag(tag)}
                            disabled={selected.length >= (maxTags || Infinity)}
                            className={cn(
                                "px-3 py-1 rounded-lg text-xs font-medium border transition-all duration-200 flex items-center gap-1",
                                "bg-secondary/50 border-border hover:border-primary/50 hover:bg-primary/5 hover:text-primary",
                                selected.length >= (maxTags || Infinity) && "opacity-50 cursor-not-allowed grayscale"
                            )}
                        >
                            <Plus className="w-3 h-3" />
                            {tag}
                        </button>
                    ))}
                    {availablePresets.length === 0 && (
                        <p className="text-xs text-muted-foreground italic px-1">No more suggestions</p>
                    )}
                </div>
            </div>

            {maxTags && (
                <p className="text-[10px] text-muted-foreground text-right px-1">
                    {selected.length} / {maxTags} tags used
                </p>
            )}
        </div>
    );
}
