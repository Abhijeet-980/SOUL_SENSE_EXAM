'use client';

import { useState, KeyboardEvent } from 'react';
import { UserSettings } from '../../lib/api/settings';
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
    Input,
    Button
} from '@/components/ui';
import { useDebounce } from '@/hooks/useDebounce';
import {
    Brain,
    ShieldAlert,
    MessageSquare,
    History,
    X,
    Plus,
    Info,
    ShieldCheck,
    Flame,
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

interface AIBoundarySettingsProps {
    settings: UserSettings;
    onChange: (updates: Partial<UserSettings>) => void;
}

export function AIBoundarySettings({ settings, onChange }: AIBoundarySettingsProps) {
    const [newTopic, setNewTopic] = useState('');
    const debouncedOnChange = useDebounce(onChange, 500);

    const handleBoundaryChange = (updates: Partial<UserSettings['ai_boundaries']>) => {
        debouncedOnChange({
            ai_boundaries: {
                ...settings.ai_boundaries,
                ...updates,
            },
        });
    };

    const addTopic = () => {
        const trimmed = newTopic.trim();
        // Regex constraint: Prevent systemic symbols (<, >, /) as requested to prevent prompt injection
        if (trimmed && !/[<>/]/.test(trimmed) && !settings.ai_boundaries.off_limit_topics.includes(trimmed)) {
            handleBoundaryChange({
                off_limit_topics: [...settings.ai_boundaries.off_limit_topics, trimmed],
            });
            setNewTopic('');
        }
    };

    const removeTopic = (topicToRemove: string) => {
        handleBoundaryChange({
            off_limit_topics: settings.ai_boundaries.off_limit_topics.filter(t => t !== topicToRemove),
        });
    };

    const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            addTopic();
        }
    };

    return (
        <div className="space-y-12">
            {/* Off-Limit Topics */}
            <div className="space-y-6">
                <div className="flex items-center gap-2 text-primary/60">
                    <ShieldAlert className="h-4 w-4" />
                    <h3 className="text-[10px] uppercase tracking-widest font-black italic">Thematic Firewalls</h3>
                </div>

                <div className="space-y-4">
                    <div className="bg-primary/5 border border-primary/10 rounded-3xl p-8 relative overflow-hidden group shadow-inner">
                        <div className="absolute top-0 right-0 p-8 opacity-[0.03] group-hover:opacity-[0.05] transition-opacity duration-700 pointer-events-none">
                            <Brain className="h-48 w-48" />
                        </div>

                        <div className="space-y-6 relative z-10">
                            <div className="space-y-1">
                                <label className="text-base font-black tracking-tight">Boundary Definition (Off-Limit Topics)</label>
                                <p className="text-xs text-muted-foreground max-w-lg leading-relaxed font-medium">
                                    Explicitly mandate thematic exclusion zones. These strings are structurally bound as rigid negative constraints, preventing backend LLM agents from parsing specific emotional schemas.
                                </p>
                            </div>

                            <div className="flex flex-wrap gap-2 min-h-[44px]">
                                <AnimatePresence mode="popLayout">
                                    {settings.ai_boundaries.off_limit_topics.map((topic) => (
                                        <motion.div
                                            key={topic}
                                            initial={{ scale: 0.8, opacity: 0, x: -10 }}
                                            animate={{ scale: 1, opacity: 1, x: 0 }}
                                            exit={{ scale: 0.8, opacity: 0, x: 10 }}
                                            className="flex items-center gap-2 px-4 py-2 bg-background/80 backdrop-blur-sm border border-primary/20 rounded-xl text-[11px] font-black shadow-sm group/tag hover:border-primary/40 transition-colors"
                                        >
                                            {topic}
                                            <button
                                                onClick={() => removeTopic(topic)}
                                                className="p-0.5 hover:bg-destructive/10 hover:text-destructive rounded-full transition-colors"
                                            >
                                                <X className="h-3 w-3" />
                                            </button>
                                        </motion.div>
                                    ))}
                                    {settings.ai_boundaries.off_limit_topics.length === 0 && (
                                        <motion.p
                                            initial={{ opacity: 0 }}
                                            animate={{ opacity: 1 }}
                                            className="text-[10px] text-muted-foreground/60 italic font-medium py-2"
                                        >
                                            No active firewalls defined. Add a topic to restrict AI context.
                                        </motion.p>
                                    )}
                                </AnimatePresence>
                            </div>

                            <div className="flex gap-3">
                                <div className="relative flex-1 group">
                                    <Input
                                        value={newTopic}
                                        onChange={(e) => setNewTopic(e.target.value)}
                                        onKeyDown={handleKeyDown}
                                        placeholder="Enter forbidden dataset (e.g. Severe Trauma, Finances)..."
                                        className="rounded-2xl bg-background/50 border-border/40 focus:border-primary/40 h-12 text-xs font-bold pl-5 transition-all shadow-sm group-hover:shadow-md"
                                    />
                                    <div className="absolute inset-y-0 right-4 flex items-center pointer-events-none opacity-20">
                                        <ShieldAlert className="h-4 w-4" />
                                    </div>
                                </div>
                                <Button
                                    onClick={addTopic}
                                    variant="outline"
                                    size="icon"
                                    className="rounded-2xl border-border/40 shrink-0 h-12 w-12 hover:bg-primary/10 hover:text-primary hover:border-primary/20 transition-all shadow-sm"
                                >
                                    <Plus className="h-5 w-5" />
                                </Button>
                            </div>

                            {/[<>/]/.test(newTopic) && (
                                <motion.div
                                    initial={{ opacity: 0, y: -5 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    className="flex items-center gap-2 p-3 bg-destructive/5 border border-destructive/10 rounded-xl"
                                >
                                    <Flame className="h-3.5 w-3.5 text-destructive animate-pulse" />
                                    <p className="text-[10px] text-destructive font-black uppercase tracking-wider">
                                        Security Breach Prevented: Systemic symbols (&lt;, &gt;, /) are forbidden in boundary tags.
                                    </p>
                                </motion.div>
                            )}
                        </div>
                    </div>

                    <div className="flex items-start gap-4 p-5 bg-muted/30 border border-border/20 rounded-2xl">
                        <div className="p-2 rounded-lg bg-background border border-border/40 text-muted-foreground">
                            <Info className="h-4 w-4" />
                        </div>
                        <div className="space-y-1">
                            <p className="text-[11px] font-black uppercase tracking-widest text-muted-foreground">UX System Dynamics</p>
                            <p className="text-[10px] text-muted-foreground italic leading-relaxed font-medium">
                                Defining a topic here structurally modifies the backend prompt. For example, adding "Medical" causes the system to append: <span className="text-primary/60 font-bold not-italic">"SYSTEM_CONSTRAINT: Strictly ignore any linguistic patterns relating to [Medical]..."</span> ensuring complete ML avoidance.
                            </p>
                        </div>
                    </div>
                </div>
            </div>

            {/* AI Tone Preference */}
            <div className="space-y-6 pt-6 border-t border-border/40">
                <div className="flex items-center gap-2 text-emerald-500/60">
                    <MessageSquare className="h-4 w-4" />
                    <h3 className="text-[10px] uppercase tracking-widest font-black italic">Linguistic Architecture</h3>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-10 items-center p-6 bg-emerald-500/[0.02] border border-emerald-500/10 rounded-3xl">
                    <div className="space-y-2">
                        <h4 className="text-sm font-black tracking-tight flex items-center gap-2">
                            Persona Resonance Tuning
                            <span className="px-2 py-0.5 rounded-full bg-emerald-500/10 text-[9px] text-emerald-600 uppercase tracking-widest">Active</span>
                        </h4>
                        <p className="text-[10px] text-muted-foreground leading-relaxed font-medium">
                            Calibrate the LLM's vocal register. This directly modifies the temperature and semantic framing parameters within the prompt injection pipeline.
                        </p>
                    </div>
                    <Select
                        value={settings.ai_boundaries.ai_tone_preference}
                        onValueChange={(val: any) => handleBoundaryChange({ ai_tone_preference: val })}
                    >
                        <SelectTrigger className="rounded-2xl bg-background/60 backdrop-blur-sm border-border/40 h-12 font-black text-[11px] px-6 shadow-sm hover:border-emerald-500/20 transition-all">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="rounded-2xl border-border/40 shadow-xl">
                            <SelectItem value="Clinical" className="rounded-lg font-bold text-xs">Clinical (Analytical & Objective)</SelectItem>
                            <SelectItem value="Warm" className="rounded-lg font-bold text-xs">Warm (Empathetic & Supportive)</SelectItem>
                            <SelectItem value="Direct" className="rounded-lg font-bold text-xs">Direct (Concise & Pragmatic)</SelectItem>
                            <SelectItem value="Philosophical" className="rounded-lg font-bold text-xs">Philosophical (Metaphorical & Deep)</SelectItem>
                        </SelectContent>
                    </Select>
                </div>
            </div>

            {/* Storage Retention */}
            <div className="space-y-6 pt-6 border-t border-border/40">
                <div className="flex items-center gap-2 text-orange-500/60">
                    <History className="h-4 w-4" />
                    <h3 className="text-[10px] uppercase tracking-widest font-black italic">Persistence Decay Protocols</h3>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-10 items-center p-6 bg-orange-500/[0.02] border border-orange-500/10 rounded-3xl">
                    <div className="space-y-2">
                        <h4 className="text-sm font-black tracking-tight">AI Memory Hard-Purge</h4>
                        <p className="text-[10px] text-muted-foreground leading-relaxed font-medium">
                            Control the temporal boundary for vector memory persistence. Data exceeding this age is physically deleted from the active RAG processing stream.
                        </p>
                    </div>
                    <Select
                        value={settings.ai_boundaries.storage_retention_days.toString()}
                        onValueChange={(val: string) => handleBoundaryChange({ storage_retention_days: parseInt(val) })}
                    >
                        <SelectTrigger className="rounded-2xl bg-background/60 backdrop-blur-sm border-border/40 h-12 font-black text-[11px] px-6 shadow-sm hover:border-orange-500/20 transition-all">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="rounded-2xl border-border/40 shadow-xl">
                            <SelectItem value="30" className="rounded-lg font-bold text-xs">30 Days (Ephemeral Burst)</SelectItem>
                            <SelectItem value="90" className="rounded-lg font-bold text-xs">90 Days (Quarterly Decay)</SelectItem>
                            <SelectItem value="180" className="rounded-lg font-bold text-xs">180 Days (Semi-Annual Purge)</SelectItem>
                            <SelectItem value="365" className="rounded-lg font-bold text-xs">365 Days (Archival Cycle)</SelectItem>
                        </SelectContent>
                    </Select>
                </div>
            </div>

            {/* Compliance Badge */}
            <div className="flex items-center justify-center pt-8">
                <motion.div
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    transition={{ delay: 0.5 }}
                    className="flex items-center gap-3 px-6 py-3 bg-emerald-500/5 text-emerald-600 rounded-2xl border border-emerald-500/20 shadow-[0_0_20px_rgba(16,185,129,0.05)]"
                >
                    <div className="relative">
                        <ShieldCheck className="h-4 w-4" />
                        <motion.div
                            animate={{ opacity: [0, 1, 0], scale: [1, 1.5, 1] }}
                            transition={{ duration: 2, repeat: Infinity }}
                            className="absolute inset-0 bg-emerald-500/20 rounded-full"
                        />
                    </div>
                    <span className="text-[9px] font-black uppercase tracking-[0.25em]">Boundary Logic Native Enforcement Active</span>
                </motion.div>
            </div>
        </div>
    );
}
