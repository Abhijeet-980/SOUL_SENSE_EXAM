'use client';

import React, { useMemo } from 'react';
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
    ReferenceLine,
    Area,
    AreaChart,
    ReferenceArea,
    Dot,
} from 'recharts';
import { format, parseISO, subDays, isWithinInterval, startOfDay } from 'date-fns';
import { JournalEntry, TimeRange } from '@/types/journal';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui';

interface MoodTrendProps {
    entries: JournalEntry[];
    timeRange: TimeRange;
    showAverage: boolean;
}

export function MoodTrend({ entries, timeRange, showAverage }: MoodTrendProps) {
    const chartData = useMemo(() => {
        const now = new Date();
        const daysToSubtract = timeRange === '7d' ? 7 : timeRange === '14d' ? 14 : 30;
        const startDate = startOfDay(subDays(now, daysToSubtract - 1));

        // Filter and sort entries
        const filteredEntries = entries
            .filter((entry) => {
                const entryDate = parseISO(entry.created_at);
                return isWithinInterval(entryDate, { start: startDate, end: now });
            })
            .sort((a, b) => parseISO(a.created_at).getTime() - parseISO(b.created_at).getTime());

        // Create a map of date string to entry for easy lookup
        const entryMap = new Map<string, JournalEntry>();
        filteredEntries.forEach((entry) => {
            const dateStr = format(parseISO(entry.created_at), 'yyyy-MM-dd');
            entryMap.set(dateStr, entry);
        });

        // Generate data for each day in the range
        const data = [];
        for (let i = 0; i < daysToSubtract; i++) {
            const date = subDays(now, daysToSubtract - 1 - i);
            const dateStr = format(date, 'yyyy-MM-dd');
            const entry = entryMap.get(dateStr);

            data.push({
                date: dateStr,
                displayDate: format(date, 'MMM d'),
                // Use mood_rating if available, otherwise fallback to sentiment_score/10 or null
                mood: entry ? entry.mood_rating ?? (entry.sentiment_score ? entry.sentiment_score / 10 : null) : null,
                fullDate: format(date, 'PPPP'),
                hasEntry: !!entry,
                content: entry?.content?.substring(0, 100) + (entry?.content && entry.content.length > 100 ? '...' : ''),
                id: entry?.id
            });
        }

        return data;
    }, [entries, timeRange]);

    const averageMood = useMemo(() => {
        const moods = chartData.filter((d) => d.mood !== null).map((d) => d.mood as number);
        if (moods.length === 0) return 0;
        return moods.reduce((a, b) => a + b, 0) / moods.length;
    }, [chartData]);

    if (entries.length === 0) {
        return (
            <Card className="w-full h-[350px] flex items-center justify-center border-dashed">
                <p className="text-muted-foreground">No data available for the selected range</p>
            </Card>
        );
    }

    return (
        <Card className="w-full backdrop-blur-md bg-white/40 dark:bg-slate-900/40 border-slate-200 dark:border-slate-800 shadow-xl overflow-hidden group">
            <CardHeader className="pb-2">
                <CardTitle className="text-xl font-bold flex items-center gap-2">
                    Mood Trends
                    <div className="h-2 w-2 rounded-full bg-primary animate-pulse" />
                </CardTitle>
                <CardDescription>
                    Visualize your emotional journey over the last {timeRange}
                </CardDescription>
            </CardHeader>
            <CardContent>
                <div className="h-[300px] w-full mt-4">
                    <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                            <defs>
                                <linearGradient id="moodGradient" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.3} />
                                    <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0} />
                                </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" vertical={false} strokeOpacity={0.1} />
                            <XAxis
                                dataKey="displayDate"
                                axisLine={false}
                                tickLine={false}
                                fontSize={12}
                                tick={{ fill: 'currentColor', opacity: 0.5 }}
                                minTickGap={20}
                            />
                            <YAxis
                                domain={[0, 10]}
                                ticks={[2, 4, 6, 8, 10]}
                                axisLine={false}
                                tickLine={false}
                                fontSize={12}
                                tick={{ fill: 'currentColor', opacity: 0.5 }}
                            />
                            <Tooltip
                                content={({ active, payload }) => {
                                    if (active && payload && payload.length) {
                                        const data = payload[0].payload;
                                        return (
                                            <div className="bg-background/95 backdrop-blur-lg border border-border p-4 rounded-xl shadow-2xl max-w-[250px] animate-in fade-in zoom-in duration-200">
                                                <p className="text-xs font-bold text-muted-foreground uppercase tracking-wider mb-1">
                                                    {data.fullDate}
                                                </p>
                                                <div className="flex items-center gap-2 mb-2">
                                                    <span className="text-2xl font-black text-primary">
                                                        {data.mood ? data.mood.toFixed(1) : 'No Data'}
                                                    </span>
                                                    <span className="text-xs text-muted-foreground">/ 10.0</span>
                                                </div>
                                                {data.hasEntry && (
                                                    <p className="text-xs italic text-foreground line-clamp-3">
                                                        &quot;{data.content}&quot;
                                                    </p>
                                                )}
                                                {!data.hasEntry && (
                                                    <p className="text-xs text-muted-foreground">No journal entry for this day</p>
                                                )}
                                            </div>
                                        );
                                    }
                                    return null;
                                }}
                            />
                            {showAverage && (
                                <ReferenceLine
                                    y={averageMood}
                                    stroke="hsl(var(--primary))"
                                    strokeDasharray="5 5"
                                    label={{
                                        value: `AVG: ${averageMood.toFixed(1)}`,
                                        position: 'right',
                                        fill: 'hsl(var(--primary))',
                                        fontSize: 10,
                                        fontWeight: 'bold',
                                    }}
                                />
                            )}
                            <Area
                                type="monotone"
                                dataKey="mood"
                                stroke="hsl(var(--primary))"
                                strokeWidth={4}
                                fill="url(#moodGradient)"
                                connectNulls
                                dot={{
                                    r: 4,
                                    fill: 'hsl(var(--primary))',
                                    strokeWidth: 2,
                                    stroke: 'hsl(var(--background))',
                                    fillOpacity: 1,
                                }}
                                activeDot={{
                                    r: 6,
                                    strokeWidth: 0,
                                    fill: 'hsl(var(--primary))',
                                }}
                                animationDuration={1500}
                                animationEasing="ease-in-out"
                            />
                        </AreaChart>
                    </ResponsiveContainer>
                </div>
            </CardContent>
        </Card>
    );
}
