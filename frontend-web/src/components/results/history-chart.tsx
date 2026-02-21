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
  Legend,
} from 'recharts';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui';
import { format, parseISO, subDays, isAfter } from 'date-fns';

export interface ExamResult {
  id: string | number;
  timestamp: string; // ISO date format
  score: number; // 0-100
  categories?: Record<string, number>; // Optional category break-down
}

interface HistoryChartProps {
  results: ExamResult[];
  timeRange: '7d' | '30d' | '90d' | 'all';
  showCategories: boolean;
}

const CATEGORY_COLORS = [
  'hsl(var(--primary))',
  'hsl(var(--secondary))',
  '#3b82f6', // blue
  '#0ea5e9', // sky
  '#8b5cf6', // violet
  '#f43f5e', // rose
];

/**
 * Custom Tooltip for the history chart to match the premium aesthetic
 */
const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="backdrop-blur-2xl bg-popover/80 border border-border/50 p-4 rounded-xl shadow-xl">
        <p className="text-slate-400 text-xs font-bold uppercase tracking-wider mb-2">
          {format(parseISO(label), 'PPP')}
        </p>
        <div className="space-y-1.5">
          {payload.map((entry: any, index: number) => (
            <div key={index} className="flex items-center justify-between gap-4">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full" style={{ backgroundColor: entry.color }} />
                <span className="text-white text-sm font-medium">{entry.name}</span>
              </div>
              <span className="text-white text-sm font-bold">{entry.value}%</span>
            </div>
          ))}
        </div>
      </div>
    );
  }
  return null;
};

export const HistoryChart: React.FC<HistoryChartProps> = ({
  results = [],
  timeRange,
  showCategories,
}) => {
  // 1. Filter by time range
  const filteredData = useMemo(() => {
    if (!results || results.length === 0) return [];
    if (timeRange === 'all') return results;

    const now = new Date();
    const days = timeRange === '7d' ? 7 : timeRange === '30d' ? 30 : 90;
    const cutoffDate = subDays(now, days);

    return results.filter((result) => {
      try {
        return isAfter(parseISO(result.timestamp), cutoffDate);
      } catch (e) {
        return false;
      }
    });
  }, [results, timeRange]);

  // 2. Sort by time (ascending for charts)
  const sortedData = useMemo(() => {
    return [...filteredData].sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
  }, [filteredData]);

  // 3. Extract all unique categories present in the data
  const categories = useMemo(() => {
    if (!showCategories || sortedData.length === 0) return [];
    const allCats = new Set<string>();
    sortedData.forEach((res) => {
      if (res.categories) {
        Object.keys(res.categories).forEach((cat) => allCats.add(cat));
      }
    });
    return Array.from(allCats);
  }, [showCategories, sortedData]);

  // Handle empty state
  if (!results || results.length === 0) {
    return (
      <Card className="w-full h-[400px] flex items-center justify-center backdrop-blur-md lg:backdrop-blur-2xl bg-background/60 border border-border/40 shadow-sm rounded-2xl">
        <div className="text-center p-8 max-w-sm">
          <div className="w-16 h-16 bg-slate-100 dark:bg-slate-800 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg
              className="w-8 h-8 text-slate-400"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={1.5}
                d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
              />
            </svg>
          </div>
          <h3 className="text-lg font-semibold text-slate-900 dark:text-white mb-2">
            No history yet
          </h3>
          <p className="text-sm text-slate-500 dark:text-slate-400">
            Start taking assessments to see your growth and emotional patterns visualized here.
          </p>
        </div>
      </Card>
    );
  }

  return (
    <Card className="w-full backdrop-blur-md lg:backdrop-blur-2xl bg-background/60 border border-border/40 shadow-sm hover:shadow-md transition-all rounded-3xl overflow-hidden">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-xl font-bold text-slate-900 dark:text-white">
              Score Trends
            </CardTitle>
            <CardDescription className="text-sm text-slate-500 font-medium">
              Tracking your progress over{' '}
              {timeRange === 'all' ? 'the entire period' : `the last ${timeRange}`}
            </CardDescription>
          </div>
          <div className="px-3 py-1 bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400 text-xs font-bold rounded-full uppercase tracking-wider">
            {timeRange}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="h-[380px] w-full mt-6">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={sortedData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} strokeOpacity={0.06} />
              <XAxis
                dataKey="timestamp"
                tickFormatter={(val) => format(parseISO(val), 'MMM d')}
                stroke="#94a3b8"
                fontSize={12}
                tickLine={false}
                axisLine={false}
                minTickGap={40}
                tick={{ dy: 10 }}
              />
              <YAxis
                domain={[0, 100]}
                stroke="#94a3b8"
                fontSize={12}
                tickLine={false}
                axisLine={false}
                tickFormatter={(val) => `${val}`}
                tick={{ dx: -10 }}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend
                verticalAlign="top"
                align="right"
                iconType="circle"
                wrapperStyle={{
                  paddingBottom: '30px',
                  fontSize: '12px',
                  fontWeight: '600',
                  color: '#64748b',
                }}
              />

              {/* Main Overall Score Line */}
              <Line
                type="monotone"
                dataKey="score"
                name="Overall Performance"
                stroke="hsl(var(--primary))"
                strokeWidth={4}
                dot={{
                  r: 5,
                  fill: 'hsl(var(--primary))',
                  strokeWidth: 2,
                  stroke: '#fff',
                }}
                activeDot={{
                  r: 8,
                  strokeWidth: 0,
                  fill: 'hsl(var(--primary))',
                }}
                animationDuration={1500}
                connectNulls
              />

              {/* Category Lines */}
              {showCategories &&
                categories.map((cat, index) => (
                  <Line
                    key={cat}
                    type="monotone"
                    dataKey={`categories.${cat}`}
                    name={cat}
                    stroke={CATEGORY_COLORS[index % CATEGORY_COLORS.length]}
                    strokeWidth={2}
                    strokeDasharray="4 4"
                    dot={false}
                    activeDot={{ r: 6 }}
                    animationDuration={1500}
                    connectNulls
                  />
                ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
};

export default HistoryChart;
