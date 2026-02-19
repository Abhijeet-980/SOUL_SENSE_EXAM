'use client';

import { useEffect, useState } from 'react';
import { BentoGrid } from '@/components/dashboard/bento-grid';
import {
  WelcomeCard,
  QuickActions,
  MoodWidget,
  RecentActivity,
  InsightCard,
  DashboardSkeleton,
  ActivityItem,
} from '@/components/dashboard';
import { SectionWrapper } from '@/components/dashboard/section-wrapper';
import { apiClient } from '@/lib/api/client';
import { useAuth } from '@/hooks/useAuth';

interface DashboardData {
  profile: any | null;
  exams: any[];
  journals: any[];
  mood: any | null;
  insights: Array<{ title: string; description: string; type: string }>;
}

export default function DashboardPage() {
  const { user } = useAuth();
  const [data, setData] = useState<DashboardData>({
    profile: null,
    exams: [],
    journals: [],
    mood: null,
    insights: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<any>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [examsRes, journalsRes] = await Promise.all([
        apiClient<any>('/exams/history?page=1&page_size=5').catch(() => ({ assessments: [] })),
        apiClient<any>('/journal/?limit=5').catch(() => ({ entries: [] })),
      ]);

      setData({
        profile: user,
        exams: examsRes.assessments || [],
        journals: journalsRes.entries || [],
        mood: null,
        insights: [
          {
            title: 'Sleep Pattern',
            description: 'You tend to score higher on EQ assessments when you get 7+ hours of sleep.',
            type: 'trend',
          },
          {
            title: 'Mindfulness Tip',
            description: 'Try a 5-minute breathing exercise before your next exam to reduce anxiety.',
            type: 'tip',
          },
        ],
      });
    } catch (err) {
      console.error('Error fetching dashboard data:', err);
      setError(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [user]);

  if (loading && !data.profile) {
    return (
      <div className="p-4 md:p-8 space-y-8">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-muted-foreground mt-2">Loading your overview...</p>
import { useApi, useOnlineStatus } from '@/hooks/useApi';
import { dashboardApi } from '@/lib/api/dashboard';
import { ErrorDisplay, LoadingState, OfflineBanner, Skeleton } from '@/components/common';

export default function DashboardPage() {
  const { user } = useAuth();
  const [data, setData] = useState<DashboardData>({
    profile: null,
    exams: [],
    journals: [],
    mood: null,
    insights: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<any>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [examsRes, journalsRes] = await Promise.all([
        apiClient<any>('/exams/history?page=1&page_size=5').catch(() => ({ assessments: [] })),
        apiClient<any>('/journal/?limit=5').catch(() => ({ entries: [] })),
      ]);

      setData({
        profile: user,
        exams: examsRes.assessments || [],
        journals: journalsRes.entries || [],
        mood: null,
        insights: [
          {
            title: 'Sleep Pattern',
            description: 'You tend to score higher on EQ assessments when you get 7+ hours of sleep.',
            type: 'trend',
          },
          {
            title: 'Mindfulness Tip',
            description: 'Try a 5-minute breathing exercise before your next exam to reduce anxiety.',
            type: 'tip',
          },
        ],
      });
    } catch (err) {
      console.error('Error fetching dashboard data:', err);
      setError(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [user]);

  // Combine exams and journals into activities
  const activities: ActivityItem[] = [
    ...data.exams.map(e => ({
      id: e.id,
      type: 'assessment' as const,
      title: `EQ Assessment - Score: ${e.total_score || e.score || 0}%`,
      timestamp: e.timestamp || e.created_at,
      href: `/results/${e.id}`
    })),
    ...data.journals.map(j => ({
      id: j.id,
      type: 'journal' as const,
      title: j.content?.substring(0, 30) + (j.content?.length > 30 ? '...' : '') || 'Untitled Journal',
      timestamp: j.created_at || j.timestamp,
      href: `/journal/${j.id}`
    }))
  ];

  if (loading && !data.profile) {
    return (
      <div className="p-4 md:p-8 space-y-8">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-muted-foreground mt-2">Loading your overview...</p>
        </div>
        <DashboardSkeleton />
      </div>
    );
  }

  return (
    <div className="p-4 md:p-8 space-y-8 max-w-7xl mx-auto">
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h1 className="text-4xl font-extrabold tracking-tight bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
            SoulSense Dashboard
          </h1>
          <p className="text-muted-foreground mt-2 text-lg">
            Welcome back, {user?.name || 'User'}. Here&apos;s your mental wellbeing at a glance.
          </p>
        </div>
      </div>

      <BentoGrid className="auto-rows-[20rem]">
        {/* Row 1 */}
        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <WelcomeCard />
        </SectionWrapper>

        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <QuickActions />
        </SectionWrapper>

        {/* Row 2 */}
        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <MoodWidget />
        </SectionWrapper>

        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <RecentActivity activities={activities} />
        </SectionWrapper>

        {/* AI Insights - Multiple */}
        {data.insights.map((insight, idx) => (
          <SectionWrapper key={`insight-${idx}`} isLoading={loading} error={error} onRetry={fetchData}>
            <InsightCard
              insight={{
                title: insight.title,
                content: insight.description,
                type: insight.type as any,
                actionLabel: insight.type === 'tip' ? 'View Guide' : 'Analyze Pattern'
              }}
              onDismiss={() => {
                setData(prev => ({
                  ...prev,
                  insights: prev.insights.filter((_, i) => i !== idx)
                }));
              }}
              onAction={(ins) => console.log('Action for:', ins.title)}
              className="md:col-span-1"
            />
          </SectionWrapper>
        ))}

        {/* Additional Insight or Filler */}
        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <InsightCard
            insight={{
              title: "Security & Privacy",
              content: "Your data is encrypted and only accessible by you. We prioritize your privacy.",
              type: "safety" as any,
            }}
      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="rounded-lg border bg-card p-6 hover:shadow-md transition-shadow">
          <h3 className="font-semibold text-sm text-muted-foreground">Total Tests Taken</h3>
          <p className="text-3xl font-bold mt-2">{summary.total_assessments}</p>
        </div>
        <div className="rounded-lg border bg-card p-6 hover:shadow-md transition-shadow">
          <h3 className="font-semibold text-sm text-muted-foreground">Average Score</h3>
          <p className="text-3xl font-bold mt-2">{summary.average_score}%</p>
        </div>
        <div className="rounded-lg border bg-card p-6 hover:shadow-md transition-shadow">
          <h3 className="font-semibold text-sm text-muted-foreground">Journal Entries</h3>
          <p className="text-3xl font-bold mt-2">{summary.journal_entries_count}</p>
        </div>
        <div className="rounded-lg border bg-card p-6 hover:shadow-md transition-shadow">
          <h3 className="font-semibold text-sm text-muted-foreground">Current Streak</h3>
          <p className="text-3xl font-bold mt-2">{summary.current_streak} days</p>
        </div>
        <DashboardSkeleton />
      </div>
    );
  }

  return (
    <div className="p-4 md:p-8 space-y-8 max-w-7xl mx-auto">
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h1 className="text-4xl font-extrabold tracking-tight bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
            SoulSense Dashboard
          </h1>
          <p className="text-muted-foreground mt-2 text-lg">
            Welcome back, {user?.name || 'User'}. Here&apos;s your mental wellbeing at a glance.
          </p>
        </div>
      {/* Wellbeing Score (if available) */}
      {summary.wellbeing_score !== undefined && (
        <div className="rounded-lg border bg-card p-6">
          <h2 className="text-xl font-semibold mb-4">Overall Wellbeing</h2>
          <div className="flex items-center gap-4">
            <div className="text-5xl font-bold text-primary">{summary.wellbeing_score}</div>
            <div className="flex-1">
              <div className="h-4 bg-muted rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-primary to-primary/80 transition-all duration-500"
                  style={{ width: `${summary.wellbeing_score}%` }}
                />
              </div>
              <p className="text-sm text-muted-foreground mt-2">
                {summary.wellbeing_score >= 80
                  ? 'Excellent'
                  : summary.wellbeing_score >= 60
                    ? 'Good'
                    : summary.wellbeing_score >= 40
                      ? 'Fair'
                      : 'Needs Improvement'}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Recent Activity */}
      <div className="rounded-lg border bg-card p-6">
        <h2 className="text-xl font-semibold mb-4">Recent Activity</h2>
        {summary.recent_activity && summary.recent_activity.length > 0 ? (
          <div className="space-y-3">
            {summary.recent_activity.map((activity) => (
              <div
                key={activity.id}
                className="flex items-start gap-3 p-3 rounded-lg hover:bg-muted/50 transition-colors"
              >
                <div
                  className={`mt-1 h-2 w-2 rounded-full ${
                    activity.type === 'assessment'
                      ? 'bg-blue-500'
                      : activity.type === 'journal'
                        ? 'bg-emerald-500'
                        : 'bg-amber-500'
                  }`}
                />
                <div className="flex-1">
                  <p className="font-medium">{activity.title}</p>
                  {activity.description && (
                    <p className="text-sm text-muted-foreground">{activity.description}</p>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {new Date(activity.timestamp).toLocaleDateString()}
                  </p>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-muted-foreground">
            No recent activity. Start by taking an assessment!
          </p>
        )}
      </div>

      <BentoGrid className="auto-rows-[20rem]">
        {/* Row 1 */}
        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <WelcomeCard />
        </SectionWrapper>

        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <QuickActions />
        </SectionWrapper>

        {/* Row 2 */}
        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <MoodWidget />
        </SectionWrapper>

        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <RecentActivity />
        </SectionWrapper>

        {/* AI Insights - Multiple */}
        {data.insights.map((insight, idx) => (
          <SectionWrapper key={`insight-${idx}`} isLoading={loading} error={error} onRetry={fetchData}>
            <InsightCard
<<<<<<< ai-generated-insights
              insight={{
                title: insight.title,
                content: insight.description,
                type: insight.type as any,
                actionLabel: insight.type === 'tip' ? 'View Guide' : 'Analyze Pattern'
              }}
              onDismiss={() => {
                setData(prev => ({
                  ...prev,
                  insights: prev.insights.filter((_, i) => i !== idx)
                }));
              }}
              onAction={(ins) => console.log('Action for:', ins.title)}
=======
              title={insight.title}
              description={insight.description}
              type={insight.type as any}
>>>>>>> main
              className="md:col-span-1"
            />
          </SectionWrapper>
        ))}

        {/* Additional Insight or Filler */}
        <SectionWrapper isLoading={loading} error={error} onRetry={fetchData}>
          <InsightCard
<<<<<<< ai-generated-insights
            insight={{
              title: "Security & Privacy",
              content: "Your data is encrypted and only accessible by you. We prioritize your privacy.",
              type: "safety" as any, // safety is handled by the component or defaults
            }}
=======
            title="Security & Privacy"
            description="Your data is encrypted and only accessible by you. We prioritize your privacy."
            type="safety"
>>>>>>> main
            className="md:col-span-1"
          />
        </SectionWrapper>
      </BentoGrid>
    </div>
  );
}
