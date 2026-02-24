import { Skeleton } from '@/components/ui';

export function DashboardSkeleton() {
  return (
    <div className="flex flex-col space-y-6 animate-pulse pb-10">
      {/* Header skeleton */}
      <div className="space-y-2">
        <Skeleton className="h-10 w-48 bg-slate-200 dark:bg-slate-800/50" />
        <Skeleton className="h-5 w-64 bg-slate-200 dark:bg-slate-800/50" />
      </div>

      {/* BentoGrid skeleton matching the actual layout */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 max-w-7xl mx-auto">
        {/* Row 1: WelcomeCard and QuickActions */}
        <Skeleton className="md:col-span-2 h-[18rem] rounded-3xl bg-slate-200 dark:bg-slate-800/50" />
        <Skeleton className="h-[18rem] rounded-3xl bg-slate-200 dark:bg-slate-800/50" />

        {/* Row 2: MoodWidget and RecentActivity */}
        <Skeleton className="h-[18rem] rounded-3xl bg-slate-200 dark:bg-slate-800/50" />
        <Skeleton className="md:col-span-2 h-[18rem] rounded-3xl bg-slate-200 dark:bg-slate-800/50" />

        {/* Row 3: AI Insights (3 items) */}
        <Skeleton className="h-[18rem] rounded-3xl bg-slate-200 dark:bg-slate-800/50" />
        <Skeleton className="h-[18rem] rounded-3xl bg-slate-200 dark:bg-slate-800/50" />
        <Skeleton className="h-[18rem] rounded-3xl bg-slate-200 dark:bg-slate-800/50" />

        {/* Row 4: Additional Insight */}
        <Skeleton className="md:col-span-3 h-[18rem] rounded-3xl bg-slate-200 dark:bg-slate-800/50" />
      </div>
    </div>
  );
}
