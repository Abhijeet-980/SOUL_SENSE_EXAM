'use client';

import { BentoGridItem } from './bento-grid';
import { Clock, CheckCircle2, BookText, ChevronRight } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { cn } from '@/lib/utils';
import Link from 'next/link';

export type ActivityType = 'assessment' | 'journal';

export interface ActivityItem {
  id: string | number;
  type: ActivityType;
  title: string;
  timestamp: string | Date;
  href: string;
}

interface RecentActivityProps {
  activities?: ActivityItem[];
  limit?: number;
}

export const RecentActivity = ({ activities = [], limit = 5 }: RecentActivityProps) => {
  const sortedActivities = [...activities]
    .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
    .slice(0, limit);

  return (
    <BentoGridItem
      title="Recent Activity"
      description="Your latest assessments and reflections."
      icon={<Clock className="h-4 w-4" />}
      className="md:col-span-2 bg-background/60 backdrop-blur-md border border-border/40 shadow-sm"
    >
      <div className="flex flex-col gap-2 mt-2">
        {sortedActivities.length > 0 ? (
          sortedActivities.map((activity) => (
            <Link
              key={`${activity.type}-${activity.id}`}
              href={activity.href}
              className="group flex items-center gap-4 p-3 rounded-xl border border-transparent hover:border-primary/10 hover:bg-primary/5 transition-all"
            >
              <div
                className={cn(
                  'p-2 rounded-lg border shadow-sm transition-colors',
                  activity.type === 'assessment'
                    ? 'bg-primary/5 text-primary border-primary/20'
                    : 'bg-emerald-500/5 text-emerald-600 border-emerald-500/20'
                )}
              >
                {activity.type === 'assessment' ? (
                  <CheckCircle2 className="h-4 w-4" strokeWidth={2} />
                ) : (
                  <BookText className="h-4 w-4" strokeWidth={2} />
                )}
              </div>

              <div className="flex-1 min-w-0">
                <p className="text-sm font-semibold truncate group-hover:text-primary transition-colors">
                  {activity.title}
                </p>
                <p className="text-[10px] sm:text-xs text-muted-foreground font-medium uppercase tracking-tight">
                  {formatDistanceToNow(new Date(activity.timestamp), { addSuffix: true })}
                </p>
              </div>

              <ChevronRight className="h-4 w-4 text-muted-foreground/30 group-hover:text-primary transition-colors" />
            </Link>
          ))
        ) : (
          <div className="flex flex-col items-center justify-center py-8 text-center bg-muted/20 rounded-2xl border border-dashed border-border/60">
            <Clock className="h-8 w-8 text-muted-foreground/40 mb-3" />
            <p className="text-sm font-semibold text-muted-foreground">No recent activity</p>
            <p className="text-xs text-muted-foreground/60 mt-1">
              Activities will appear here once you start exploring.
            </p>
          </div>
        )}
      </div>
    </BentoGridItem>
  );
};
