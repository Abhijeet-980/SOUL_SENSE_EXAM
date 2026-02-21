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
      className="md:col-span-2"
    >
      <div className="flex flex-col gap-3 h-full justify-center">
        {sortedActivities.length > 0 ? (
          sortedActivities.map((activity) => (
            <Link
              key={`${activity.type}-${activity.id}`}
              href={activity.href}
              className="group flex items-center gap-4 p-3 rounded-2xl bg-white/40 dark:bg-black/20 border border-white/20 hover:bg-white/80 dark:hover:bg-black/40 transition-all hover:shadow-md"
            >
              <div
                className={cn(
                  'p-2 rounded-xl border',
                  activity.type === 'assessment'
                    ? 'bg-blue-500/10 text-blue-600 border-blue-500/20'
                    : 'bg-emerald-500/10 text-emerald-600 border-emerald-500/20'
                )}
              >
                {activity.type === 'assessment' ? (
                  <CheckCircle2 className="h-4 w-4" />
                ) : (
                  <BookText className="h-4 w-4" />
                )}
              </div>

              <div className="flex-1 min-w-0">
                <p className="text-sm font-bold truncate group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
                  {activity.title}
                </p>
                <p className="text-xs text-neutral-500 dark:text-neutral-400">
                  {formatDistanceToNow(new Date(activity.timestamp), { addSuffix: true })}
                </p>
              </div>

              <ChevronRight className="h-4 w-4 text-neutral-300 group-hover:text-neutral-500 transition-colors" />
            </Link>
          ))
        ) : (
          <div className="flex flex-col items-center justify-center py-6 text-center">
            <div className="p-4 rounded-full bg-neutral-100 dark:bg-neutral-800 mb-3">
              <Clock className="h-6 w-6 text-neutral-400" />
            </div>
            <p className="text-sm font-medium text-neutral-600 dark:text-neutral-400">
              No recent activity.
            </p>
            <p className="text-xs text-neutral-500 mt-1">Start by taking an assessment!</p>
          </div>
        )}
      </div>
    </BentoGridItem>
  );
};
