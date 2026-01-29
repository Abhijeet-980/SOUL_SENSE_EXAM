import React from 'react';
import { motion } from 'framer-motion';
import { ExternalLink, GitPullRequest, CircleDot, AlertCircle } from 'lucide-react';
import { MissionItem } from './types';
import { cn } from '@/lib/utils';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui';

interface MissionCardProps {
  item: MissionItem;
}

export const MissionCard: React.FC<MissionCardProps> = ({ item }) => {
  const isPR = item.type === 'pr';

  const priorityColor = {
    High: 'text-red-400 border-red-400/30 bg-red-400/10',
    Normal: 'text-blue-400 border-blue-400/30 bg-blue-400/10',
    Low: 'text-slate-400 border-slate-400/30 bg-slate-400/10',
  }[item.priority];

  return (
    <motion.div
      layoutId={item.id}
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      whileHover={{ y: -2, transition: { duration: 0.2 } }}
      className="group relative p-3 rounded-xl bg-white/60 dark:bg-slate-900/60 border border-slate-200 dark:border-white/5 hover:border-slate-300 dark:hover:border-white/10 hover:shadow-md transition-all backdrop-blur-sm cursor-grab active:cursor-grabbing"
    >
      <div className="flex justify-between items-start mb-2">
        <span
          className={cn(
            'text-[10px] px-1.5 py-0.5 rounded-md border font-bold uppercase tracking-wider',
            priorityColor
          )}
        >
          {item.priority}
        </span>
        <span className="text-[10px] font-bold text-slate-400 font-mono">#{item.number}</span>
      </div>

      <h4 className="text-sm font-bold text-slate-900 dark:text-slate-100 leading-snug mb-3 line-clamp-2 group-hover:text-blue-500 transition-colors">
        {item.title}
      </h4>

      <div className="flex items-center justify-between mt-auto">
        <div className="flex items-center gap-2">
          {item.assignee ? (
            <Avatar className="h-6 w-6 ring-1 ring-white dark:ring-slate-800">
              <AvatarImage src={item.assignee.avatar} />
              <AvatarFallback className="text-[9px] font-bold bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400">
                {item.assignee.login[0].toUpperCase()}
              </AvatarFallback>
            </Avatar>
          ) : (
            <div className="h-6 w-6 rounded-full bg-slate-100 dark:bg-slate-800/50 border border-dashed border-slate-300 dark:border-slate-700 flex items-center justify-center">
              <span className="text-[10px] font-bold text-slate-400">?</span>
            </div>
          )}

          <div className="flex gap-1">
            {isPR ? (
              <GitPullRequest className="w-3.5 h-3.5 text-purple-500" />
            ) : (
              <CircleDot className="w-3.5 h-3.5 text-emerald-500" />
            )}
          </div>
        </div>

        <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
          <a
            href={item.url}
            target="_blank"
            rel="noreferrer"
            className="p-1.5 hover:bg-slate-100 dark:hover:bg-white/10 rounded-lg transition-colors"
          >
            <ExternalLink className="w-3.5 h-3.5 text-slate-400 hover:text-slate-900 dark:hover:text-white" />
          </a>
        </div>
      </div>
    </motion.div>
  );
};
