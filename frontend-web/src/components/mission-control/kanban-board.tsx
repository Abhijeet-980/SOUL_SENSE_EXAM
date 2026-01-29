import React from 'react';
import { MissionItem, MissionStatus } from './types';
import { MissionCard } from './mission-card';

interface KanbanBoardProps {
  items: MissionItem[];
}

const COLUMNS: { id: MissionStatus; label: string; color: string }[] = [
  { id: 'Backlog', label: 'Backlog', color: 'border-slate-500/20' },
  { id: 'Ready', label: 'Ready / Todo', color: 'border-blue-500/20' },
  { id: 'In Progress', label: 'In Progress', color: 'border-orange-500/20' },
  { id: 'In Review', label: 'In Review', color: 'border-purple-500/20' },
  { id: 'Done', label: 'Done', color: 'border-green-500/20' },
];

export const KanbanBoard: React.FC<KanbanBoardProps> = ({ items }) => {
  // Memoize column sorting to avoid unnecessary re-renders
  const sortedColumns = React.useMemo(() => {
    return [...COLUMNS].sort((a, b) => {
      const aCount = items.filter((i) => i.status === a.id).length;
      const bCount = items.filter((i) => i.status === b.id).length;

      // If one is empty and other isn't, move empty to end
      if (aCount === 0 && bCount > 0) return 1;
      if (aCount > 0 && bCount === 0) return -1;

      // Otherwise maintain original COLUMNS order
      return COLUMNS.indexOf(a) - COLUMNS.indexOf(b);
    });
  }, [items]);

  return (
    <div className="h-full flex gap-4 overflow-x-auto pb-4">
      {sortedColumns.map((col) => {
        const colItems = items.filter((i) => i.status === col.id);

        return (
          <div
            key={col.id}
            className="min-w-[280px] w-[280px] flex flex-col h-full bg-slate-50/50 dark:bg-slate-900/40 rounded-xl border border-slate-200 dark:border-white/5 backdrop-blur-sm shadow-sm transition-all"
          >
            {/* Column Header */}
            <div
              className={`p-3 border-b border-dashed ${col.color} flex justify-between items-center bg-white/40 dark:bg-white/5 rounded-t-xl`}
            >
              <h3 className="font-bold text-sm text-slate-900 dark:text-slate-100">{col.label}</h3>
              <span className="text-xs bg-slate-200/50 dark:bg-white/10 px-2 py-0.5 rounded-full text-slate-500 dark:text-slate-400 font-mono font-bold">
                {colItems.length}
              </span>
            </div>

            {/* Scrollable Area */}
            <div className="flex-1 overflow-y-auto p-2 scrollbar-thin scrollbar-thumb-slate-200 dark:scrollbar-thumb-slate-800">
              <div className="flex flex-col gap-2 pb-2">
                {colItems.map((item) => (
                  <MissionCard key={item.id} item={item} />
                ))}

                {colItems.length === 0 && (
                  <div className="h-24 border-2 border-dashed border-slate-200 dark:border-slate-800 rounded-lg flex items-center justify-center m-2 opacity-50">
                    <span className="text-xs text-slate-400 font-medium italic">All Clear</span>
                  </div>
                )}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
};
