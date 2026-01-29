import React from 'react';
import { MissionItem } from './types';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui';
import { cn } from '@/lib/utils';
import { CheckCircle2, CircleDashed, GitPullRequest } from 'lucide-react';

interface DataGridProps {
  items: MissionItem[];
}

export const DataGrid: React.FC<DataGridProps> = ({ items }) => {
  return (
    <div className="rounded-xl border border-border/40 bg-card/20 backdrop-blur-sm overflow-hidden h-full flex flex-col">
      <div className="flex-1 overflow-auto scrollbar-thin scrollbar-thumb-muted-foreground/20">
        <table className="w-full caption-bottom text-sm text-left">
          <thead className="bg-muted/20 sticky top-0 z-10 backdrop-blur-md">
            <tr className="border-b border-border/40 transition-colors">
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground w-[100px]">
                ID
              </th>
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground">
                Title
              </th>
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground w-[120px]">
                Status
              </th>
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground w-[100px]">
                Priority
              </th>
              <th className="h-12 px-4 text-left align-middle font-medium text-muted-foreground w-[150px]">
                Assignee
              </th>
              <th className="h-12 px-4 text-right align-middle font-medium text-muted-foreground w-[100px]">
                Domain
              </th>
            </tr>
          </thead>
          <tbody className="[&_tr:last-child]:border-0">
            {items.map((item) => (
              <tr
                key={item.id}
                className="border-b border-border/40 transition-colors hover:bg-muted/10"
              >
                <td className="p-4 align-middle font-mono text-xs text-muted-foreground">
                  {isFinite(item.number) ? `#${item.number}` : item.id}
                </td>
                <td className="p-4 align-middle">
                  <div className="flex flex-col gap-1">
                    <span className="font-medium text-sm line-clamp-1">{item.title}</span>
                    <div className="flex gap-1 flex-wrap">
                      {item.labels.slice(0, 3).map((l) => (
                        <span
                          key={l}
                          className="text-[10px] bg-muted/30 px-1 rounded text-muted-foreground/70"
                        >
                          {l}
                        </span>
                      ))}
                    </div>
                  </div>
                </td>
                <td className="p-4 align-middle">
                  <div className="flex items-center gap-2">
                    {item.status === 'Done' ? (
                      <CheckCircle2 className="w-3.5 h-3.5 text-green-500" />
                    ) : item.type === 'pr' ? (
                      <GitPullRequest className="w-3.5 h-3.5 text-purple-500" />
                    ) : (
                      <CircleDashed className="w-3.5 h-3.5 text-slate-500" />
                    )}
                    <span className="text-xs font-medium">{item.status}</span>
                  </div>
                </td>
                <td className="p-4 align-middle">
                  <span
                    className={cn(
                      'text-[10px] px-1.5 py-0.5 rounded-full border font-normal',
                      item.priority === 'High'
                        ? 'text-red-400 border-red-500/50 bg-red-500/10'
                        : item.priority === 'Normal'
                          ? 'text-blue-400 border-blue-500/50 bg-blue-500/10'
                          : 'text-gray-400 border-gray-500/50 bg-gray-500/10'
                    )}
                  >
                    {item.priority}
                  </span>
                </td>
                <td className="p-4 align-middle">
                  {item.assignee ? (
                    <div className="flex items-center gap-2">
                      <Avatar className="h-5 w-5">
                        <AvatarImage src={item.assignee.avatar} />
                        <AvatarFallback className="text-[9px]">
                          {item.assignee.login[0]}
                        </AvatarFallback>
                      </Avatar>
                      <span className="text-xs text-muted-foreground truncate max-w-[80px]">
                        {item.assignee.login}
                      </span>
                    </div>
                  ) : (
                    <span className="text-xs text-muted-foreground/50 italic">Unassigned</span>
                  )}
                </td>
                <td className="p-4 align-middle text-right text-xs text-muted-foreground">
                  {item.domain}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
