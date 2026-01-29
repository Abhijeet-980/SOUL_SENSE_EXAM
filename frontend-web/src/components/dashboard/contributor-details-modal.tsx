import { motion, AnimatePresence } from 'framer-motion';
import { X, GitPullRequest, ExternalLink, Calendar } from 'lucide-react';
import { Avatar, AvatarFallback, AvatarImage, Button } from '@/components/ui';
import { format, parseISO } from 'date-fns';

interface PR {
  title: string;
  number: number;
  state: string;
  html_url: string;
  created_at: string;
}

interface Contributor {
  login: string;
  avatar_url: string;
  pr_count: number;
  recent_prs: PR[];
}

interface ContributorDetailsModalProps {
  contributor: Contributor | null;
  isOpen: boolean;
  onClose: () => void;
}

export function ContributorDetailsModal({
  contributor,
  isOpen,
  onClose,
}: ContributorDetailsModalProps) {
  if (!contributor) return null;

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="fixed inset-0 bg-slate-900/40 backdrop-blur-sm z-[100] cursor-pointer"
          />

          {/* Modal Container */}
          <div className="fixed inset-0 flex items-center justify-center z-[101] pointer-events-none p-4">
            <motion.div
              initial={{ opacity: 0, scale: 0.9, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.9, y: 20 }}
              className="w-full max-w-lg bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl border border-slate-200 dark:border-white/10 rounded-3xl shadow-2xl overflow-hidden pointer-events-auto flex flex-col max-h-[80vh]"
            >
              {/* Header */}
              <div className="p-6 border-b border-slate-200 dark:border-white/5 flex items-center justify-between bg-white/50 dark:bg-white/5">
                <div className="flex items-center gap-4">
                  <Avatar className="h-12 w-12 ring-2 ring-blue-500/20 dark:ring-blue-500/50">
                    <AvatarImage src={contributor.avatar_url} alt={contributor.login} />
                    <AvatarFallback>{contributor.login[0]}</AvatarFallback>
                  </Avatar>
                  <div>
                    <h3 className="text-xl font-black tracking-tight text-slate-900 dark:text-white capitalize leading-tight">
                      {contributor.login}
                    </h3>
                    <div className="flex items-center gap-3">
                      <p className="text-[10px] font-bold text-slate-500 dark:text-slate-400 uppercase tracking-widest">
                        Core Contributor
                      </p>
                      <a
                        href={`https://github.com/${contributor.login}`}
                        target="_blank"
                        rel="noreferrer"
                        className="flex items-center gap-1 text-[10px] font-black text-blue-600 dark:text-blue-400 hover:text-blue-500 transition-colors uppercase"
                      >
                        <ExternalLink className="h-2.5 w-2.5" />
                        GitHub
                      </a>
                    </div>
                  </div>
                </div>
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={onClose}
                  className="h-8 w-8 rounded-full border border-slate-200 dark:border-white/10 hover:bg-slate-100 dark:hover:bg-white/5 active:scale-95 transition-all text-slate-500 dark:text-slate-400 hover:text-red-500 dark:hover:text-red-400"
                >
                  <X className="h-4 w-4" />
                </Button>
              </div>

              {/* Content */}
              <div className="flex-1 overflow-y-auto p-6 space-y-6 scrollbar-thin scrollbar-thumb-slate-300 dark:scrollbar-thumb-slate-700 scrollbar-track-transparent">
                {/* Stats Summary */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="p-4 rounded-2xl bg-white/50 dark:bg-white/5 border border-slate-200 dark:border-white/5 flex flex-col">
                    <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">
                      Impact Scale
                    </span>
                    <div className="flex items-end gap-1">
                      <span className="text-2xl font-black text-blue-600 dark:text-blue-400">
                        {contributor.pr_count}
                      </span>
                      <span className="text-[10px] font-bold text-slate-400 mb-1">Recent PRs</span>
                    </div>
                  </div>
                  <div className="p-4 rounded-2xl bg-white/50 dark:bg-white/5 border border-slate-200 dark:border-white/5 flex flex-col">
                    <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">
                      Contribution Type
                    </span>
                    <span className="text-lg font-black text-purple-600 dark:text-purple-400">
                      Engineering
                    </span>
                  </div>
                </div>

                {/* PR List */}
                <div className="space-y-4">
                  <div className="flex items-center gap-2 mb-2">
                    <GitPullRequest className="h-4 w-4 text-blue-500" />
                    <h4 className="text-sm font-black text-slate-900 dark:text-slate-200 uppercase tracking-tight">
                      Recent Activity Log
                    </h4>
                  </div>

                  {contributor.recent_prs.length === 0 ? (
                    <div className="text-center py-8 px-4 rounded-2xl bg-slate-50 dark:bg-white/5 border border-dashed border-slate-200 dark:border-white/10">
                      <p className="text-xs text-slate-500 font-medium">
                        No recent PR activity indexed.
                      </p>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {contributor.recent_prs.map((pr) => (
                        <div
                          key={pr.number}
                          className="p-4 rounded-2xl bg-white dark:bg-white/5 border border-slate-200 dark:border-white/5 hover:border-blue-500/30 dark:hover:border-blue-500/30 transition-all group/pr shadow-sm dark:shadow-none hover:shadow-md"
                        >
                          <div className="flex items-start justify-between gap-4">
                            <div className="space-y-1">
                              <p className="text-xs font-bold text-slate-800 dark:text-slate-200 leading-snug group-hover/pr:text-blue-600 dark:group-hover/pr:text-blue-400 transition-colors">
                                {pr.title}
                              </p>
                              <div className="flex items-center gap-3">
                                <span
                                  className={`text-[9px] font-black uppercase px-2 py-0.5 rounded ${
                                    pr.state === 'open'
                                      ? 'bg-green-500/10 text-green-600 dark:text-green-500'
                                      : 'bg-purple-500/10 text-purple-600 dark:text-purple-500'
                                  }`}
                                >
                                  {pr.state}
                                </span>
                                <div className="flex items-center gap-1 text-[9px] font-bold text-slate-500">
                                  <Calendar className="h-2.5 w-2.5" />
                                  {format(parseISO(pr.created_at), 'MMM d, yyyy')}
                                </div>
                              </div>
                            </div>
                            <a
                              href={pr.html_url}
                              target="_blank"
                              rel="noreferrer"
                              className="p-2 rounded-xl bg-slate-100 dark:bg-slate-800 text-slate-400 hover:text-white hover:bg-blue-600 transition-all active:scale-95"
                            >
                              <ExternalLink className="h-3.5 w-3.5" />
                            </a>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>

              {/* Footer */}
              <div className="p-6 bg-slate-50/50 dark:bg-black/20 border-t border-slate-200 dark:border-white/5">
                <Button
                  className="w-full h-12 bg-slate-900 dark:bg-white hover:bg-slate-800 dark:hover:bg-slate-200 text-white dark:text-slate-900 font-black uppercase tracking-widest rounded-xl shadow-xl active:scale-95 transition-all text-xs"
                  onClick={onClose}
                >
                  Close Insights
                </Button>
              </div>
            </motion.div>
          </div>
        </>
      )}
    </AnimatePresence>
  );
}
