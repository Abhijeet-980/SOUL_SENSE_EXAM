import React, { useState, useRef, useEffect } from 'react';
import { Search, X, Check, ChevronDown, Filter } from 'lucide-react';
import { Input, Button } from '@/components/ui';
import { cn } from '@/lib/utils';
import { AnimatePresence, motion } from 'framer-motion';

interface ActiveFilters {
  priorities: string[];
  status: string[];
  domains: string[];
}

interface DataFiltersProps {
  onSearch: (query: string) => void;
  filters: ActiveFilters;
  onFilterChange: (type: keyof ActiveFilters, value: string) => void;
  onReset: () => void;
}

// Helper hook for closing dropdowns on outside click
function useOnClickOutside(ref: React.RefObject<HTMLDivElement>, handler: () => void) {
  useEffect(() => {
    const listener = (event: MouseEvent | TouchEvent) => {
      if (!ref.current || ref.current.contains(event.target as Node)) {
        return;
      }
      handler();
    };
    document.addEventListener('mousedown', listener);
    document.addEventListener('touchstart', listener);
    return () => {
      document.removeEventListener('mousedown', listener);
      document.removeEventListener('touchstart', listener);
    };
  }, [ref, handler]);
}

export const DataFilters: React.FC<DataFiltersProps> = ({
  onSearch,
  filters,
  onFilterChange,
  onReset,
}) => {
  const [activeDropdown, setActiveDropdown] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useOnClickOutside(containerRef, () => setActiveDropdown(null));

  const totalFilters = filters.priorities.length + filters.status.length + filters.domains.length;

  const toggleDropdown = (name: string) => {
    setActiveDropdown(activeDropdown === name ? null : name);
  };

  const clearAll = () => {
    // We'd ideally need a clear function from parent, but we can iterate to toggle off
    // Actually, parent state management suggests we might need a reset prop or just let user manually clear.
    // For now, let's just keep the UX of individual toggles or we can expose a clear handler if we refactor parent.
    // Since we can't easily "reset" without parent help or many calls, let's trust the user to uncheck
    // OR we can hack it by calling onFilterChange for every active item (inefficient).
    // Let's stick to individual category management for this iteration,
    // but we can add a visual "Clear" that just visually guides them or we add a `onClearAll` prop later.
    // Wait, the user wants "User Friendly". Let's unimplemented "Clear All" logic in UI for now
    // unless we update the parent. Let's start with just better dropdowns.
  };

  // Actually, let's implement the dropdowns first.

  return (
    <div
      className="flex flex-col sm:flex-row gap-3 items-center w-full max-w-2xl"
      ref={containerRef}
    >
      {/* Search Bar - Always visible and wide */}
      <div className="relative flex-1 w-full">
        <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Search objectives..."
          className="pl-9 h-9 bg-card/40 border-border/40 focus-visible:ring-1 transition-all focus:bg-card/80"
          onChange={(e) => onSearch(e.target.value)}
        />
      </div>

      <motion.div layout className="flex flex-wrap items-center gap-2 w-full sm:w-auto">
        {/* Priority Dropdown */}
        <div
          className="relative"
          onMouseEnter={() => setActiveDropdown('priority')}
          onMouseLeave={() => setActiveDropdown(null)}
        >
          <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
            <Button
              variant="outline"
              size="sm"
              onClick={() => toggleDropdown('priority')}
              className={cn(
                'h-9 gap-2 border-dashed border-border/60 bg-card/40 hover:bg-card/80 hover:border-primary/50 transition-all',
                filters.priorities.length > 0 && 'border-solid border-primary/50 bg-primary/5'
              )}
            >
              <span className="text-xs font-medium">Priority</span>
              {filters.priorities.length > 0 && (
                <span className="flex items-center justify-center bg-primary text-primary-foreground text-[10px] font-bold h-4 w-4 rounded-full">
                  {filters.priorities.length}
                </span>
              )}
              <ChevronDown className="h-3 w-3 opacity-50" />
            </Button>
          </motion.div>

          <AnimatePresence>
            {activeDropdown === 'priority' && (
              <motion.div
                initial={{ opacity: 0, y: 4, scale: 0.95 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: 2, scale: 0.95 }}
                className="absolute top-10 left-0 w-48 bg-card/95 backdrop-blur-xl border border-border/40 rounded-xl shadow-xl p-2 z-50 flex flex-col gap-1"
              >
                {['High', 'Normal', 'Low'].map((p) => {
                  const isSelected = filters.priorities.includes(p);
                  return (
                    <button
                      key={p}
                      onClick={() => onFilterChange('priorities', p)}
                      className={cn(
                        'flex items-center justify-between w-full px-3 py-2 text-sm rounded-lg transition-colors',
                        isSelected
                          ? 'bg-primary/10 text-primary'
                          : 'hover:bg-muted/50 text-muted-foreground hover:text-foreground'
                      )}
                    >
                      <div className="flex items-center gap-2">
                        <span
                          className={cn(
                            'w-2 h-2 rounded-full',
                            p === 'High'
                              ? 'bg-red-400'
                              : p === 'Normal'
                                ? 'bg-blue-400'
                                : 'bg-slate-400'
                          )}
                        />
                        {p}
                      </div>
                      {isSelected && <Check className="h-3.5 w-3.5" />}
                    </button>
                  );
                })}
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Status Dropdown */}
        <div
          className="relative"
          onMouseEnter={() => setActiveDropdown('status')}
          onMouseLeave={() => setActiveDropdown(null)}
        >
          <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
            <Button
              variant="outline"
              size="sm"
              onClick={() => toggleDropdown('status')}
              className={cn(
                'h-9 gap-2 border-dashed border-border/60 bg-card/40 hover:bg-card/80 hover:border-primary/50 transition-all',
                filters.status.length > 0 && 'border-solid border-primary/50 bg-primary/5'
              )}
            >
              <span className="text-xs font-medium">Status</span>
              {filters.status.length > 0 && (
                <span className="flex items-center justify-center bg-primary text-primary-foreground text-[10px] font-bold h-4 w-4 rounded-full">
                  {filters.status.length}
                </span>
              )}
              <ChevronDown className="h-3 w-3 opacity-50" />
            </Button>
          </motion.div>

          <AnimatePresence>
            {activeDropdown === 'status' && (
              <motion.div
                initial={{ opacity: 0, y: 4, scale: 0.95 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: 2, scale: 0.95 }}
                className="absolute top-10 left-0 w-48 bg-card/95 backdrop-blur-xl border border-border/40 rounded-xl shadow-xl p-2 z-50 flex flex-col gap-1"
              >
                {['Backlog', 'Ready', 'In Progress', 'In Review', 'Done'].map((s) => {
                  const isSelected = filters.status.includes(s);
                  return (
                    <button
                      key={s}
                      onClick={() => onFilterChange('status', s)}
                      className={cn(
                        'flex items-center justify-between w-full px-3 py-2 text-sm rounded-lg transition-colors',
                        isSelected
                          ? 'bg-primary/10 text-primary'
                          : 'hover:bg-muted/50 text-muted-foreground hover:text-foreground'
                      )}
                    >
                      <span>{s}</span>
                      {isSelected && <Check className="h-3.5 w-3.5" />}
                    </button>
                  );
                })}
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Domain Dropdown */}
        <div
          className="relative"
          onMouseEnter={() => setActiveDropdown('domain')}
          onMouseLeave={() => setActiveDropdown(null)}
        >
          <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
            <Button
              variant="outline"
              size="sm"
              onClick={() => toggleDropdown('domain')}
              className={cn(
                'h-9 gap-2 border-dashed border-border/60 bg-card/40 hover:bg-card/80 hover:border-primary/50 transition-all',
                filters.domains.length > 0 && 'border-solid border-primary/50 bg-primary/5'
              )}
            >
              <span className="text-xs font-medium">Domain</span>
              {filters.domains.length > 0 && (
                <span className="flex items-center justify-center bg-primary text-primary-foreground text-[10px] font-bold h-4 w-4 rounded-full">
                  {filters.domains.length}
                </span>
              )}
              <ChevronDown className="h-3 w-3 opacity-50" />
            </Button>
          </motion.div>

          <AnimatePresence>
            {activeDropdown === 'domain' && (
              <motion.div
                initial={{ opacity: 0, y: 4, scale: 0.95 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: 2, scale: 0.95 }}
                className="absolute top-10 left-0 w-48 bg-card/95 backdrop-blur-xl border border-border/40 rounded-xl shadow-xl p-2 z-50 flex flex-col gap-1"
              >
                {['Frontend', 'Backend', 'DevOps', 'Docs', 'General'].map((d) => {
                  const isSelected = filters.domains.includes(d);
                  return (
                    <button
                      key={d}
                      onClick={() => onFilterChange('domains', d)}
                      className={cn(
                        'flex items-center justify-between w-full px-3 py-2 text-sm rounded-lg transition-colors',
                        isSelected
                          ? 'bg-primary/10 text-primary'
                          : 'hover:bg-muted/50 text-muted-foreground hover:text-foreground'
                      )}
                    >
                      <span>{d}</span>
                      {isSelected && <Check className="h-3.5 w-3.5" />}
                    </button>
                  );
                })}
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Clear All - Always visible to prevent layout shift */}
        <Button
          variant="ghost"
          size="sm"
          onClick={onReset}
          disabled={totalFilters === 0}
          className={cn(
            'h-8 px-2 transition-all',
            totalFilters === 0
              ? 'opacity-50 grayscale cursor-not-allowed hidden sm:flex' /* Hidden on mobile when empty to save space, visible on desktop */
              : 'text-muted-foreground hover:text-foreground hover:bg-destructive/10 hover:text-destructive'
          )}
          title="Clear all filters"
        >
          <span className="text-xs font-medium">Reset</span>
          <X className="ml-1 h-3 w-3" />
        </Button>
      </motion.div>
    </div>
  );
};
