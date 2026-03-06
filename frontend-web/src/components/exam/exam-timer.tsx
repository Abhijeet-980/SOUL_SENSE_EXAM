'use client';

import React from 'react';
import { motion } from 'framer-motion';
import { Clock } from 'lucide-react';
import { useTimer } from '@/hooks/useTimer';
import { cn } from '@/lib/utils';

interface ExamTimerProps {
  durationMinutes: number;
  onTimeUp: () => void;
  isPaused?: boolean;
  className?: string;
}

export const ExamTimer: React.FC<ExamTimerProps> = ({
  durationMinutes,
  onTimeUp,
  isPaused = false,
  className,
}) => {
  const { formattedTime, color, isWarning } = useTimer({
    durationMinutes,
    onTimeUp,
    isPaused,
  });

  return (
    <motion.div
      className={cn(
        'flex items-center gap-2 rounded-lg border px-4 py-2 font-mono text-lg font-semibold',
        color,
        isWarning && 'border-current',
        className
      )}
      animate={isWarning ? {
        scale: [1, 1.05, 1],
        transition: {
          duration: 1,
          repeat: Infinity,
          repeatType: 'reverse' as const,
        },
      } : {}}
      initial={false}
    >
      <Clock className="h-5 w-5" />
      <span>{formattedTime}</span>
      {isPaused && (
        <motion.span
          className="ml-2 text-sm font-normal text-muted-foreground"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
        >
          (Paused)
        </motion.span>
      )}
    </motion.div>
  );
};