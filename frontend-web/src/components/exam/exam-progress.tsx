'use client';

import React from 'react';
import { motion } from 'framer-motion';
import { Progress } from '@/components/ui/progress';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface ExamProgressProps {
  /** Current question index (1-based) */
  current: number;
  /** Total number of questions */
  total: number;
  /** Number of questions answered */
  answeredCount: number;
  /** Callback fired when clicking a question number/dot */
  onJumpToQuestion?: (questionIndex: number) => void;
  /** Additional CSS classes */
  className?: string;
  /** Show clickable question dots */
  showQuestionDots?: boolean;
}

export const ExamProgress: React.FC<ExamProgressProps> = ({
  current,
  total,
  answeredCount,
  onJumpToQuestion,
  className,
  showQuestionDots = true,
}) => {
  // Calculate progress percentage (0-100)
  const progressPercentage = (answeredCount / total) * 100;

  return (
    <div className={cn('w-full space-y-4', className)}>
      {/* Progress bar section */}
      <div className="space-y-2">
        <div className="flex flex-col gap-2 sm:gap-0 sm:flex-row items-start sm:items-center justify-between text-sm md:text-base">
          <span className="font-medium text-slate-700 dark:text-slate-300">
            Question <span className="font-bold text-primary">{current}</span> of{' '}
            <span className="font-bold">{total}</span>
          </span>
          <span className="text-sm text-slate-600 dark:text-slate-400 font-medium">
            <span className="text-primary font-bold">{answeredCount}</span> answered
          </span>
        </div>

        {/* Visual progress bar */}
        <Progress
          value={progressPercentage}
          showLabel={false}
          size="sm"
          color="primary"
          className="w-full"
        />

        {/* Progress text */}
        <div className="text-xs text-slate-500 dark:text-slate-400 text-right">
          {Math.round(progressPercentage)}% Complete
        </div>
      </div>

      {/* Clickable question dots */}
      {showQuestionDots && total <= 50 && (
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="flex flex-wrap gap-2 pt-2"
        >
          {Array.from({ length: total }).map((_, index) => {
            const questionNum = index + 1;
            const isAnswered = questionNum <= answeredCount;
            const isCurrent = questionNum === current;

            return (
              <motion.div
                key={questionNum}
                whileHover={{ scale: 1.1 }}
                whileTap={{ scale: 0.95 }}
              >
                <Button
                  variant={isCurrent ? 'default' : isAnswered ? 'secondary' : 'outline'}
                  size="icon"
                  onClick={() => onJumpToQuestion?.(questionNum)}
                  className={cn(
                    'h-9 w-9 rounded-full text-xs font-semibold transition-all',
                    isCurrent && 'ring-2 ring-primary ring-offset-2 dark:ring-offset-slate-900',
                    isAnswered && !isCurrent && 'opacity-75 hover:opacity-100'
                  )}
                  title={`Jump to Question ${questionNum}${isAnswered ? ' (answered)' : ''}`}
                >
                  {questionNum}
                </Button>
              </motion.div>
            );
          })}
        </motion.div>
      )}

      {/* For large exams (>50 questions), show simplified indicator */}
      {showQuestionDots && total > 50 && (
        <div className="text-xs text-slate-500 dark:text-slate-400">
          {total} questions total
        </div>
      )}
    </div>
  );
};