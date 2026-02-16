'use client';

import React from 'react';
import { Progress } from '@/components/ui';
import { useExamStore } from '@/stores/examStore';
import { cn } from '@/lib/utils';

interface ExamProgressProps {
  className?: string;
}

export const ExamProgress: React.FC<ExamProgressProps> = ({ className }) => {
  const { getProgressPercentage, getAnsweredCount, questions } = useExamStore();

  const progress = getProgressPercentage();
  const answeredCount = getAnsweredCount();
  const totalQuestions = questions.length;

  return (
    <div className={cn('space-y-2', className)}>
      <div className="flex items-center justify-between text-sm">
        <span className="text-muted-foreground">
          Question {answeredCount} of {totalQuestions}
        </span>
        <span className="font-medium">{Math.round(progress)}% Complete</span>
      </div>
      <Progress value={progress} showLabel={false} size="sm" color="primary" className="w-full" />
    </div>
  );
};
