'use client';

import React, { useEffect } from 'react';
import { Button } from '@/components/ui';
import { ChevronLeft, ChevronRight, Send } from 'lucide-react';
import { useExamStore } from '@/stores/examStore';
import { cn } from '@/lib/utils';

interface ExamNavigationProps {
  onSubmit: () => void;
  onReview?: () => void; // New prop for review action
  isSubmitting?: boolean;
  className?: string;
  canGoNext?: boolean;
}

export const ExamNavigation: React.FC<ExamNavigationProps> = ({
  onSubmit,
  onReview,
  isSubmitting = false,
  className,
  canGoNext = true,
}) => {
  const { previousQuestion, nextQuestion, getIsFirstQuestion, getIsLastQuestion } = useExamStore();

  const isFirst = getIsFirstQuestion();
  const isLast = getIsLastQuestion();

  useEffect(() => {
    // Handle keyboard navigation for exam questions, but ignore when form elements are focused
    // to allow native browser behavior for radio buttons, inputs, etc. (fixes #875)
    const handleKeyDown = (e: KeyboardEvent) => {
      const activeElement = document.activeElement;
      const isInputFocused =
        activeElement &&
        (activeElement.tagName === "INPUT" ||
        activeElement.tagName === "TEXTAREA" ||
        activeElement.tagName === "SELECT" ||
        activeElement.getAttribute("role") === "radio");
      if (isInputFocused) {
        return; // Abort global navigation, let the browser handle standard UI traversing.
      }

      if (e.key === 'ArrowLeft' && !isFirst) {
        previousQuestion();
      }

      if (e.key === 'ArrowRight' && canGoNext && !isLast) {
        nextQuestion();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isFirst, canGoNext, isLast, previousQuestion, nextQuestion]);

  return (
    <div className={cn('flex items-center justify-between', className)}>
      <Button
        variant="outline"
        onClick={previousQuestion}
        disabled={isFirst || isSubmitting}
        className="flex items-center gap-2"
      >
        <ChevronLeft className="h-4 w-4" />
        Previous
      </Button>

      <div className="flex items-center gap-2">
        {!isLast ? (
          <Button
            onClick={nextQuestion}
            disabled={!canGoNext || isSubmitting}
            className="flex items-center gap-2"
          >
            Next
            <ChevronRight className="h-4 w-4" />
          </Button>
        ) : (
          <Button
            onClick={onReview || onSubmit}
            disabled={isSubmitting}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700"
          >
            {isSubmitting ? (
              <>
                <div className="h-4 w-4 animate-spin rounded-full border-2 border-white border-t-transparent" />
                {onReview ? 'Loading Review...' : 'Submitting...'}
              </>
            ) : (
              <>
                <Send className="h-4 w-4" />
                {onReview ? 'Review Answers' : 'Submit Exam'}
              </>
            )}
          </Button>
        )}
      </div>
    </div>
  );
};
