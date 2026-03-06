'use client';

import { useEffect, useRef, useCallback } from 'react';
import { useExamStore } from '@/stores/examStore';
import { examsApi } from '@/lib/api/exams';

export function useAutoSaveExam() {
  const { answers, currentExamId, startTime } = useExamStore();
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);
  const lastSavedAnswersRef = useRef<Record<number, number>>({});

  const saveDraft = useCallback(async (answersToSave: Record<number, number>) => {
    if (!currentExamId || Object.keys(answersToSave).length === 0) {
      return;
    }

    try {
      // Calculate current duration
      const durationSeconds = startTime
        ? Math.floor((Date.now() - new Date(startTime).getTime()) / 1000)
        : 0;

      const draftData = {
        answers: answersToSave,
        current_question_index: 0, // Could be enhanced to track current question
        duration_seconds: durationSeconds,
      };

      await examsApi.saveDraft(currentExamId, draftData);
      lastSavedAnswersRef.current = { ...answersToSave };

      // Optional: Could add success logging or telemetry here
      console.debug('Draft saved successfully', { examId: currentExamId, answerCount: Object.keys(answersToSave).length });
    } catch (error) {
      // Silently fail to not disrupt UX - could add error logging/telemetry
      console.warn('Failed to save draft:', error);
    }
  }, [currentExamId, startTime]);

  const debouncedSave = useCallback((answersToSave: Record<number, number>) => {
    // Clear existing timeout
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    // Only save if answers have actually changed
    const hasChanges = JSON.stringify(answersToSave) !== JSON.stringify(lastSavedAnswersRef.current);
    if (!hasChanges) {
      return;
    }

    // Set new timeout for 5 seconds
    timeoutRef.current = setTimeout(() => {
      saveDraft(answersToSave);
    }, 5000);
  }, [saveDraft]);

  // Cancel any pending save (used when submitting exam)
  const cancelAutoSave = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
  }, []);

  useEffect(() => {
    debouncedSave(answers);
  }, [answers, debouncedSave]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  return { cancelAutoSave };
}