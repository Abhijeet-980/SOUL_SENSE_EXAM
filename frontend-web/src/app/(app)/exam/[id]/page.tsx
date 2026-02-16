'use client';

import React, { useEffect, useState } from 'react';
import { useRouter, useParams } from 'next/navigation';
import { motion, AnimatePresence } from 'framer-motion';
import { AlertTriangle, RefreshCw } from 'lucide-react';

import { useQuestions } from '@/hooks/useQuestions';
import { useExamStore } from '@/stores/examStore';
import { useExamSubmit } from '@/hooks/useExamSubmit';
import { ExamTimer, ExamProgress, ExamNavigation, QuestionCard } from '@/components/exam';
import { Button, Skeleton, Card, CardContent, CardHeader, CardTitle } from '@/components/ui';

export default function ExamPage() {
  const router = useRouter();
  const params = useParams();
  const examId = params.id as string;

  const [showLeaveWarning, setShowLeaveWarning] = useState(false);
  const [isLeaving, setIsLeaving] = useState(false);

  // Exam state
  const {
    questions,
    currentIndex,
    answers,
    startTime,
    isCompleted,
    setQuestions,
    setAnswer,
    getCurrentQuestion,
    getAnsweredCount,
    completeExam,
    resetExam,
  } = useExamStore();

  // API hooks
  const {
    questions: apiQuestions,
    isLoading,
    error,
    refetch,
  } = useQuestions({
    count: 20, // Default count, could be configurable based on exam type
    enabled: !isCompleted,
  });

  const { submitExam, isSubmitting, error: submitError, result } = useExamSubmit();

  // Load questions on mount
  useEffect(() => {
    if (apiQuestions.length > 0 && questions.length === 0) {
      setQuestions(apiQuestions);
    }
  }, [apiQuestions, questions.length, setQuestions]);

  // Handle exam completion
  useEffect(() => {
    if (result) {
      completeExam();
      // Redirect to results page
      router.push('/results');
    }
  }, [result, completeExam, router]);

  // Handle beforeunload to warn user
  useEffect(() => {
    const handleBeforeUnload = (e: BeforeUnloadEvent) => {
      if (getAnsweredCount() > 0 && !isCompleted) {
        e.preventDefault();
        e.returnValue = '';
        return '';
      }
    };

    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => window.removeEventListener('beforeunload', handleBeforeUnload);
  }, [getAnsweredCount, isCompleted]);

  // Handle leaving page confirmation
  const handleLeaveAttempt = () => {
    if (getAnsweredCount() > 0 && !isCompleted) {
      setShowLeaveWarning(true);
    } else {
      handleLeave();
    }
  };

  const handleLeave = () => {
    setIsLeaving(true);
    resetExam();
    router.push('/exam');
  };

  const handleStay = () => {
    setShowLeaveWarning(false);
  };

  // Handle answer selection
  const handleAnswerSelect = (value: number) => {
    const currentQuestion = getCurrentQuestion();
    if (currentQuestion) {
      setAnswer(currentQuestion.id, value);
    }
  };

  // Handle exam submission
  const handleSubmit = async () => {
    const currentQuestion = getCurrentQuestion();
    if (!currentQuestion) return;

    // Make sure current answer is saved
    const currentAnswer = answers[currentQuestion.id];
    if (currentAnswer !== undefined) {
      setAnswer(currentQuestion.id, currentAnswer);
    }

    // Calculate duration
    const durationSeconds = startTime
      ? Math.floor((Date.now() - new Date(startTime).getTime()) / 1000)
      : 0;

    // Prepare submission data
    const submissionData = {
      answers: Object.entries(answers).map(([questionId, value]) => ({
        question_id: parseInt(questionId),
        value,
      })),
      duration_seconds: durationSeconds,
    };

    await submitExam(submissionData);
  };

  // Handle timer expiration
  const handleTimeUp = () => {
    handleSubmit();
  };

  // Loading state
  if (isLoading) {
    return (
      <div className="container mx-auto px-4 py-8 max-w-4xl">
        <div className="space-y-6">
          <div>
            <Skeleton className="h-8 w-64" />
            <Skeleton className="h-4 w-96 mt-2" />
          </div>

          <Card>
            <CardHeader>
              <Skeleton className="h-6 w-32" />
            </CardHeader>
            <CardContent className="space-y-4">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-3/4" />
              <div className="grid grid-cols-5 gap-2">
                {Array.from({ length: 5 }).map((_, i) => (
                  <Skeleton key={i} className="h-10 w-full" />
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="container mx-auto px-4 py-8 max-w-4xl">
        <Card className="text-center">
          <CardContent className="pt-6">
            <AlertTriangle className="h-12 w-12 text-destructive mx-auto mb-4" />
            <h2 className="text-xl font-semibold mb-2">Failed to Load Questions</h2>
            <p className="text-muted-foreground mb-4">{error}</p>
            <div className="flex gap-2 justify-center">
              <Button onClick={refetch} variant="outline">
                <RefreshCw className="h-4 w-4 mr-2" />
                Try Again
              </Button>
              <Button onClick={handleLeaveAttempt} variant="ghost">
                Go Back
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  const currentQuestion = getCurrentQuestion();

  if (!currentQuestion) {
    return (
      <div className="container mx-auto px-4 py-8 max-w-4xl">
        <Card className="text-center">
          <CardContent className="pt-6">
            <h2 className="text-xl font-semibold mb-2">No Questions Available</h2>
            <p className="text-muted-foreground mb-4">Unable to load questions for this exam.</p>
            <Button onClick={handleLeaveAttempt}>Return to Exam Selection</Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <>
      <div className="container mx-auto px-4 py-8 max-w-4xl">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Exam in Progress</h1>
            <p className="text-muted-foreground mt-1">
              Answer each question carefully. You can navigate between questions.
            </p>
          </div>

          <div className="flex items-center gap-4">
            <ExamTimer
              durationMinutes={60} // Could be configurable based on exam type
              onTimeUp={handleTimeUp}
              isPaused={false}
            />
            <Button variant="ghost" onClick={handleLeaveAttempt}>
              Exit Exam
            </Button>
          </div>
        </div>

        {/* Progress */}
        <div className="mb-6">
          <ExamProgress />
        </div>

        {/* Question Card */}
        <AnimatePresence mode="wait">
          <motion.div
            key={currentIndex}
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            transition={{ duration: 0.3 }}
          >
            <QuestionCard
              question={currentQuestion}
              selectedValue={answers[currentQuestion.id]}
              onSelect={handleAnswerSelect}
              totalQuestions={questions.length}
              currentIndex={currentIndex + 1}
              disabled={isSubmitting}
            />
          </motion.div>
        </AnimatePresence>

        {/* Navigation */}
        <div className="mt-8">
          <ExamNavigation onSubmit={handleSubmit} isSubmitting={isSubmitting} />
        </div>

        {/* Submit Error */}
        {submitError && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="mt-4 p-4 bg-destructive/10 border border-destructive/20 rounded-lg"
          >
            <p className="text-destructive text-sm">{submitError}</p>
          </motion.div>
        )}
      </div>

      {/* Leave Warning Modal */}
      <AnimatePresence>
        {showLeaveWarning && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50"
          >
            <motion.div
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              className="bg-background rounded-lg p-6 max-w-md w-full"
            >
              <h3 className="text-lg font-semibold mb-2">Leave Exam?</h3>
              <p className="text-muted-foreground mb-4">
                You have answered {getAnsweredCount()} questions. Your progress will be lost if you
                leave now.
              </p>
              <div className="flex gap-2 justify-end">
                <Button variant="outline" onClick={handleStay}>
                  Stay
                </Button>
                <Button variant="destructive" onClick={handleLeave} disabled={isLeaving}>
                  {isLeaving ? 'Leaving...' : 'Leave Exam'}
                </Button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
