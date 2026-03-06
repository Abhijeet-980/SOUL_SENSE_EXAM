'use client';

import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { motion } from 'framer-motion';
import { CheckCircle2, Clock, HelpCircle, ArrowRight } from 'lucide-react';
import { useExamStore } from '@/stores/examStore';
import { Button, Card, CardContent } from '@/components/ui';

// Confetti particle component
const Particle = ({ delay }: { delay: number }) => {
  const randomX = Math.random() * 100 - 50;
  const randomRotation = Math.random() * 360;
  const randomDuration = Math.random() * 2 + 2.5;

  return (
    <motion.div
      className="pointer-events-none absolute h-2 w-2 rounded-full bg-gradient-to-br from-yellow-400 to-orange-400"
      initial={{
        x: 0,
        y: 0,
        opacity: 1,
        rotate: 0,
      }}
      animate={{
        x: randomX * 4,
        y: 100 + Math.random() * 50,
        opacity: 0,
        rotate: randomRotation,
      }}
      transition={{
        duration: randomDuration,
        delay,
        ease: 'easeOut',
      }}
    />
  );
};

export default function ExamCompletePage() {
  const router = useRouter();
  const [displaySummary, setDisplaySummary] = useState({
    timeTaken: 0,
    answered: 0,
    total: 0,
  });

  // Get exam state
  const { questions, startTime, getAnsweredCount, resetExam } = useExamStore();

  // Capture summary and clear state on mount
  useEffect(() => {
    // Calculate summary before clearing
    const answered = getAnsweredCount();
    const total = questions.length;
    const timeTaken = startTime
      ? Math.floor((Date.now() - new Date(startTime).getTime()) / 1000)
      : 0;

    setDisplaySummary({
      timeTaken,
      answered,
      total,
    });

    // Clear exam state to prevent going back
    window.history.replaceState(null, '', '/exam/complete');

    // Prevent back navigation
    const handlePopState = (e: PopStateEvent) => {
      e.preventDefault();
      window.history.replaceState(null, '', '/exam/complete');
    };

    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, [getAnsweredCount, questions.length, startTime]);

  // Handle navigation
  const handleViewResults = () => {
    // Reset exam state only when leaving
    resetExam();
    router.push('/results');
  };

  const handleReturnDashboard = () => {
    // Reset exam state only when leaving
    resetExam();
    router.push('/dashboard');
  };

  // Format time display
  const formatTime = (seconds: number): string => {
    const minutes = Math.floor(seconds / 60);
    const secs = seconds % 60;
    if (minutes === 0) return `${secs}s`;
    return `${minutes}m ${secs}s`;
  };

  // Confetti particles
  const confettiParticles = Array.from({ length: 24 }).map((_, i) => (
    <Particle key={i} delay={i * 0.05} />
  ));

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50 dark:from-slate-950 dark:to-slate-900 flex items-center justify-center px-4 py-8">
      {/* Confetti animation */}
      <div className="pointer-events-none fixed inset-0 top-0">
        <div className="relative h-screen w-full">{confettiParticles}</div>
      </div>

      {/* Main content container */}
      <motion.div
        className="w-full max-w-md"
        initial={{ opacity: 0, scale: 0.9, y: 20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        transition={{
          duration: 0.6,
          ease: [0.34, 1.56, 0.64, 1],
        }}
      >
        <div className="text-center">
          {/* Success Animation - Bouncing Checkmark */}
          <motion.div
            className="mb-8 flex justify-center"
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            transition={{
              delay: 0.2,
              duration: 0.6,
              type: 'spring',
              stiffness: 100,
              damping: 10,
            }}
          >
            <motion.div
              animate={{
                y: [0, -8, 0],
              }}
              transition={{
                duration: 2,
                repeat: Infinity,
                ease: 'easeInOut',
              }}
            >
              <CheckCircle2 className="h-24 w-24 text-green-500" strokeWidth={1.5} />
            </motion.div>
          </motion.div>

          {/* Title */}
          <motion.h1
            className="mb-3 text-4xl font-bold tracking-tight text-slate-900 dark:text-white"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4, duration: 0.5 }}
          >
            Assessment Complete!
          </motion.h1>

          {/* Message */}
          <motion.p
            className="mb-8 text-lg text-slate-600 dark:text-slate-400"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.5, duration: 0.5 }}
          >
            Your results are ready
          </motion.p>

          {/* Summary Card */}
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.6, duration: 0.5 }}
            className="mb-8"
          >
            <Card className="border-slate-200 bg-white/80 backdrop-blur-sm dark:border-slate-700 dark:bg-slate-800/50">
              <CardContent className="grid grid-cols-2 gap-4 pt-6">
                {/* Questions Answered */}
                <div className="text-center">
                  <motion.div
                    className="mb-2 flex justify-center"
                    animate={{ rotate: 360 }}
                    transition={{
                      duration: 3,
                      repeat: Infinity,
                      ease: 'linear',
                    }}
                  >
                    <HelpCircle className="h-6 w-6 text-blue-500" />
                  </motion.div>
                  <p className="text-sm text-slate-600 dark:text-slate-400">Questions</p>
                  <p className="text-2xl font-bold text-slate-900 dark:text-white">
                    {displaySummary.answered}/{displaySummary.total}
                  </p>
                </div>

                {/* Time Taken */}
                <div className="text-center">
                  <motion.div
                    className="mb-2 flex justify-center"
                    animate={{ scale: [1, 1.1, 1] }}
                    transition={{
                      duration: 2,
                      repeat: Infinity,
                      ease: 'easeInOut',
                    }}
                  >
                    <Clock className="h-6 w-6 text-green-500" />
                  </motion.div>
                  <p className="text-sm text-slate-600 dark:text-slate-400">Time</p>
                  <p className="text-2xl font-bold text-slate-900 dark:text-white">
                    {formatTime(displaySummary.timeTaken)}
                  </p>
                </div>
              </CardContent>
            </Card>
          </motion.div>

          {/* Action Buttons */}
          <motion.div
            className="space-y-3"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.7, duration: 0.5 }}
          >
            {/* View Results Button - Primary */}
            <Button
              onClick={handleViewResults}
              size="lg"
              className="w-full bg-gradient-to-r from-blue-600 to-blue-700 text-white hover:from-blue-700 hover:to-blue-800 shadow-lg transition-all hover:shadow-xl"
            >
              View Results
              <ArrowRight className="ml-2 h-4 w-4" />
            </Button>

            {/* Return to Dashboard Button - Secondary */}
            <Button
              onClick={handleReturnDashboard}
              variant="outline"
              size="lg"
              className="w-full border-slate-300 hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-800"
            >
              Return to Dashboard
            </Button>
          </motion.div>
        </div>
      </motion.div>
    </div>
  );
}
