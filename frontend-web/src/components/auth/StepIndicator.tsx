'use client';

import { motion } from 'framer-motion';
import { Check, Circle } from 'lucide-react';
import { cn } from '@/lib/utils';

interface Step {
  id: string;
  label: string;
  description?: string;
}

interface StepIndicatorProps {
  steps: Step[];
  currentStep: number;
  className?: string;
}

export function StepIndicator({ steps, currentStep, className }: StepIndicatorProps) {
  return (
    <div className={cn('w-full', className)}>
      <div className="flex items-center justify-between">
        {steps.map((step, index) => {
          const isCompleted = index < currentStep;
          const isCurrent = index === currentStep;
          const isPending = index > currentStep;

          return (
            <div key={step.id} className="flex items-center flex-1 last:flex-none">
              {/* Step Circle */}
              <div className="flex flex-col items-center">
                <motion.div
                  initial={false}
                  animate={{
                    scale: isCurrent ? 1.1 : 1,
                    backgroundColor: isCompleted
                      ? '#10B981'
                      : isCurrent
                        ? '#3B82F6'
                        : '#E2E8F0',
                  }}
                  className={cn(
                    'w-10 h-10 rounded-full flex items-center justify-center border-2 transition-colors duration-300',
                    isCompleted && 'border-green-500 bg-green-500',
                    isCurrent && 'border-primary bg-primary shadow-lg shadow-primary/30',
                    isPending && 'border-gray-200 bg-white'
                  )}
                >
                  {isCompleted ? (
                    <Check className="w-5 h-5 text-white" />
                  ) : isCurrent ? (
                    <span className="text-white font-semibold text-sm">{index + 1}</span>
                  ) : (
                    <span className="text-gray-400 font-medium text-sm">{index + 1}</span>
                  )}
                </motion.div>

                {/* Step Label */}
                <div className="mt-2 text-center">
                  <p
                    className={cn(
                      'text-xs font-medium transition-colors duration-300 whitespace-nowrap',
                      isCompleted && 'text-green-600',
                      isCurrent && 'text-primary',
                      isPending && 'text-gray-400'
                    )}
                  >
                    {step.label}
                  </p>
                  {step.description && (
                    <p className="text-[10px] text-gray-400 mt-0.5 hidden sm:block">
                      {step.description}
                    </p>
                  )}
                </div>
              </div>

              {/* Connector Line */}
              {index < steps.length - 1 && (
                <div className="flex-1 h-0.5 mx-4 relative">
                  <div className="absolute inset-0 bg-gray-200 rounded-full" />
                  <motion.div
                    initial={{ scaleX: 0 }}
                    animate={{ scaleX: isCompleted ? 1 : 0 }}
                    transition={{ duration: 0.3, delay: 0.1 }}
                    className="absolute inset-0 bg-green-500 rounded-full origin-left"
                  />
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
