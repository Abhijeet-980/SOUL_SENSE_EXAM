import * as React from 'react';
import { motion } from 'framer-motion';
import { cn } from '@/lib/utils';

export interface ProgressProps extends React.HTMLAttributes<HTMLDivElement> {
  value: number; // 0-100
  showLabel?: boolean;
  size?: 'sm' | 'md' | 'lg';
  color?: 'primary' | 'success' | 'warning' | 'danger';
}

const Progress = React.forwardRef<HTMLDivElement, ProgressProps>(
  ({ className, value, showLabel = false, size = 'md', color = 'primary', ...props }, ref) => {
    // Clamp value between 0 and 100
    const clampedValue = Math.min(100, Math.max(0, value));

    const sizes = {
      sm: 'h-2',
      md: 'h-3',
      lg: 'h-4',
    };

    const colors = {
      primary: 'bg-primary',
      success: 'bg-success',
      warning: 'bg-warning',
      danger: 'bg-destructive',
    };

    const labelSizes = {
      sm: 'text-xs',
      md: 'text-sm',
      lg: 'text-base',
    };

    return (
      <div
        ref={ref}
        className={cn('w-full', className)}
        {...props}
      >
        <div className={cn(
          'relative w-full overflow-hidden rounded-full bg-secondary',
          sizes[size]
        )}>
          <motion.div
            className={cn(
              'h-full rounded-full',
              colors[color]
            )}
            initial={{ width: 0 }}
            animate={{ width: `${clampedValue}%` }}
            transition={{ duration: 0.5, ease: 'easeInOut' }}
          />
        </div>
        {showLabel && (
          <div className={cn(
            'mt-1 text-center font-medium text-muted-foreground',
            labelSizes[size]
          )}>
            {clampedValue}%
          </div>
        )}
      </div>
    );
  }
);

Progress.displayName = 'Progress';

export { Progress };