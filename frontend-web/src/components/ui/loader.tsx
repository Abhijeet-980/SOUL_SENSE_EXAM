'use client';

import React from 'react';
import { motion } from 'framer-motion';
import { Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';

interface LoaderProps {
  className?: string;
  size?: 'sm' | 'md' | 'lg' | 'xl';
  text?: string;
  fullScreen?: boolean;
}

const sizeMap = {
  sm: 'w-4 h-4',
  md: 'w-8 h-8',
  lg: 'w-12 h-12',
  xl: 'w-16 h-16',
};

export function Loader({ className, size = 'md', text, fullScreen = false }: LoaderProps) {
  const content = (
    <div className={cn('flex flex-col items-center justify-center gap-4', className)}>
      <div className="animate-spin">
        <Loader2 className={cn('text-sky-500', sizeMap[size])} />
      </div>
      {(text || fullScreen) && (
        <p className="text-sm font-medium text-muted-foreground animate-pulse">
          {text || 'Initializing...'}
        </p>
      )}
    </div>
  );

  if (fullScreen) {
    return (
      <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-background/80 backdrop-blur-sm">
        {content}
      </div>
    );
  }

  return content;
}
