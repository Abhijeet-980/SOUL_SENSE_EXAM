'use client';

import React from 'react';
import { cn } from '@/lib/utils';

interface FormMessageProps {
  name: string;
  message?: string;
  className?: string;
  type?: 'hint' | 'description' | 'help';
}

export function FormMessage({ name, message, className = '', type = 'description' }: FormMessageProps) {
  if (!message) return null;

  return (
    <div
      className={cn('text-sm text-muted-foreground', className)}
      id={`${name}-description`}
      role={type === 'help' ? 'note' : undefined}
    >
      {message}
    </div>
  );
}
