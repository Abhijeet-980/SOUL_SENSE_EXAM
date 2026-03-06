'use client';

import React from 'react';
import { cn } from '@/lib/utils';

interface SkipLinkProps {
  className?: string;
}

export function SkipLinks({ className = '' }: SkipLinkProps) {
  const handleSkip = (targetId: string) => (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    const target = document.getElementById(targetId);
    if (target) {
      target.focus();
      target.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <div
      className={cn(
        'fixed top-0 left-0 z-[100] flex flex-col gap-2 p-2',
        'sr-only focus-within:not-sr-only focus-within:absolute',
        className
      )}
    >
      <a
        href="#main-content"
        onClick={handleSkip('main-content')}
        className="inline-flex items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-md transition-colors hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
      >
        Skip to main content
      </a>
      <a
        href="#navigation"
        onClick={handleSkip('navigation')}
        className="inline-flex items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-md transition-colors hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
      >
        Skip to navigation
      </a>
    </div>
  );
}
