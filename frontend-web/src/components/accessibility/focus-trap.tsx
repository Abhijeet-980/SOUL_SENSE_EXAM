'use client';

import React, { useEffect, useRef, useCallback } from 'react';
import { cn } from '@/lib/utils';

interface FocusTrapProps {
  children: React.ReactNode;
  enabled?: boolean;
  className?: string;
}

export function FocusTrap({ children, enabled = true, className = '' }: FocusTrapProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  const getFocusableElements = useCallback(() => {
    if (!containerRef.current) return [];

    const focusableSelectors = [
      'a[href]',
      'button:not([disabled])',
      'textarea:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      '[tabindex]:not([tabindex="-1"])',
      '[contenteditable="true"]',
    ].join(', ');

    return Array.from(containerRef.current.querySelectorAll<HTMLElement>(focusableSelectors));
  }, []);

  const handleKeyDown = useCallback(
    (event: KeyboardEvent) => {
      if (!enabled || event.key !== 'Tab') return;

      const focusableElements = getFocusableElements();
      if (focusableElements.length === 0) return;

      const firstElement = focusableElements[0];
      const lastElement = focusableElements[focusableElements.length - 1];

      if (event.shiftKey) {
        if (document.activeElement === firstElement) {
          event.preventDefault();
          lastElement.focus();
        }
      } else {
        if (document.activeElement === lastElement) {
          event.preventDefault();
          firstElement.focus();
        }
      }
    },
    [enabled, getFocusableElements]
  );

  useEffect(() => {
    if (!enabled) return;

    const container = containerRef.current;
    if (!container) return;

    container.addEventListener('keydown', handleKeyDown);

    const focusableElements = getFocusableElements();
    if (focusableElements.length > 0) {
      focusableElements[0].focus();
    }

    return () => {
      container.removeEventListener('keydown', handleKeyDown);
    };
  }, [enabled, getFocusableElements, handleKeyDown]);

  return (
    <div ref={containerRef} className={className}>
      {children}
    </div>
  );
}
