'use client';

import React, { useEffect, useRef } from 'react';
import { cn } from '@/lib/utils';

interface LiveRegionProps {
  message: string;
  role?: 'status' | 'alert';
  politeness?: 'polite' | 'assertive';
  ariaLive?: 'polite' | 'assertive' | 'off';
  className?: string;
}

export function LiveRegion({
  message,
  role = 'status',
  politeness = 'polite',
  ariaLive = politeness,
  className = '',
}: LiveRegionProps) {
  const liveRegionRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (liveRegionRef.current && message) {
      liveRegionRef.current.textContent = message;
    }
  }, [message]);

  return (
    <div
      ref={liveRegionRef}
      role={role}
      aria-live={ariaLive}
      aria-atomic="true"
      className={cn('sr-only', className)}
    />
  );
}

interface AnnouncerProps {
  message: string;
  timeout?: number;
}

export function Announcer({ message, timeout = 1000 }: AnnouncerProps) {
  const [announcement, setAnnouncement] = React.useState('');

  React.useEffect(() => {
    if (message) {
      setAnnouncement(message);
      const timer = setTimeout(() => {
        setAnnouncement('');
      }, timeout);
      return () => clearTimeout(timer);
    }
  }, [message, timeout]);

  return <LiveRegion message={announcement} role="status" politeness="polite" />;
}
