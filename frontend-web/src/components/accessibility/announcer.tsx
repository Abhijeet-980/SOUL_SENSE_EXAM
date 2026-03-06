'use client';

import React from 'react';

interface AnnouncerProps {
  message: string;
  timeout?: number;
}

let announcementTimeout: NodeJS.Timeout | null = null;

export function Announcer({ message, timeout = 1000 }: AnnouncerProps) {
  const [announcement, setAnnouncement] = React.useState('');

  React.useEffect(() => {
    if (message) {
      if (announcementTimeout) {
        clearTimeout(announcementTimeout);
      }

      setAnnouncement(message);

      announcementTimeout = setTimeout(() => {
        setAnnouncement('');
      }, timeout);

      return () => {
        if (announcementTimeout) {
          clearTimeout(announcementTimeout);
        }
      };
    }
  }, [message, timeout]);

  return (
    <div
      role="status"
      aria-live="polite"
      aria-atomic="true"
      className="sr-only"
    >
      {announcement}
    </div>
  );
}
