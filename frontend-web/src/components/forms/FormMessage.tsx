'use client';

import React from 'react';

interface FormMessageProps {
  name: string;
  message?: string;
  className?: string;
}

export function FormMessage({ name, message, className = '' }: FormMessageProps) {
  if (!message) return null;

  return (
    <div className={`form-message ${className}`} id={`${name}-message`}>
      {message}
    </div>
  );
}