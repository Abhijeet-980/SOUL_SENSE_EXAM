'use client';

import React from 'react';

interface FormErrorProps {
  error?: string;
  className?: string;
}

export function FormError({ error, className = '' }: FormErrorProps) {
  if (!error) return null;

  return (
    <div className={`form-error ${className}`} role="alert" aria-live="polite">
      {error}
    </div>
  );
}