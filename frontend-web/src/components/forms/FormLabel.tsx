'use client';

import React from 'react';

interface FormLabelProps {
  htmlFor: string;
  children: React.ReactNode;
  required?: boolean;
  className?: string;
}

export function FormLabel({ htmlFor, children, required = false, className = '' }: FormLabelProps) {
  return (
    <label htmlFor={htmlFor} className={`form-label ${className}`}>
      {children}
      {required && <span className="required-indicator" aria-label="required">*</span>}
    </label>
  );
}