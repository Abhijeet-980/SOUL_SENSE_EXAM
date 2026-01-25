'use client';

import React from 'react';
import { useController, Control, FieldValues, Path } from 'react-hook-form';
import { FormLabel } from './FormLabel';
import { FormError } from './FormError';
import { FormMessage } from './FormMessage';

interface FormFieldProps<T extends FieldValues> {
  control: Control<T>;
  name: Path<T>;
  label?: string;
  placeholder?: string;
  type?: string;
  required?: boolean;
  disabled?: boolean;
  className?: string;
  children?: (field: any) => React.ReactNode;
}

export function FormField<T extends FieldValues>({
  control,
  name,
  label,
  placeholder,
  type = 'text',
  required = false,
  disabled = false,
  className = '',
  children,
}: FormFieldProps<T>) {
  const {
    field,
    fieldState: { error },
  } = useController({
    name,
    control,
  });

  const fieldProps = {
    ...field,
    placeholder,
    type,
    required,
    disabled,
    className: `form-field ${className}`,
  };

  return (
    <div className="form-field-container">
      {label && <FormLabel htmlFor={name} required={required}>{label}</FormLabel>}
      {children ? (
        children(fieldProps)
      ) : (
        <input {...fieldProps} />
      )}
      <FormError error={error?.message} />
      <FormMessage name={name} />
    </div>
  );
}