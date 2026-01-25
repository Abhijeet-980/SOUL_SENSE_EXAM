'use client';

import { Form, FormField } from '@/components/forms';
import { registrationSchema, RegistrationFormData } from '@/lib/validation';

export default function Home() {
  const onSubmit = async (data: RegistrationFormData) => {
    console.log('Form submitted:', data);
    // Handle form submission
  };

  return (
    <main style={{ padding: '2rem' }}>
      <h1>SOUL SENSE - Form Validation Demo</h1>
      <Form schema={registrationSchema} onSubmit={onSubmit}>
        {(methods) => (
          <>
            <FormField
              control={methods.control}
              name="username"
              label="Username"
              required
            />
            <FormField
              control={methods.control}
              name="email"
              label="Email"
              type="email"
              required
            />
            <FormField
              control={methods.control}
              name="password"
              label="Password"
              type="password"
              required
            />
            <FormField
              control={methods.control}
              name="confirmPassword"
              label="Confirm Password"
              type="password"
              required
            />
            <FormField
              control={methods.control}
              name="firstName"
              label="First Name"
              required
            />
            <FormField
              control={methods.control}
              name="lastName"
              label="Last Name"
            />
            <FormField
              control={methods.control}
              name="acceptTerms"
              label="Accept Terms"
              type="checkbox"
              required
            >
              {(fieldProps) => <input {...fieldProps} type="checkbox" />}
            </FormField>
            <button type="submit" disabled={methods.formState.isSubmitting}>
              {methods.formState.isSubmitting ? 'Submitting...' : 'Register'}
            </button>
          </>
        )}
      </Form>
    </main>
  );
}