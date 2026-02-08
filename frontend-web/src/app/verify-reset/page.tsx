'use client';

import React, { useState, useEffect } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { motion } from 'framer-motion';
import { ArrowLeft, Loader2, Lock, ShieldCheck } from 'lucide-react';
import { Form, FormField } from '@/components/forms';
import { Button, Input } from '@/components/ui';
import { AuthLayout, PasswordStrengthIndicator } from '@/components/auth';
import { resetPasswordSchema } from '@/lib/validation';
import { authApi } from '@/lib/api/auth';
import { z } from 'zod';
import Link from 'next/link';
import { UseFormReturn } from 'react-hook-form';
import { useRateLimiter } from '@/hooks/useRateLimiter';

type ResetPasswordFormData = z.infer<typeof resetPasswordSchema>;

export default function VerifyResetPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const emailParam = searchParams.get('email');

  const [isLoading, setIsLoading] = useState(false);
  const [isSuccess, setIsSuccess] = useState(false);

  const { lockoutTime, isLocked, handleRateLimitError } = useRateLimiter();

  // Redirect if no email
  useEffect(() => {
    if (!emailParam) {
      router.push('/forgot-password');
    }
  }, [emailParam, router]);

  const handleSubmit = async (
    data: ResetPasswordFormData,
    methods: UseFormReturn<ResetPasswordFormData>
  ) => {
    if (isLocked) return;

    setIsLoading(true);
    try {
      await authApi.completePasswordReset({
        email: data.email,
        otp_code: data.otp,
        new_password: data.password,
      });
      setIsSuccess(true);
      setTimeout(() => {
        router.push('/login');
      }, 3000);
    } catch (error: any) {
      console.error('Reset error:', error);

      if (handleRateLimitError(error, (msg) => methods.setError('root', { message: msg }))) {
        return;
      }

      const msg = error?.message || 'Verification failed';
      methods.setError('root', { message: msg });
    } finally {
      setIsLoading(false);
    }
  };

  const effectiveLoading = isLoading || isLocked;

  if (isSuccess) {
    return (
      <AuthLayout title="Password Reset!" subtitle="Your password has been successfully updated.">
        <div className="text-center space-y-6">
          <div className="mx-auto w-16 h-16 rounded-full bg-success/10 flex items-center justify-center">
            <ShieldCheck className="h-8 w-8 text-success" />
          </div>
          <p className="text-muted-foreground">
            You will be redirected to the login page shortly...
          </p>
          <Link href="/login">
            <Button className="w-full">Go to Login</Button>
          </Link>
        </div>
      </AuthLayout>
    );
  }

  if (!emailParam) return null;

  return (
    <AuthLayout
      title="Secure your account"
      subtitle={`Enter the code sent to ${emailParam} and choose a new password`}
    >
      <Form
        schema={resetPasswordSchema}
        onSubmit={handleSubmit}
        defaultValues={{ email: emailParam }}
        className="space-y-4"
      >
        {(methods) => (
          <>
            <input type="hidden" {...methods.register('email')} />

            {methods.formState.errors.root && (
              <div className="bg-destructive/10 border border-destructive/20 text-destructive text-xs p-3 rounded-md flex items-center mb-4">
                <ShieldCheck className="h-4 w-4 mr-2 flex-shrink-0 text-destructive" />
                {methods.formState.errors.root.message}
              </div>
            )}

            <FormField
              control={methods.control}
              name="otp"
              label="Verification Code"
              placeholder="6-digit code"
              maxLength={6}
              disabled={effectiveLoading}
            />

            <FormField control={methods.control} name="password" label="New Password" required>
              {(fieldProps) => (
                <div className="relative space-y-2">
                  <Input
                    {...fieldProps}
                    type="password"
                    placeholder="Enter new password"
                    autoComplete="new-password"
                    disabled={effectiveLoading}
                  />
                  <PasswordStrengthIndicator password={fieldProps.value || ''} />
                </div>
              )}
            </FormField>

            <FormField
              control={methods.control}
              name="confirmPassword"
              label="Confirm Password"
              placeholder="Confirm new password"
              type="password"
              disabled={effectiveLoading}
            />

            <div className="pt-2">
              <Button
                type="submit"
                disabled={effectiveLoading}
                className="w-full h-11 bg-gradient-to-r from-primary to-secondary"
              >
                {isLoading ? (
                  lockoutTime > 0 ? (
                    `Retry in ${lockoutTime}s`
                  ) : (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Verifying...
                    </>
                  )
                ) : (
                  <>
                    <Lock className="mr-2 h-4 w-4" />
                    Reset Password
                  </>
                )}
              </Button>
            </div>

            <Link href="/forgot-password">
              <Button variant="ghost" className="w-full mt-2" type="button">
                <ArrowLeft className="mr-2 h-4 w-4" />
                Change Email
              </Button>
            </Link>
          </>
        )}
      </Form>
    </AuthLayout>
  );
}
