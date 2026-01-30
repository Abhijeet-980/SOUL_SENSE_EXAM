'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import { motion } from 'framer-motion';
import { Eye, EyeOff, Loader2 } from 'lucide-react';
import { Form, FormField } from '@/components/forms';
import { Button, Input } from '@/components/ui';
import { AuthLayout, SocialLogin, PasswordStrengthIndicator } from '@/components/auth';
import { registrationSchema } from '@/lib/validation';
import { z } from 'zod';

type RegisterFormData = z.infer<typeof registrationSchema>;

export default function RegisterPage() {
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [passwordValue, setPasswordValue] = useState('');

  const handleSubmit = async (data: RegisterFormData) => {
    setIsLoading(true);
    // Simulate API call
    await new Promise((resolve) => setTimeout(resolve, 1500));
    console.log('Register data:', data);
    setIsLoading(false);
    // TODO: Implement actual registration logic
  };

  return (
    <AuthLayout
      title="Create an account"
      subtitle="Start your emotional intelligence journey today"
    >
      <Form schema={registrationSchema} onSubmit={handleSubmit} className="space-y-4">
        {(methods) => (
          <>
            <div className="grid grid-cols-2 gap-4">
              <motion.div
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.2 }}
              >
                <FormField
                  control={methods.control}
                  name="firstName"
                  label="First name"
                  placeholder="John"
                  required
                />
              </motion.div>

              <motion.div
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.2 }}
              >
                <FormField
                  control={methods.control}
                  name="lastName"
                  label="Last name"
                  placeholder="Doe"
                />
              </motion.div>
            </div>

            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.25 }}
            >
              <FormField
                control={methods.control}
                name="username"
                label="Username"
                placeholder="johndoe"
                required
              />
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.3 }}
            >
              <FormField
                control={methods.control}
                name="email"
                label="Email"
                placeholder="you@example.com"
                type="email"
                required
              />
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.35 }}
            >
              <FormField
                control={methods.control}
                name="password"
                label="Password"
                required
              >
                {(fieldProps) => (
                  <div>
                    <div className="relative">
                      <Input
                        {...fieldProps}
                        type={showPassword ? 'text' : 'password'}
                        placeholder="Create a strong password"
                        className="pr-10"
                        onChange={(e) => {
                          fieldProps.onChange(e);
                          setPasswordValue(e.target.value);
                        }}
                      />
                      <button
                        type="button"
                        onClick={() => setShowPassword(!showPassword)}
                        className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                        tabIndex={-1}
                      >
                        {showPassword ? (
                          <EyeOff className="h-4 w-4" />
                        ) : (
                          <Eye className="h-4 w-4" />
                        )}
                      </button>
                    </div>
                    <PasswordStrengthIndicator password={passwordValue} />
                  </div>
                )}
              </FormField>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.4 }}
            >
              <FormField
                control={methods.control}
                name="confirmPassword"
                label="Confirm password"
                required
              >
                {(fieldProps) => (
                  <div className="relative">
                    <Input
                      {...fieldProps}
                      type={showConfirmPassword ? 'text' : 'password'}
                      placeholder="Confirm your password"
                      className="pr-10"
                    />
                    <button
                      type="button"
                      onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                      tabIndex={-1}
                    >
                      {showConfirmPassword ? (
                        <EyeOff className="h-4 w-4" />
                      ) : (
                        <Eye className="h-4 w-4" />
                      )}
                    </button>
                  </div>
                )}
              </FormField>
            </motion.div>

            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.45 }}
              className="flex items-start gap-2"
            >
              <input
                type="checkbox"
                id="acceptTerms"
                {...methods.register('acceptTerms')}
                className="mt-1 h-4 w-4 rounded border-input text-primary focus:ring-primary"
              />
              <label htmlFor="acceptTerms" className="text-sm text-muted-foreground">
                I agree to the{' '}
                <Link href="/terms" className="text-primary hover:underline">
                  Terms of Service
                </Link>{' '}
                and{' '}
                <Link href="/privacy" className="text-primary hover:underline">
                  Privacy Policy
                </Link>
              </label>
            </motion.div>
            {methods.formState.errors.acceptTerms && (
              <p className="text-sm text-destructive">
                {methods.formState.errors.acceptTerms.message}
              </p>
            )}

            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.5 }}
            >
              <Button
                type="submit"
                disabled={isLoading}
                className="w-full h-11 bg-gradient-to-r from-primary to-secondary hover:opacity-90 transition-opacity"
              >
                {isLoading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Creating account...
                  </>
                ) : (
                  'Create account'
                )}
              </Button>
            </motion.div>

            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.55 }}
            >
              <SocialLogin isLoading={isLoading} />
            </motion.div>

            <motion.p
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.6 }}
              className="text-center text-sm text-muted-foreground"
            >
              Already have an account?{' '}
              <Link
                href="/login"
                className="text-primary hover:text-primary/80 font-medium transition-colors"
              >
                Sign in
              </Link>
            </motion.p>
          </>
        )}
      </Form>
    </AuthLayout>
  );
}
