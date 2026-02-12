'use client';

import React, { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { motion, AnimatePresence } from 'framer-motion';
import { Eye, EyeOff, Loader2, AlertCircle, AlertTriangle } from 'lucide-react';
import { Form, FormField } from '@/components/forms';
import { Button, Input } from '@/components/ui';
import { AuthLayout, SocialLogin } from '@/components/auth';
import { loginSchema } from '@/lib/validation';
import { z } from 'zod';
import { UseFormReturn } from 'react-hook-form';
import { useAuth } from '@/hooks/useAuth';

type LoginFormData = z.infer<typeof loginSchema>;

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1';

export default function LoginPage() {
  const router = useRouter();
  const { login } = useAuth();

  // UI State
  const [showPassword, setShowPassword] = useState(false);
  const [isLoggingIn, setIsLoggingIn] = useState(false);
  const [sessionWarning, setSessionWarning] = useState<string | null>(null);
  const [showWarningModal, setShowWarningModal] = useState(false);
  const [pendingRedirectToken, setPendingRedirectToken] = useState<string | null>(null);

  // 2FA State
  const [show2FA, setShow2FA] = useState(false);
  const [preAuthToken, setPreAuthToken] = useState('');
  const [otpCode, setOtpCode] = useState('');
  const [twoFaError, setTwoFaError] = useState('');

  // Lockout State
  const [lockoutTime, setLockoutTime] = useState<number>(0);

  // Lockout Timer Effect
  useEffect(() => {
    if (lockoutTime <= 0) return;

    const timer = setInterval(() => {
      setLockoutTime((prev) => Math.max(0, prev - 1));
    }, 1000);

    return () => clearInterval(timer);
  }, [lockoutTime]);

  const handleLoginSubmit = async (
    data: LoginFormData,
    methods: UseFormReturn<LoginFormData>
  ) => {
    if (lockoutTime > 0) return;

    setIsLoggingIn(true);
    setSessionWarning(null);

    try {
      const formData = new URLSearchParams({
        username: data.identifier,
        password: data.password,
      });

      const response = await fetch(`${API_BASE_URL}/auth/login`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: formData.toString(),
      });

      const result = await response.json();

      // Handle 2FA requirement
      if (response.status === 202) {
        // Check for warnings in 2FA response
        if (result.warnings?.length > 0) {
          setSessionWarning(result.warnings[0].message);
        }
        setPreAuthToken(result.pre_auth_token);
        setShow2FA(true);
        return;
      }

      // Handle errors
      if (!response.ok) {
        handleLoginError(result, methods);
        return;
      }

      // Success - check for session warnings
      if (result.warnings?.length > 0) {
        // Show modal and block redirect
        setSessionWarning(result.warnings[0].message);
        setPendingRedirectToken(result.access_token);
        setShowWarningModal(true);
      } else {
        // No warnings, proceed directly
        localStorage.setItem('token', result.access_token);
        router.push('/dashboard');
      }
    } catch (error) {
      console.error('Login error:', error);
      methods.setError('root', {
        message: error instanceof Error ? error.message : 'Login failed. Please try again.',
      });
    } finally {
      setIsLoggingIn(false);
    }
  };

  const handleLoginError = (errorData: any, methods: UseFormReturn<LoginFormData>) => {
    const code = errorData.detail?.code;

    switch (code) {
      case 'AUTH001':
        methods.setError('identifier', { 
          message: 'Invalid username/email or password' 
        });
        break;

      case 'AUTH002':
        const waitSeconds = errorData.detail?.details?.wait_seconds || 60;
        setLockoutTime(waitSeconds);
        methods.setError('root', {
          message: `Too many failed attempts. Account locked for ${waitSeconds} seconds.`,
        });
        break;

      default:
        methods.setError('root', {
          message: errorData.detail?.message || errorData.detail || 'Login failed',
        });
    }
  };

  const handleVerifyOTP = async () => {
    if (otpCode.length !== 6) return;

    setIsLoggingIn(true);
    setTwoFaError('');

    try {
      const response = await fetch(`${API_BASE_URL}/auth/login/2fa`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          pre_auth_token: preAuthToken,
          code: otpCode,
        }),
      });

      const result = await response.json();

      if (!response.ok) {
        throw new Error(result.detail?.message || 'Verification failed');
      }

      // Check for warnings
      if (result.warnings?.length > 0) {
        // Show modal and block redirect
        setSessionWarning(result.warnings[0].message);
        setPendingRedirectToken(result.access_token);
        setShowWarningModal(true);
      } else {
        // No warnings, proceed directly
        localStorage.setItem('token', result.access_token);
        router.push('/dashboard');
      }
    } catch (error) {
      setTwoFaError(error instanceof Error ? error.message : 'Invalid verification code');
    } finally {
      setIsLoggingIn(false);
    }
  };

  const handleAcknowledgeWarning = () => {
    if (pendingRedirectToken) {
      localStorage.setItem('token', pendingRedirectToken);
      setShowWarningModal(false);
      router.push('/dashboard');
    }
  };

  const isDisabled = isLoggingIn || lockoutTime > 0;

  // Session Warning Modal
  const SessionWarningModal = () => (
    <AnimatePresence>
      {showWarningModal && (
        <>
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50"
            onClick={(e) => e.stopPropagation()}
          />
          
          {/* Modal */}
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <motion.div
              initial={{ opacity: 0, scale: 0.95, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 20 }}
              className="bg-white rounded-lg shadow-2xl max-w-md w-full p-6 space-y-4"
              onClick={(e) => e.stopPropagation()}
            >
              {/* Icon */}
              <div className="flex items-center justify-center w-12 h-12 rounded-full bg-yellow-100 mx-auto">
                <AlertTriangle className="h-6 w-6 text-yellow-600" />
              </div>

              {/* Title */}
              <h3 className="text-xl font-semibold text-center text-gray-900">
                Multiple Active Sessions Detected
              </h3>

              {/* Message */}
              <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4">
                <p className="text-sm text-yellow-800 text-center">
                  {sessionWarning}
                </p>
              </div>

              {/* Description */}
              <p className="text-sm text-gray-600 text-center">
                Please ensure you're logging in from a secure location. If you don't recognize these sessions, change your password immediately.
              </p>

              {/* Action Button */}
              <Button
                onClick={handleAcknowledgeWarning}
                className="w-full bg-gradient-to-r from-primary to-secondary hover:opacity-90"
              >
                I Understand, Continue
              </Button>
            </motion.div>
          </div>
        </>
      )}
    </AnimatePresence>
  );

  // 2FA View
  if (show2FA) {
    return (
      <>
        <SessionWarningModal />
        <AuthLayout 
          title="Two-Factor Authentication" 
          subtitle="Enter the 6-digit code sent to your email"
        >
          <div className="space-y-6">
          <div className="space-y-2">
            <label className="text-sm font-medium leading-none">
              Verification Code
            </label>
            <Input
              value={otpCode}
              onChange={(e) => setOtpCode(e.target.value.replace(/\D/g, ''))}
              placeholder="000000"
              className="text-center text-lg tracking-widest"
              maxLength={6}
              disabled={isDisabled}
              autoFocus
            />
            {twoFaError && (
              <p className="text-sm font-medium text-red-600 flex items-center gap-2">
                <AlertCircle className="h-4 w-4" />
                {twoFaError}
              </p>
            )}
          </div>

          <Button
            onClick={handleVerifyOTP}
            className="w-full"
            disabled={isDisabled || otpCode.length !== 6}
          >
            {isLoggingIn && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Verify Code
          </Button>

          <Button
            variant="ghost"
            onClick={() => {
              setShow2FA(false);
              setOtpCode('');
              setTwoFaError('');
            }}
            className="w-full text-muted-foreground"
            disabled={isDisabled}
          >
            Back to Login
          </Button>
        </div>
      </AuthLayout>
      </>
    );
  }

  // Main Login View
  return (
    <>
      <SessionWarningModal />
      <AuthLayout 
        title="Welcome back" 
        subtitle="Enter your credentials to access your account"
      >
        <Form
        schema={loginSchema}
        onSubmit={handleLoginSubmit}
        className="space-y-5"
      >
        {(methods) => (
          <>
            {/* Error Messages */}
            {methods.formState.errors.root && (
              <div className="bg-red-50 border border-red-200 text-red-600 text-sm p-3 rounded-md flex items-center">
                <AlertCircle className="h-4 w-4 mr-2 flex-shrink-0" />
                {lockoutTime > 0
                  ? `Too many failed attempts. Please try again in ${lockoutTime}s`
                  : methods.formState.errors.root.message}
              </div>
            )}

            {/* Keyboard Listener */}
            <FormKeyboardListener reset={methods.reset} />

            {/* Email/Username Field */}
            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.1 }}
            >
              <FormField
                control={methods.control}
                name="identifier"
                label="Email or Username"
                placeholder="you@example.com"
                type="text"
                required
                disabled={isDisabled}
              />
            </motion.div>

            {/* Password Field */}
            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.15 }}
            >
              <FormField 
                control={methods.control} 
                name="password" 
                label="Password" 
                required
              >
                {(fieldProps) => (
                  <div className="relative">
                    <Input
                      {...fieldProps}
                      type={showPassword ? 'text' : 'password'}
                      placeholder="Enter your password"
                      className="pr-10"
                      disabled={isDisabled}
                      autoComplete="current-password"
                      onPaste={(e) => e.preventDefault()}
                      onCopy={(e) => e.preventDefault()}
                      onCut={(e) => e.preventDefault()}
                      onContextMenu={(e) => e.preventDefault()}
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                      tabIndex={-1}
                      aria-label={showPassword ? 'Hide password' : 'Show password'}
                    >
                      {showPassword ? (
                        <EyeOff className="h-4 w-4" />
                      ) : (
                        <Eye className="h-4 w-4" />
                      )}
                    </button>
                  </div>
                )}
              </FormField>
            </motion.div>

            {/* Remember Me & Forgot Password */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.2 }}
              className="flex items-center justify-between"
            >
              <label className="flex items-center gap-2 cursor-pointer group">
                <input
                  type="checkbox"
                  {...methods.register('rememberMe')}
                  disabled={isDisabled}
                  className="h-4 w-4 rounded border-input text-primary focus:ring-primary transition-colors cursor-pointer disabled:cursor-not-allowed"
                />
                <span className="text-sm text-muted-foreground group-hover:text-foreground transition-colors">
                  Remember me
                </span>
              </label>
              <Link
                href="/forgot-password"
                className="text-sm text-primary hover:text-primary/80 transition-colors"
              >
                Forgot password?
              </Link>
            </motion.div>

            {/* Submit Button */}
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.25 }}
            >
              <Button
                type="submit"
                disabled={isDisabled}
                className="w-full h-11 bg-gradient-to-r from-primary to-secondary hover:opacity-90 transition-opacity"
              >
                {lockoutTime > 0 ? (
                  `Retry in ${lockoutTime}s`
                ) : isLoggingIn ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Signing in...
                  </>
                ) : (
                  'Sign in'
                )}
              </Button>
            </motion.div>

            {/* Social Login */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.3 }}
            >
              <SocialLogin isLoading={isDisabled} />
            </motion.div>

            {/* Sign Up Link */}
            <motion.p
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.35 }}
              className="text-center text-sm text-muted-foreground"
            >
              Don&apos;t have an account?{' '}
              <Link
                href="/register"
                className="text-primary hover:text-primary/80 font-medium transition-colors"
              >
                Sign up
              </Link>
            </motion.p>
          </>
        )}
      </Form>
    </AuthLayout>
    </>
  );
}


function FormKeyboardListener({ reset }: { reset: (values?: any) => void }) {
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        reset({
          identifier: '',
          password: '',
          rememberMe: false,
        });
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [reset]);

  return null;
}