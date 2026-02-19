import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X, Mail, Lock, CheckCircle, AlertCircle, Loader2 } from 'lucide-react';
import { Button, Input } from '@/components/ui';

interface ForgotPasswordModalProps {
  isOpen: boolean;
  onClose: () => void;
}

type Step = 'email' | 'otp' | 'new-password' | 'success';

export function ForgotPasswordModal({ isOpen, onClose }: ForgotPasswordModalProps) {
  const [step, setStep] = useState<Step>('email');
  const [email, setEmail] = useState('');
  const [otp, setOtp] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const resetModal = () => {
    setStep('email');
    setEmail('');
    setOtp('');
    setNewPassword('');
    setConfirmPassword('');
    setError('');
    setSuccess('');
  };

  const handleClose = () => {
    resetModal();
    onClose();
  };

  const handleInitiateReset = async () => {
    if (!email.trim()) {
      setError('Please enter your email address');
      return;
    }

    setIsLoading(true);
    setError('');

    const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
    try {
      const response = await fetch(`${apiUrl}/api/v1/auth/password-reset/initiate`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ email: email.trim() }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail?.message || 'Failed to send reset email');
      }

      setSuccess('Password reset code sent to your email');
      setStep('otp');
    } catch (error) {
      setError(error instanceof Error ? error.message : 'An error occurred');
    } finally {
      setIsLoading(false);
    }
  };

  const handleVerifyOtp = async () => {
    if (!otp.trim()) {
      setError('Please enter the verification code');
      return;
    }

    setIsLoading(true);
    setError('');

    try {
      // For now, we'll just move to the next step
      // In a real implementation, you might want to verify the OTP here
      setStep('new-password');
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Invalid verification code');
    } finally {
      setIsLoading(false);
    }
  };

  const handleResetPassword = async () => {
    if (!newPassword.trim()) {
      setError('Please enter a new password');
      return;
    }

    if (newPassword !== confirmPassword) {
      setError('Passwords do not match');
      return;
    }

    if (newPassword.length < 8) {
      setError('Password must be at least 8 characters long');
      return;
    }

    setIsLoading(true);
    setError('');

    const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
    try {
      const response = await fetch(`${apiUrl}/api/v1/auth/password-reset/complete`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          email: email.trim(),
          otp: otp.trim(),
          new_password: newPassword,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail?.message || 'Failed to reset password');
      }

      setSuccess('Password reset successfully!');
      setStep('success');
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Failed to reset password');
    } finally {
      setIsLoading(false);
    }
  };

  const getStepTitle = () => {
    switch (step) {
      case 'email':
        return 'Reset Password';
      case 'otp':
        return 'Enter Verification Code';
      case 'new-password':
        return 'Set New Password';
      case 'success':
        return 'Password Reset Complete';
      default:
        return 'Reset Password';
    }
  };

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 bg-black/50 z-50"
            onClick={handleClose}
          />

          {/* Modal */}
          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 0.95 }}
            className="fixed inset-0 z-50 flex items-center justify-center p-4"
          >
            <div className="bg-white rounded-lg shadow-xl max-w-md w-full max-h-[90vh] overflow-y-auto">
              {/* Header */}
              <div className="flex items-center justify-between p-6 border-b">
                <h2 className="text-xl font-semibold">{getStepTitle()}</h2>
                <Button variant="ghost" size="sm" onClick={handleClose} className="h-8 w-8 p-0">
                  <X className="h-4 w-4" />
                </Button>
              </div>

              {/* Content */}
              <div className="p-6">
                {error && (
                  <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-md flex items-center text-red-700">
                    <AlertCircle className="h-4 w-4 mr-2 flex-shrink-0" />
                    {error}
                  </div>
                )}

                {success && step !== 'success' && (
                  <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded-md flex items-center text-green-700">
                    <CheckCircle className="h-4 w-4 mr-2 flex-shrink-0" />
                    {success}
                  </div>
                )}

                {step === 'email' && (
                  <div className="space-y-4">
                    <p className="text-sm text-gray-600">
                      Enter your email address and we&apos;ll send you a verification code to reset
                      your password.
                    </p>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Email Address</label>
                      <Input
                        type="email"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        placeholder="you@example.com"
                        disabled={isLoading}
                      />
                    </div>
                    <Button
                      onClick={handleInitiateReset}
                      disabled={isLoading || !email.trim()}
                      className="w-full"
                    >
                      {isLoading ? (
                        <>
                          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                          Sending...
                        </>
                      ) : (
                        <>
                          <Mail className="mr-2 h-4 w-4" />
                          Send Reset Code
                        </>
                      )}
                    </Button>
                  </div>
                )}

                {step === 'otp' && (
                  <div className="space-y-4">
                    <p className="text-sm text-gray-600">
                      We&apos;ve sent a verification code to <strong>{email}</strong>. Enter the
                      code below to continue.
                    </p>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Verification Code</label>
                      <Input
                        type="text"
                        value={otp}
                        onChange={(e) => setOtp(e.target.value)}
                        placeholder="Enter 6-digit code"
                        maxLength={6}
                        disabled={isLoading}
                      />
                    </div>
                    <div className="flex gap-2">
                      <Button
                        variant="outline"
                        onClick={() => setStep('email')}
                        disabled={isLoading}
                        className="flex-1"
                      >
                        Back
                      </Button>
                      <Button
                        onClick={handleVerifyOtp}
                        disabled={isLoading || !otp.trim()}
                        className="flex-1"
                      >
                        {isLoading ? (
                          <>
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            Verifying...
                          </>
                        ) : (
                          'Verify Code'
                        )}
                      </Button>
                    </div>
                  </div>
                )}

                {step === 'new-password' && (
                  <div className="space-y-4">
                    <p className="text-sm text-gray-600">Enter your new password below.</p>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">New Password</label>
                      <Input
                        type="password"
                        value={newPassword}
                        onChange={(e) => setNewPassword(e.target.value)}
                        placeholder="Enter new password"
                        disabled={isLoading}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Confirm Password</label>
                      <Input
                        type="password"
                        value={confirmPassword}
                        onChange={(e) => setConfirmPassword(e.target.value)}
                        placeholder="Confirm new password"
                        disabled={isLoading}
                      />
                    </div>
                    <div className="flex gap-2">
                      <Button
                        variant="outline"
                        onClick={() => setStep('otp')}
                        disabled={isLoading}
                        className="flex-1"
                      >
                        Back
                      </Button>
                      <Button
                        onClick={handleResetPassword}
                        disabled={isLoading || !newPassword.trim() || !confirmPassword.trim()}
                        className="flex-1"
                      >
                        {isLoading ? (
                          <>
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            Resetting...
                          </>
                        ) : (
                          <>
                            <Lock className="mr-2 h-4 w-4" />
                            Reset Password
                          </>
                        )}
                      </Button>
                    </div>
                  </div>
                )}

                {step === 'success' && (
                  <div className="space-y-4 text-center">
                    <div className="flex justify-center">
                      <CheckCircle className="h-16 w-16 text-green-500" />
                    </div>
                    <h3 className="text-lg font-semibold text-green-700">
                      Password Reset Successful!
                    </h3>
                    <p className="text-sm text-gray-600">
                      Your password has been successfully reset. You can now log in with your new
                      password.
                    </p>
                    <Button onClick={handleClose} className="w-full">
                      Continue to Login
                    </Button>
                  </div>
                )}
              </div>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}
