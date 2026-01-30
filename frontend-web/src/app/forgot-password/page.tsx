'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import { motion } from 'framer-motion';
import { ArrowLeft, Loader2, Mail, CheckCircle2 } from 'lucide-react';
import { Form, FormField } from '@/components/forms';
import { Button } from '@/components/ui';
import { AuthLayout } from '@/components/auth';
import { forgotPasswordSchema } from '@/lib/validation';
import { z } from 'zod';

type ForgotPasswordFormData = z.infer<typeof forgotPasswordSchema>;

export default function ForgotPasswordPage() {
    const [isLoading, setIsLoading] = useState(false);
    const [isSubmitted, setIsSubmitted] = useState(false);
    const [submittedEmail, setSubmittedEmail] = useState('');

    const handleSubmit = async (data: ForgotPasswordFormData) => {
        setIsLoading(true);
        // Simulate API call
        await new Promise((resolve) => setTimeout(resolve, 1500));
        console.log('Forgot password request:', data);
        setSubmittedEmail(data.email);
        setIsSubmitted(true);
        setIsLoading(false);
        // TODO: Implement actual forgot password logic
    };

    if (isSubmitted) {
        return (
            <AuthLayout
                title="Check your email"
                subtitle="We've sent you a password reset link"
            >
                <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                    className="text-center space-y-6"
                >
                    <div className="mx-auto w-16 h-16 rounded-full bg-success/10 flex items-center justify-center">
                        <CheckCircle2 className="h-8 w-8 text-success" />
                    </div>

                    <div className="space-y-2">
                        <p className="text-muted-foreground">
                            We sent an email to
                        </p>
                        <p className="font-medium text-foreground">
                            {submittedEmail}
                        </p>
                        <p className="text-sm text-muted-foreground">
                            Click the link in the email to reset your password. If you don&apos;t see it, check your spam folder.
                        </p>
                    </div>

                    <div className="space-y-3">
                        <Button
                            onClick={() => setIsSubmitted(false)}
                            variant="outline"
                            className="w-full"
                        >
                            Try a different email
                        </Button>

                        <Link href="/login">
                            <Button variant="ghost" className="w-full">
                                <ArrowLeft className="mr-2 h-4 w-4" />
                                Back to sign in
                            </Button>
                        </Link>
                    </div>
                </motion.div>
            </AuthLayout>
        );
    }

    return (
        <AuthLayout
            title="Forgot password?"
            subtitle="No worries, we'll send you reset instructions"
        >
            <Form schema={forgotPasswordSchema} onSubmit={handleSubmit} className="space-y-5">
                {(methods) => (
                    <>
                        <motion.div
                            initial={{ opacity: 0, x: -20 }}
                            animate={{ opacity: 1, x: 0 }}
                            transition={{ delay: 0.2 }}
                        >
                            <FormField
                                control={methods.control}
                                name="email"
                                label="Email"
                                placeholder="Enter your email address"
                                type="email"
                                required
                            />
                        </motion.div>

                        <motion.div
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: 0.25 }}
                        >
                            <Button
                                type="submit"
                                disabled={isLoading}
                                className="w-full h-11 bg-gradient-to-r from-primary to-secondary hover:opacity-90 transition-opacity"
                            >
                                {isLoading ? (
                                    <>
                                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                        Sending...
                                    </>
                                ) : (
                                    <>
                                        <Mail className="mr-2 h-4 w-4" />
                                        Send reset link
                                    </>
                                )}
                            </Button>
                        </motion.div>

                        <motion.div
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            transition={{ delay: 0.3 }}
                        >
                            <Link href="/login">
                                <Button variant="ghost" className="w-full">
                                    <ArrowLeft className="mr-2 h-4 w-4" />
                                    Back to sign in
                                </Button>
                            </Link>
                        </motion.div>
                    </>
                )}
            </Form>
        </AuthLayout>
    );
}
