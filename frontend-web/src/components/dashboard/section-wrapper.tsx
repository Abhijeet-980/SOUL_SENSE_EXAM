'use client';

import React, { ReactNode } from 'react';
import { Skeleton } from '@/components/ui';
import { AlertCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';

interface SectionWrapperProps {
    isLoading: boolean;
    error: any;
    children: ReactNode;
    fallback?: ReactNode;
    onRetry?: () => void;
}

export const SectionWrapper = ({
    isLoading,
    error,
    children,
    fallback,
    onRetry
}: SectionWrapperProps) => {
    if (isLoading) {
        return fallback || <Skeleton className="w-full h-[200px] rounded-3xl" />;
    }

    if (error) {
        return (
            <div className="flex flex-col items-center justify-center p-6 h-[200px] rounded-3xl border border-dashed border-red-500/50 bg-red-500/5">
                <AlertCircle className="h-8 w-8 text-red-500 mb-2" />
                <p className="text-sm font-medium text-red-600 dark:text-red-400">Failed to load section</p>
                {onRetry && (
                    <Button variant="ghost" size="sm" onClick={onRetry} className="mt-2 text-red-500 hover:text-red-600">
                        Retry
                    </Button>
                )}
            </div>
        );
    }

    return <>{children}</>;
};
