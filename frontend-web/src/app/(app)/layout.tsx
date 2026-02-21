'use client';

import * as React from 'react';
import { useEffect } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import { useAuth } from '@/hooks/useAuth';
import { Sidebar, Header } from '@/components/app';

export default function AppLayout({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, isLoading } = useAuth();
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    if (!isLoading && !isAuthenticated) {
      const callbackUrl = encodeURIComponent(pathname);
      router.push(`/login?callbackUrl=${callbackUrl}`);
    }
  }, [isLoading, isAuthenticated, router, pathname]);

  return (
    <div className="flex h-screen bg-background text-foreground relative">
      {(isLoading || !isAuthenticated) && (
        <div className="absolute inset-0 z-50 flex items-center justify-center bg-background">
          {isLoading && (
            <div className="w-12 h-12 border-4 border-muted rounded-full animate-spin border-t-primary"></div>
          )}
        </div>
      )}

      <Sidebar />

      <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
        <Header />
        <main
          className="flex-1 overflow-y-auto p-4 md:p-8"
          style={{ display: isLoading || !isAuthenticated ? 'none' : 'block' }}
        >
          {children}
        </main>
      </div>
    </div>
  );
}
