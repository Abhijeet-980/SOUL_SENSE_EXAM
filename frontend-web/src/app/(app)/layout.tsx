'use client';

import * as React from 'react';
import { useAuth } from '@/hooks/useAuth';
import { Sidebar, Header } from '@/components/app';

export default function AppLayout({ children }: { children: React.ReactNode }) {
  // Authentication checks are handled by Edge middleware; this hook is used only for UI state
  const { isAuthenticated, isLoading } = useAuth();

  return (
    <div className="flex h-screen bg-background text-foreground relative">
      <Sidebar />

      {/* Main content area: flex-1 ensures it expands to fill all remaining space
          when the sidebar collapses (desktop 80px strip) or is removed from flow (mobile fixed positioning) */}
      <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
        <Header />
        <main className="flex-1 overflow-y-auto p-4 md:p-8">
          {children}
        </main>
      </div>
    </div>
  );
}
