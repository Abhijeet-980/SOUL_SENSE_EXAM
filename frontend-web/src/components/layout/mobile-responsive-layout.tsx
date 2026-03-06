'use client';

import React from 'react';
import { cn } from '@/lib/utils';
import { useBreakpoint } from '@/hooks/useMobileGestures';

interface MobileResponsiveLayoutProps {
  children: React.ReactNode;
  className?: string;
  sidebar?: React.ReactNode;
  header?: React.ReactNode;
}

export function MobileResponsiveLayout({
  children,
  className,
  sidebar,
  header,
}: MobileResponsiveLayoutProps) {
  const { isMobile } = useBreakpoint();
  
  return (
    <div className={cn('min-h-screen bg-background', className)}>
      {header && (
        <div className="sticky-mobile-header border-b">
          {header}
        </div>
      )}
      
      <div className="flex">
        {sidebar && !isMobile && (
          <aside className="w-64 flex-shrink-0 border-r bg-card/50 hidden lg:block">
            <div className="sticky top-16 h-[calc(100vh-4rem)] overflow-y-auto p-4">
              {sidebar}
            </div>
          </aside>
        )}
        
        <main className={cn(
          'flex-1 min-w-0',
          isMobile && 'pb-mobile-nav'
        )}>
          <div className="container mx-auto px-4 py-6 lg:px-8">
            {children}
          </div>
        </main>
      </div>
    </div>
  );
}

interface MobileCardListProps<T> {
  items: T[];
  renderItem: (item: T, index: number) => React.ReactNode;
  keyExtractor: (item: T) => string | number;
  className?: string;
  emptyMessage?: string;
}

export function MobileCardList<T>({
  items,
  renderItem,
  keyExtractor,
  className,
  emptyMessage = 'No items found',
}: MobileCardListProps<T>) {
  if (items.length === 0) {
    return (
      <div className="flex items-center justify-center py-12 text-muted-foreground">
        {emptyMessage}
      </div>
    );
  }
  
  return (
    <div className={cn('space-y-4', className)}>
      {items.map((item, index) => (
        <div key={keyExtractor(item)} className="touch-manipulation">
          {renderItem(item, index)}
        </div>
      ))}
    </div>
  );
}

interface MobileSwipeableContainerProps {
  children: React.ReactNode;
  className?: string;
}

export function MobileSwipeableContainer({
  children,
  className,
}: MobileSwipeableContainerProps) {
  return (
    <div className={cn(
      'overflow-x-auto scrollbar-hide ios-scroll snap-x snap-mandatory',
      className
    )}>
      {children}
    </div>
  );
}

interface MobileSwipeableCardProps {
  children: React.ReactNode;
  className?: string;
}

export function MobileSwipeableCard({
  children,
  className,
}: MobileSwipeableCardProps) {
  return (
    <div className={cn(
      'flex-shrink-0 w-[calc(100%-2rem)] sm:w-auto snap-center',
      className
    )}>
      {children}
    </div>
  );
}

export function MobileSpacer() {
  return <div className="h-20 md:hidden" />;
}

export function MobileOnly({ children }: { children: React.ReactNode }) {
  return <div className="md:hidden">{children}</div>;
}

export function DesktopOnly({ children }: { children: React.ReactNode }) {
  return <div className="hidden md:block">{children}</div>;
}

export function TouchOptimizedButton({
  children,
  className,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      className={cn(
        'min-h-touch min-w-touch touch-manipulation touch-active no-tap-highlight',
        'flex items-center justify-center',
        className
      )}
      {...props}
    >
      {children}
    </button>
  );
}

export function TouchOptimizedLink({
  children,
  className,
  ...props
}: React.AnchorHTMLAttributes<HTMLAnchorElement>) {
  return (
    <a
      className={cn(
        'min-h-touch min-w-touch touch-manipulation touch-active no-tap-highlight',
        'flex items-center justify-center',
        className
      )}
      {...props}
    >
      {children}
    </a>
  );
}
