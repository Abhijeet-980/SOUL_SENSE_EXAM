'use client';

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { motion, AnimatePresence } from 'framer-motion';
import { Home, LayoutDashboard, Zap, MessageSquare, User, Rocket, Sun, Moon } from 'lucide-react';
import { useTheme } from 'next-themes';
import { cn } from '@/lib/utils';

const navItems = [
  { name: 'Home', href: '/', icon: Home },
  { name: 'Pulse', href: '/community', icon: LayoutDashboard },
  { name: 'Features', href: '/#features', icon: Zap },
  { name: 'Testimonials', href: '/#testimonials', icon: MessageSquare },
];

export function FloatingNavbar() {
  const pathname = usePathname();
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const [isScrolled, setIsScrolled] = useState(false);
  const [isVisible, setIsVisible] = useState(true);
  const [lastScrollY, setLastScrollY] = useState(0);
  const { setTheme, theme } = useTheme();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const handleScroll = () => {
      const currentScrollY = window.scrollY;

      // Basic "isScrolled" logic for scale/shadow
      setIsScrolled(currentScrollY > 20);

      // Smart Visibility logic
      if (currentScrollY > lastScrollY && currentScrollY > 100) {
        // Scrolling Down & past threshold -> Hide
        setIsVisible(false);
      } else {
        // Scrolling Up -> Show
        setIsVisible(true);
      }

      setLastScrollY(currentScrollY);
    };

    window.addEventListener('scroll', handleScroll, { passive: true });
    return () => window.removeEventListener('scroll', handleScroll);
  }, [lastScrollY]);

  return (
    <div className="fixed top-8 left-0 right-0 z-[100] flex justify-center px-4 pointer-events-none">
      <motion.nav
        initial={{ y: -100, opacity: 0 }}
        animate={{
          y: isVisible ? 0 : -120,
          opacity: isVisible ? 1 : 0,
          scale: isScrolled ? 0.95 : 1,
        }}
        transition={{
          type: 'spring',
          stiffness: 260,
          damping: 20,
          opacity: { duration: 0.2 },
        }}
        className={cn(
          'pointer-events-auto flex items-center justify-between px-2 md:px-3 rounded-full border',
          'bg-white/70 dark:bg-slate-950/80 backdrop-blur-xl border-slate-200/50 dark:border-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.12)] dark:shadow-[0_8px_32px_rgba(0,0,0,0.4)]',
          'min-w-[320px] md:min-w-[640px] lg:min-w-[800px]',
          isScrolled ? 'py-1' : 'py-1.5'
        )}
      >
        {/* LEFT: Branding (Minimalist) */}
        <div className="flex items-center">
          <Link
            href="/"
            className="flex items-center justify-center w-10 h-10 rounded-full hover:bg-slate-100 dark:hover:bg-white/5 transition-all group"
          >
            <div className="relative">
              <div className="absolute inset-0 bg-sky-500/15 blur-lg rounded-full group-hover:bg-sky-500/30 transition-all" />
              <Rocket className="relative w-4 h-4 text-sky-600 dark:text-sky-400 group-hover:rotate-12 transition-transform duration-500" />
            </div>
          </Link>
        </div>

        {/* CENTER: Navigation (Modern Glide - Transparent) */}
        <div className="flex items-center gap-1">
          {navItems.map((item, index) => {
            const isActive = pathname === item.href;
            const isHovered = hoveredIndex === index;

            return (
              <Link
                key={item.name}
                href={item.href}
                onMouseEnter={() => setHoveredIndex(index)}
                onMouseLeave={() => setHoveredIndex(null)}
                className="relative flex items-center h-8 px-5 rounded-full outline-none transition-all duration-300"
              >
                {/* Glide Indicator - Scoped and synced with parent entrance */}
                <AnimatePresence mode="popLayout">
                  {(isActive || isHovered) && (
                    <motion.div
                      layoutId="navbar-glide-pill"
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      exit={{ opacity: 0 }}
                      className={cn(
                        'absolute inset-0 rounded-full z-0',
                        isActive
                          ? 'bg-sky-500/10 dark:bg-sky-400/10 border border-sky-200/50 dark:border-sky-400/20'
                          : 'bg-slate-200/50 dark:bg-white/5'
                      )}
                      transition={{
                        layout: { type: 'spring', bounce: 0.1, duration: 0.4 },
                        opacity: { duration: 0.2 },
                      }}
                    />
                  )}
                </AnimatePresence>

                <div className="relative z-10 flex items-center">
                  <span
                    className={cn(
                      'whitespace-nowrap text-[10px] font-bold tracking-[0.16em] uppercase antialiased transition-colors duration-300',
                      isActive
                        ? 'text-sky-600 dark:text-sky-400'
                        : isHovered
                          ? 'text-slate-900 dark:text-white'
                          : 'text-slate-500 dark:text-slate-400'
                    )}
                  >
                    {item.name}
                  </span>
                </div>
              </Link>
            );
          })}
        </div>

        {/* RIGHT: Sophisticated Controls & Sign In */}
        <div className="flex items-center gap-2 px-1">
          {mounted && (
            <button
              onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
              className="p-2 rounded-full hover:bg-slate-100 dark:hover:bg-white/10 text-slate-500 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white transition-all active:scale-95"
              aria-label="Toggle theme"
            >
              {theme === 'dark' ? (
                <Sun className="w-3.5 h-3.5" />
              ) : (
                <Moon className="w-3.5 h-3.5" />
              )}
            </button>
          )}

          <div className="h-4 w-[1px] bg-slate-200 dark:bg-white/10 hidden md:block mx-1" />

          <motion.div whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}>
            <Link
              href="/login"
              className={cn(
                'px-6 py-2 text-[10px] font-bold uppercase tracking-[0.2em] rounded-full transition-all shadow-lg',
                'bg-slate-950 dark:bg-white text-white dark:text-slate-900 hover:bg-sky-600 dark:hover:bg-sky-50 shadow-slate-900/10 dark:shadow-white/5'
              )}
            >
              Sign In
            </Link>
          </motion.div>
        </div>
      </motion.nav>
    </div>
  );
}
