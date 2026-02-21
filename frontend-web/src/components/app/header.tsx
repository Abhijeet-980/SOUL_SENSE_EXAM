'use client';

import * as React from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { Bell, LogOut, Settings, User, Search, ChevronDown } from 'lucide-react';
import { Avatar, AvatarFallback, AvatarImage, Button, Input } from '@/components/ui';
import { useAuth } from '@/hooks/useAuth';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

interface HeaderProps {
  className?: string;
  notificationCount?: number;
}

export const Header = React.forwardRef<HTMLElement, HeaderProps>(
  ({ className, notificationCount = 0 }, ref) => {
    const router = useRouter();
    const { user, logout } = useAuth();
    const [isDropdownOpen, setIsDropdownOpen] = React.useState(false);
    const [isScrolled, setIsScrolled] = React.useState(false);
    const dropdownRef = React.useRef<HTMLDivElement>(null);

    // Track scroll for header transition
    React.useEffect(() => {
      const handleScroll = () => {
        setIsScrolled(window.scrollY > 10);
      };
      window.addEventListener('scroll', handleScroll);
      return () => window.removeEventListener('scroll', handleScroll);
    }, []);

    // Close dropdown when clicking outside
    React.useEffect(() => {
      const handleClickOutside = (event: MouseEvent) => {
        if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
          setIsDropdownOpen(false);
        }
      };

      if (isDropdownOpen) {
        document.addEventListener('mousedown', handleClickOutside);
      }

      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }, [isDropdownOpen]);

    const getUserInitials = (name?: string) => {
      if (!name) return 'U';
      return name
        .split(' ')
        .map((p) => p[0])
        .join('')
        .slice(0, 2)
        .toUpperCase();
    };

    const handleLogout = async () => {
      setIsDropdownOpen(false);
      await logout();
    };

    const handleNavigate = (path: string) => {
      setIsDropdownOpen(false);
      router.push(path);
    };

    return (
      <header
        ref={ref}
        className={cn(
          'sticky top-0 z-50 w-full transition-all duration-300',
          isScrolled
            ? 'h-14 bg-background/40 backdrop-blur-2xl border-b border-border/40 shadow-sm'
            : 'h-16 bg-background/0 border-b border-transparent',
          className
        )}
      >
        <div className="flex h-full items-center justify-between px-4 sm:px-6 lg:px-8 max-w-[1600px] mx-auto">
          {/* Left side - Search Bar Placeholder */}
          <div className="flex-1 max-w-md hidden md:block">
            <div className="relative group">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground group-focus-within:text-primary transition-colors" />
              <Input
                placeholder="Search emotions, exams, or journals..."
                className="pl-10 h-9 bg-muted/40 border-none focus-visible:ring-1 focus-visible:ring-primary/40 rounded-full transition-all"
              />
            </div>
          </div>

          <div className="flex md:hidden items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-primary to-secondary text-white font-bold text-sm shadow-sm">
              S
            </div>
          </div>

          {/* Right side - Notifications and User menu */}
          <div className="flex items-center gap-2 sm:gap-4">
            <button
              onClick={() => handleNavigate('/notifications')}
              className="relative rounded-full p-2 text-muted-foreground transition-all hover:bg-muted hover:text-foreground active:scale-95"
              aria-label="Notifications"
            >
              <Bell className="h-5 w-5" />
              {notificationCount > 0 && (
                <span className="absolute right-1.5 top-1.5 flex h-4 w-4 items-center justify-center rounded-full bg-primary text-[10px] font-bold text-white shadow-sm ring-2 ring-background">
                  {notificationCount > 99 ? '9' : notificationCount}
                </span>
              )}
            </button>

            <div className="h-6 w-px bg-border/60 mx-1 hidden sm:block" />

            {/* User Avatar Dropdown */}
            <div className="relative" ref={dropdownRef}>
              <button
                onClick={() => setIsDropdownOpen(!isDropdownOpen)}
                className={cn(
                  'flex items-center gap-2 rounded-full pl-1 pr-2 py-1 transition-all hover:bg-muted active:scale-95',
                  isDropdownOpen && 'bg-muted'
                )}
                aria-label="User menu"
              >
                <div className="relative">
                  <Avatar className="h-8 w-8 border-2 border-background shadow-sm">
                    <AvatarFallback className="bg-gradient-to-br from-primary/80 to-secondary/80 text-white text-xs font-bold">
                      {getUserInitials(user?.name)}
                    </AvatarFallback>
                  </Avatar>
                  <div className="absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 rounded-full bg-green-500 border-2 border-background" />
                </div>

                <div className="hidden sm:flex flex-col items-start leading-none gap-1">
                  <span className="text-xs font-semibold truncate max-w-[100px]">
                    {user?.name || 'User'}
                  </span>
                  <ChevronDown
                    className={cn(
                      'h-3 w-3 text-muted-foreground transition-transform duration-300',
                      isDropdownOpen && 'rotate-180'
                    )}
                  />
                </div>
              </button>

              {/* Enhanced Dropdown Menu */}
              <AnimatePresence>
                {isDropdownOpen && (
                  <motion.div
                    initial={{ opacity: 0, y: 10, scale: 0.95 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: 10, scale: 0.95 }}
                    transition={{ duration: 0.2, ease: 'easeOut' }}
                    className="absolute right-0 mt-3 w-64 rounded-2xl border bg-background/95 backdrop-blur-xl shadow-2xl z-50 overflow-hidden"
                  >
                    {/* User Profile Summary */}
                    <div className="bg-muted/30 px-5 py-4 flex items-center gap-3">
                      <Avatar className="h-10 w-10 border border-primary/20">
                        <AvatarFallback className="bg-primary/5 text-primary tracking-wider font-semibold">
                          {getUserInitials(user?.name)}
                        </AvatarFallback>
                      </Avatar>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-bold text-foreground truncate">
                          {user?.name || 'User'}
                        </p>
                        <p className="text-[10px] text-muted-foreground truncate font-medium tracking-wide">
                          {user?.email}
                        </p>
                      </div>
                    </div>

                    {/* Menu Items */}
                    <div className="p-2 space-y-1">
                      <button
                        onClick={() => handleNavigate('/profile')}
                        className="flex w-full items-center gap-3 px-3 py-2 text-sm font-medium rounded-xl transition-colors hover:bg-primary/10 hover:text-primary group"
                      >
                        <div className="p-1.5 rounded-lg bg-muted group-hover:bg-primary/20 transition-colors">
                          <User className="h-4 w-4" />
                        </div>
                        My Profile
                      </button>

                      <button
                        onClick={() => handleNavigate('/settings')}
                        className="flex w-full items-center gap-3 px-3 py-2 text-sm font-medium rounded-xl transition-colors hover:bg-primary/10 hover:text-primary group"
                      >
                        <div className="p-1.5 rounded-lg bg-muted group-hover:bg-primary/20 transition-colors">
                          <Settings className="h-4 w-4" />
                        </div>
                        Settings
                      </button>
                    </div>

                    <div className="px-2 pb-2">
                      <button
                        onClick={handleLogout}
                        className="flex w-full items-center gap-3 px-3 py-2 text-sm font-bold text-red-500 rounded-xl transition-colors hover:bg-red-500/10 group"
                      >
                        <div className="p-1.5 rounded-lg bg-red-500/10 group-hover:bg-red-500/20 transition-colors">
                          <LogOut className="h-4 w-4" />
                        </div>
                        Log out
                      </button>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </div>
        </div>
      </header>
    );
  }
);

Header.displayName = 'Header';
