'use client';

import React from 'react';
import Link from 'next/link';
import { motion, Variants } from 'framer-motion';
import { PenTool, Activity, BarChart3, User } from 'lucide-react';
import { Card } from '@/components/ui';
import { cn } from '@/lib/utils';

const actions = [
  {
    label: 'New Journal',
    icon: PenTool,
    href: '/journal/new',
    description: 'Reflect on your day',
  },
  {
    label: 'Take Assessment',
    icon: Activity,
    href: '/exam',
    description: 'Check your EQ',
  },
  {
    label: 'View Results',
    icon: BarChart3,
    href: '/results',
    description: 'Analyze progress',
  },
  {
    label: 'Update Profile',
    icon: User,
    href: '/profile',
    description: 'Manage settings',
  },
];

const containerVariants: Variants = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1,
    },
  },
};

const itemVariants: Variants = {
  hidden: { opacity: 0, y: 15 },
  show: { opacity: 1, y: 0, transition: { type: 'tween', ease: 'easeOut', duration: 0.3 } },
};

export function QuickActions() {
  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold tracking-tight">Quick Actions</h2>
      <motion.div
        variants={containerVariants}
        initial="hidden"
        animate="show"
        className="grid grid-cols-2 lg:grid-cols-4 gap-4"
      >
        {actions.map((action) => (
          <motion.div key={action.label} variants={itemVariants}>
            <Link
              href={action.href}
              className="block group h-full focus:outline-none focus-visible:ring-2 focus-visible:ring-primary/50 focus-visible:ring-offset-2 rounded-2xl"
              aria-label={action.label}
            >
              <Card className="h-full border border-border/40 shadow-sm hover:shadow-md transition-all duration-300 bg-background/60 backdrop-blur-md hover:bg-muted/30 overflow-hidden relative">
                <div className="p-5 flex flex-col items-center justify-center text-center space-y-3 h-full relative z-10 transition-transform duration-300 active:scale-[0.98]">
                  <div className="p-3 rounded-xl bg-primary/5 text-primary ring-1 ring-inset ring-foreground/5 shadow-sm group-hover:bg-primary/10 transition-colors duration-300">
                    <action.icon className="w-5 h-5" strokeWidth={1.5} />
                  </div>
                  <div className="space-y-1">
                    <h3 className="font-medium text-sm sm:text-base leading-none group-hover:text-primary transition-colors">
                      {action.label}
                    </h3>
                    <p className="text-xs text-muted-foreground hidden sm:block">
                      {action.description}
                    </p>
                  </div>
                </div>
              </Card>
            </Link>
          </motion.div>
        ))}
      </motion.div>
    </div>
  );
}
