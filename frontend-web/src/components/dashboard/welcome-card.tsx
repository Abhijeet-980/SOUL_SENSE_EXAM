'use client';

import { useAuth } from '@/hooks/useAuth';
import { BentoGridItem } from './bento-grid';
import { User } from 'lucide-react';

export const WelcomeCard = () => {
  const { user } = useAuth();

  return (
    <BentoGridItem
      title={`Welcome back, ${user?.name || 'Friend'}!`}
      description="Your personal growth journey continues here. Take a moment to reflect and see how you're progressing today."
      header={
        <div className="flex flex-1 w-full h-full min-h-[6rem] rounded-xl bg-gradient-to-br from-blue-500/20 to-purple-500/20 items-center justify-center">
          <User className="h-12 w-12 text-blue-500" />
        </div>
      }
      icon={<User className="h-4 w-4" />}
      className="md:col-span-2"
    />
  );
};
