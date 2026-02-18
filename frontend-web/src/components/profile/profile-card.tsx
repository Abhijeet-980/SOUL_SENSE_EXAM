'use client';

import { PersonalProfile } from '@/lib/api/profile';
import { Avatar, AvatarFallback } from '@/components/ui';
import { cn } from '@/lib/utils';

interface ProfileCardProps {
  profile: PersonalProfile | null;
  user: {
    username?: string;
    email?: string;
    created_at?: string;
    name?: string;
  } | null;
  className?: string;
}

export function ProfileCard({ profile, user, className }: ProfileCardProps) {
  const getInitials = () => {
    if (profile?.first_name && profile?.last_name) {
      return `${profile.first_name[0]}${profile.last_name[0]}`.toUpperCase();
    }
    return (user?.name || user?.username || 'U')
      .split(' ')
      .map((n) => n[0])
      .join('')
      .slice(0, 2)
      .toUpperCase();
  };

  return (
    <div className={cn('space-y-6', className)}>
      {/* Avatar and Name Section */}
      <div className="flex flex-col sm:flex-row items-center gap-6">
        <Avatar className="h-24 w-24 border-4 border-background shadow-lg">
          <AvatarFallback className="bg-gradient-to-br from-primary to-secondary text-white text-xl font-bold">
            {getInitials()}
          </AvatarFallback>
        </Avatar>

        <div className="text-center sm:text-left">
          <h2 className="text-2xl font-bold">
            {profile?.first_name && profile?.last_name
              ? `${profile.first_name} ${profile.last_name}`
              : user?.name || user?.username || 'User'}
          </h2>
          <p className="text-muted-foreground">@{user?.username}</p>
        </div>
      </div>

      {/* Profile Details */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="space-y-4">
          <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
            <span className="text-muted-foreground">📧</span>
            <div>
              <p className="text-sm text-muted-foreground">Email</p>
              <p className="font-medium">{user?.email || 'Not provided'}</p>
            </div>
          </div>

          <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
            <span className="text-muted-foreground">📅</span>
            <div>
              <p className="text-sm text-muted-foreground">Member since</p>
              <p className="font-medium">
                {user?.created_at
                  ? new Date(user.created_at).toLocaleDateString('en-US', {
                      month: 'long',
                      year: 'numeric',
                    })
                  : 'Unknown'}
              </p>
            </div>
          </div>
        </div>

        <div className="space-y-4">
          {profile?.age && (
            <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
              <span className="text-muted-foreground">👤</span>
              <div>
                <p className="text-sm text-muted-foreground">Age</p>
                <p className="font-medium">{profile.age} years old</p>
              </div>
            </div>
          )}

          {profile?.occupation && (
            <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
              <span className="text-muted-foreground">💼</span>
              <div>
                <p className="text-sm text-muted-foreground">Occupation</p>
                <p className="font-medium">{profile.occupation}</p>
              </div>
            </div>
          )}

          {profile?.education_level && (
            <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
              <span className="text-muted-foreground">🎓</span>
              <div>
                <p className="text-sm text-muted-foreground">Education</p>
                <p className="font-medium">{profile.education_level}</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}