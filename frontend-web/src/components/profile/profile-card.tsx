'use client';

import { PersonalProfile } from '@/lib/api/profile';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui';
import { cn } from '@/lib/utils';
import { Edit } from 'lucide-react';

interface ProfileCardProps {
  profile: PersonalProfile | null;
  user?: {
    username?: string;
    email?: string;
    created_at?: string;
    name?: string;
  };
  variant?: 'compact' | 'full';
  editable?: boolean;
  onEdit?: () => void;
  className?: string;
}

export function ProfileCard({
  profile,
  user,
  variant = 'full',
  editable = false,
  onEdit,
  className
}: ProfileCardProps) {
  // Handle null profile
  if (!profile) {
    return (
      <div className={cn('bg-card text-card-foreground rounded-lg border shadow-sm p-6 text-center', className)}>
        <p className="text-muted-foreground">Profile data not available</p>
      </div>
    );
  }

  const getInitials = () => {
    return `${profile.first_name[0]}${profile.last_name[0]}`.toUpperCase();
  };

  const getFullName = () => {
    return `${profile.first_name} ${profile.last_name}`;
  };

  const truncateBio = (bio: string, maxLength: number = 100) => {
    if (bio.length <= maxLength) return bio;
    return bio.substring(0, maxLength) + '...';
  };

  const formatMemberSince = (dateString?: string) => {
    const date = dateString || user?.created_at;
    if (!date) return 'Unknown';
    return new Date(date).toLocaleDateString('en-US', {
      month: 'long',
      year: 'numeric',
    });
  };

  const isCompact = variant === 'compact';

  return (
    <div
      className={cn(
        'relative bg-card text-card-foreground rounded-lg border shadow-sm transition-all duration-200 hover:shadow-md',
        editable && 'cursor-pointer group',
        className
      )}
      onClick={editable ? onEdit : undefined}
    >
      {/* Edit overlay */}
      {editable && (
        <div className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity duration-200 rounded-lg flex items-center justify-center z-10">
          <div className="bg-white rounded-full p-3 shadow-lg">
            <Edit className="h-5 w-5 text-gray-700" />
          </div>
        </div>
      )}

      <div className={cn('p-6', isCompact && 'p-4')}>
        {/* Avatar and Name Section */}
        <div className="flex flex-col items-center text-center space-y-4">
          <Avatar className={cn('border-4 border-background shadow-lg', isCompact ? 'h-16 w-16' : 'h-24 w-24')}>
            <AvatarImage src={profile.avatar_url} alt={getFullName()} />
            <AvatarFallback className="bg-gradient-to-br from-primary to-secondary text-white font-bold">
              {getInitials()}
            </AvatarFallback>
          </Avatar>

          <div className="space-y-1">
            <h2 className={cn('font-bold', isCompact ? 'text-lg' : 'text-2xl')}>
              {getFullName()}
            </h2>
            {profile.bio && (
              <p className={cn('text-muted-foreground', isCompact ? 'text-sm' : 'text-base')}>
                {isCompact ? truncateBio(profile.bio, 80) : profile.bio}
              </p>
            )}
          </div>
        </div>

        {/* Details Section */}
        {!isCompact && (
          <div className="mt-6 space-y-4">
            <div className="flex items-center justify-center gap-2 text-sm text-muted-foreground">
              <span>📅</span>
              <span>Member since {formatMemberSince()}</span>
            </div>

            {/* EQ Stats */}
            {profile.eq_stats && (
              <div className="grid grid-cols-2 gap-4 pt-4 border-t">
                {profile.eq_stats.last_score !== undefined && (
                  <div className="text-center">
                    <div className="text-2xl font-bold text-primary">
                      {profile.eq_stats.last_score}
                    </div>
                    <div className="text-xs text-muted-foreground">Last EQ Score</div>
                  </div>
                )}
                {profile.eq_stats.total_assessments !== undefined && (
                  <div className="text-center">
                    <div className="text-2xl font-bold text-primary">
                      {profile.eq_stats.total_assessments}
                    </div>
                    <div className="text-xs text-muted-foreground">Total Assessments</div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Compact mode additional info */}
        {isCompact && (
          <div className="mt-4 space-y-2 text-center">
            <div className="text-xs text-muted-foreground">
              Member since {formatMemberSince()}
            </div>
            {profile.eq_stats && (
              <div className="flex justify-center gap-4 text-xs text-muted-foreground">
                {profile.eq_stats.last_score !== undefined && (
                  <span>EQ: {profile.eq_stats.last_score}</span>
                )}
                {profile.eq_stats.total_assessments !== undefined && (
                  <span>{profile.eq_stats.total_assessments} assessments</span>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}