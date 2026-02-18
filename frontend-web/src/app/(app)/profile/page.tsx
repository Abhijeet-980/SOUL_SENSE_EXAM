'use client';

import { useState } from 'react';
import { useAuth } from '@/hooks/useAuth';
import { useProfile } from '@/hooks/useProfile';
import { ProfileCard } from '@/components/profile/profile-card';
import { ProfileForm } from '@/components/profile/profile-form';
import { Button } from '@/components/ui';
import { Card, CardContent } from '@/components/ui';
import { Skeleton } from '@/components/ui';
import { motion, AnimatePresence } from 'framer-motion';
import { useApi } from '@/hooks/useApi';
import { resultsApi } from '@/lib/api/results';
import { journalApi } from '@/lib/api/journal';

export default function ProfilePage() {
  const { user } = useAuth();
  const { profile, loading, error, updateProfile, refetch } = useProfile();
  const [isEditing, setIsEditing] = useState(false);

  const { data: examHistory } = useApi({
    apiFn: () => resultsApi.getHistory(1, 1),
    deps: [],
  });

  const { data: journalAnalytics } = useApi({
    apiFn: () => journalApi.getAnalytics(),
    deps: [],
  });

  const handleEditToggle = () => {
    setIsEditing(!isEditing);
  };

  const handleSave = async (data: any) => {
    await updateProfile(data);
    setIsEditing(false);
  };

  const handleCancel = () => {
    setIsEditing(false);
  };

  if (loading) {
    return (
      <div className="max-w-4xl mx-auto py-8 px-4 space-y-8">
        <Skeleton className="h-48 w-full rounded-3xl" />
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Skeleton className="h-64 rounded-3xl" />
          <Skeleton className="h-64 md:col-span-2 rounded-3xl" />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-4xl mx-auto py-8 px-4">
        <div className="text-center">
          <p className="text-red-500 mb-4">Failed to load profile: {error}</p>
          <Button onClick={refetch}>Try Again</Button>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto py-8 px-4 space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Profile</h1>
          <p className="text-muted-foreground">Manage your personal information and view your progress</p>
        </div>
        <Button onClick={handleEditToggle} variant={isEditing ? "outline" : "default"}>
          ✏️ {isEditing ? 'Cancel Edit' : 'Edit Profile'}
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Profile Section */}
        <div className="lg:col-span-2">
          <Card className="rounded-3xl border-none bg-background/60 backdrop-blur-xl shadow-xl">
            <CardContent className="p-8">
              <AnimatePresence mode="wait">
                {!isEditing ? (
                  <motion.div
                    key="view"
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -20 }}
                    transition={{ duration: 0.3 }}
                  >
                    <ProfileCard profile={profile} user={user} />
                  </motion.div>
                ) : (
                  <motion.div
                    key="edit"
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -20 }}
                    transition={{ duration: 0.3 }}
                  >
                    <div className="space-y-6">
                      <div>
                        <h2 className="text-2xl font-bold mb-2">Edit Profile</h2>
                        <p className="text-muted-foreground">Update your personal information</p>
                      </div>
                      <ProfileForm
                        profile={profile}
                        onSave={handleSave}
                        onCancel={handleCancel}
                        loading={loading}
                      />
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </CardContent>
          </Card>
        </div>

        {/* Stats Section */}
        <div className="space-y-6">
          {/* Member Since Card */}
          <Card className="rounded-3xl border-none bg-gradient-to-br from-primary to-indigo-600 text-white shadow-xl">
            <CardContent className="p-6">
              <div className="flex items-center gap-3 mb-4">
                📅
                <h3 className="text-lg font-bold">Member Since</h3>
              </div>
              <p className="text-2xl font-black">
                {user?.created_at
                  ? new Date(user.created_at).toLocaleDateString('en-US', {
                      month: 'short',
                      year: 'numeric',
                    })
                  : '2026'}
              </p>
            </CardContent>
          </Card>

          {/* Stats Cards */}
          <div className="grid grid-cols-1 gap-4">
            <Card className="rounded-3xl border-none shadow-lg hover:scale-105 transition-transform">
              <CardContent className="p-6 text-center">
                <div className="mx-auto p-3 rounded-2xl bg-blue-500/10 w-fit mb-3">
                  🎯
                </div>
                <p className="text-3xl font-black text-foreground">{examHistory?.total || 0}</p>
                <p className="text-sm font-bold text-muted-foreground uppercase tracking-widest">
                  Total Exams
                </p>
              </CardContent>
            </Card>

            <Card className="rounded-3xl border-none shadow-lg hover:scale-105 transition-transform">
              <CardContent className="p-6 text-center">
                <div className="mx-auto p-3 rounded-2xl bg-green-500/10 w-fit mb-3">
                  📖
                </div>
                <p className="text-3xl font-black text-foreground">{journalAnalytics?.total_entries || 0}</p>
                <p className="text-sm font-bold text-muted-foreground uppercase tracking-widest">
                  Journal Entries
                </p>
              </CardContent>
            </Card>

            <Card className="rounded-3xl border-none shadow-lg hover:scale-105 transition-transform">
              <CardContent className="p-6 text-center">
                <div className="mx-auto p-3 rounded-2xl bg-orange-500/10 w-fit mb-3">
                  🏆
                </div>
                <p className="text-3xl font-black text-foreground">{journalAnalytics?.streak_days || 0}</p>
                <p className="text-sm font-bold text-muted-foreground uppercase tracking-widest">
                  Streak Days
                </p>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}
