'use client';

import { useState } from 'react';
import { PersonalProfile, UpdatePersonalProfile } from '@/lib/api/profile';
import { Button } from '@/components/ui';
import { Input } from '@/components/ui';
import { Label } from '@/components/ui';

interface ProfileFormProps {
  profile: PersonalProfile | null;
  onSave: (data: UpdatePersonalProfile) => Promise<void>;
  onCancel: () => void;
  loading?: boolean;
}

export function ProfileForm({ profile, onSave, onCancel, loading = false }: ProfileFormProps) {
  const [formData, setFormData] = useState<UpdatePersonalProfile>({
    first_name: profile?.first_name || '',
    last_name: profile?.last_name || '',
    age: profile?.age || undefined,
    gender: profile?.gender || '',
    occupation: profile?.occupation || '',
    education_level: profile?.education_level || '',
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    await onSave(formData);
  };

  const handleInputChange = (field: keyof UpdatePersonalProfile, value: string | number | undefined) => {
    setFormData(prev => ({
      ...prev,
      [field]: value,
    }));
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="first_name">First Name</Label>
          <Input
            id="first_name"
            type="text"
            value={formData.first_name || ''}
            onChange={(e) => handleInputChange('first_name', e.target.value)}
            placeholder="Enter your first name"
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="last_name">Last Name</Label>
          <Input
            id="last_name"
            type="text"
            value={formData.last_name || ''}
            onChange={(e) => handleInputChange('last_name', e.target.value)}
            placeholder="Enter your last name"
          />
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="age">Age</Label>
          <Input
            id="age"
            type="number"
            min="1"
            max="120"
            value={formData.age || ''}
            onChange={(e) => {
              const value = e.target.value;
              handleInputChange('age', value ? parseInt(value) : undefined);
            }}
            placeholder="Enter your age"
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="gender">Gender</Label>
          <select
            id="gender"
            value={formData.gender || ''}
            onChange={(e) => handleInputChange('gender', e.target.value)}
            className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <option value="">Select gender</option>
            <option value="male">Male</option>
            <option value="female">Female</option>
            <option value="other">Other</option>
            <option value="prefer-not-to-say">Prefer not to say</option>
          </select>
        </div>
      </div>

      <div className="space-y-2">
        <Label htmlFor="occupation">Occupation</Label>
        <Input
          id="occupation"
          type="text"
          value={formData.occupation || ''}
          onChange={(e) => handleInputChange('occupation', e.target.value)}
          placeholder="Enter your occupation"
        />
      </div>

      <div className="space-y-2">
        <Label htmlFor="education_level">Education Level</Label>
        <select
          id="education_level"
          value={formData.education_level || ''}
          onChange={(e) => handleInputChange('education_level', e.target.value)}
          className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
        >
          <option value="">Select education level</option>
          <option value="high-school">High School</option>
          <option value="associate">Associate Degree</option>
          <option value="bachelor">Bachelor's Degree</option>
          <option value="master">Master's Degree</option>
          <option value="doctorate">Doctorate</option>
          <option value="other">Other</option>
        </select>
      </div>

      <div className="flex gap-3 pt-4">
        <Button type="submit" disabled={loading} className="flex-1">
          {loading ? (
            <>⏳ Saving...</>
          ) : (
            <>💾 Save Changes</>
          )}
        </Button>

        <Button type="button" variant="outline" onClick={onCancel} disabled={loading}>
          ❌ Cancel
        </Button>
      </div>
    </form>
  );
}