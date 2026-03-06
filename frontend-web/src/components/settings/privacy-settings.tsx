'use client';

import { useState } from 'react';
import {
  Shield,
  Database,
  Download,
  Trash2,
  AlertTriangle,
  Lock,
  Loader2,
  Calendar,
  BarChart3,
  HardDrive,
} from 'lucide-react';
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
  Button,
  Switch,
  Progress,
  Input,
  Modal,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui';
import { motion, AnimatePresence } from 'framer-motion';

export interface PrivacySettingsProps {
  settings: {
    analyticsSharing: boolean;
    dataRetention: string;
    dataUsageSummary?: {
      totalRecords: number;
      lastExport?: string;
      accountAge: string;
      storageUsed: string;
    };
  };
  onChange: (updatedSettings: Partial<PrivacySettingsProps['settings']>) => void;
  onExportData: () => Promise<void>;
  onDeleteAccount: (password: string) => Promise<void>;
}

/**
 * PrivacySettings Component
 *
 * Provides a premium interface for managing data privacy, analytics,
 * data retention, and account deletion.
 */
export function PremiumPrivacySettings({
  settings,
  onChange,
  onExportData,
  onDeleteAccount,
}: PrivacySettingsProps) {
  const [isExporting, setIsExporting] = useState(false);
  const [exportProgress, setExportProgress] = useState(0);
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [deleteStep, setDeleteStep] = useState(1);
  const [password, setPassword] = useState('');
  const [isDeleting, setIsDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  /**
   * Handles the data export process with progress simulation
   */
  const handleExport = async () => {
    setIsExporting(true);
    setExportProgress(10);

    // Simulate progression for better UX
    const interval = setInterval(() => {
      setExportProgress((prev) => {
        if (prev >= 90) {
          clearInterval(interval);
          return 90;
        }
        return prev + 5;
      });
    }, 400);

    try {
      await onExportData();
      setExportProgress(100);
      setTimeout(() => {
        setIsExporting(false);
        setExportProgress(0);
      }, 1500);
    } catch (err) {
      setError('Failed to export data. Please try again.');
      setIsExporting(false);
    } finally {
      clearInterval(interval);
    }
  };

  /**
   * Handles the account deletion process after multi-step confirmation
   */
  const handleDelete = async () => {
    if (!password) return;
    setIsDeleting(true);
    setError(null);
    try {
      await onDeleteAccount(password);
      // If redirection happens elsewhere, this will just close the modal
      setIsDeleteModalOpen(false);
    } catch (err) {
      setError('Incorrect password or error deleting account.');
      setIsDeleting(false);
      setDeleteStep(1); // Go back to password step
    }
  };

  return (
    <div className="max-w-4xl mx-auto space-y-8 p-1">
      {/* Header Section */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex flex-col gap-2"
      >
        <h2 className="text-3xl font-bold tracking-tight bg-gradient-to-r from-primary to-primary/60 bg-clip-text text-transparent flex items-center gap-2">
          <Shield className="h-8 w-8 text-primary" />
          Privacy & Data Management
        </h2>
        <p className="text-muted-foreground">
          Take control of your personal information and how it&apos;s used across the Soul Sense
          platform.
        </p>
      </motion.div>

      {/* Analytics & Data Usage Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card className="overflow-hidden border-primary/10 hover:border-primary/30 transition-all duration-300 group">
          <CardHeader className="bg-primary/5 group-hover:bg-primary/10 transition-colors">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-primary/10 text-primary group-hover:scale-110 transition-transform">
                <BarChart3 className="h-5 w-5" />
              </div>
              <div>
                <CardTitle className="text-lg">Analytics & Sharing</CardTitle>
                <CardDescription>Improve Soul Sense experience</CardDescription>
              </div>
            </div>
          </CardHeader>
          <CardContent className="pt-6">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <p className="text-sm font-medium">Anonymized Usage Data</p>
                <p className="text-xs text-muted-foreground leading-relaxed">
                  Help us understand how Soul Sense is used by sharing anonymized interaction data.
                  No sensitive results are ever shared.
                </p>
              </div>
              <Switch
                checked={settings.analyticsSharing}
                onCheckedChange={(checked) => onChange({ analyticsSharing: checked })}
              />
            </div>
          </CardContent>
        </Card>

        <Card className="overflow-hidden border-primary/10 hover:border-primary/30 transition-all duration-300 group">
          <CardHeader className="bg-primary/5 group-hover:bg-primary/10 transition-colors">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-primary/10 text-primary group-hover:scale-110 transition-transform">
                <Calendar className="h-5 w-5" />
              </div>
              <div>
                <CardTitle className="text-lg">Data Retention</CardTitle>
                <CardDescription>Automatic cleanup settings</CardDescription>
              </div>
            </div>
          </CardHeader>
          <CardContent className="pt-6">
            <div className="space-y-4">
              <div className="space-y-1">
                <p className="text-sm font-medium">Auto-deletion Period</p>
                <p className="text-xs text-muted-foreground leading-relaxed">
                  Choose how long we should keep your exam history and session data before automatic
                  permanent deletion.
                </p>
              </div>
              <Select
                value={settings.dataRetention}
                onValueChange={(value) => onChange({ dataRetention: value })}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select retention period" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="30_days">30 Days</SelectItem>
                  <SelectItem value="90_days">90 Days</SelectItem>
                  <SelectItem value="1_year">1 Year</SelectItem>
                  <SelectItem value="forever">Indefinite (Keep all history)</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Data Usage Summary Section */}
      {settings.dataUsageSummary && (
        <Card className="border-muted bg-muted/20 backdrop-blur-sm">
          <CardHeader>
            <div className="flex items-center gap-2">
              <Database className="h-5 w-5 text-muted-foreground" />
              <CardTitle className="text-lg">Data Usage Summary</CardTitle>
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              <div className="space-y-1">
                <p className="text-[10px] text-muted-foreground uppercase tracking-widest font-bold">
                  Total Records
                </p>
                <p className="text-xl font-bold font-mono tracking-tight">
                  {settings.dataUsageSummary.totalRecords.toLocaleString()}
                </p>
              </div>
              <div className="space-y-1">
                <p className="text-[10px] text-muted-foreground uppercase tracking-widest font-bold">
                  Storage Used
                </p>
                <p className="text-xl font-bold font-mono tracking-tight">
                  {settings.dataUsageSummary.storageUsed}
                </p>
              </div>
              <div className="space-y-1">
                <p className="text-[10px] text-muted-foreground uppercase tracking-widest font-bold">
                  Account Age
                </p>
                <p className="text-xl font-bold font-mono tracking-tight">
                  {settings.dataUsageSummary.accountAge}
                </p>
              </div>
              <div className="space-y-1">
                <p className="text-[10px] text-muted-foreground uppercase tracking-widest font-bold">
                  Last Export
                </p>
                <p className="text-lg font-bold font-mono tracking-tight text-primary/80">
                  {settings.dataUsageSummary.lastExport || 'Never'}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Portability Card */}
      <Card className="border-primary/10 shadow-sm">
        <CardHeader>
          <CardTitle>Data Portability & Actions</CardTitle>
          <CardDescription>Download your data bundle or manage account status.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="flex flex-col sm:flex-row items-center justify-between gap-4 p-5 rounded-2xl bg-primary/5 border border-primary/20 transition-all hover:bg-primary/[0.08]">
            <div className="flex items-center gap-4">
              <div className="h-14 w-14 rounded-2xl bg-primary/10 flex items-center justify-center text-primary rotate-3 transition-transform hover:rotate-0">
                <Download className="h-7 w-7" />
              </div>
              <div>
                <p className="text-lg font-bold">Export My Data</p>
                <p className="text-sm text-muted-foreground">
                  Receive a secure JSON bundle of all your platform activity.
                </p>
              </div>
            </div>
            <div className="w-full sm:w-auto flex flex-col items-center sm:items-end gap-3">
              <Button
                onClick={handleExport}
                disabled={isExporting}
                className="w-full sm:w-56 bg-primary hover:bg-primary/90 text-primary-foreground shadow-xl shadow-primary/20 h-12 text-base"
              >
                {isExporting ? (
                  <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                ) : (
                  <HardDrive className="mr-2 h-5 w-5" />
                )}
                {isExporting ? 'Exporting Data...' : 'Initiate Export'}
              </Button>
              {isExporting && (
                <div className="w-full sm:w-56 space-y-2">
                  <Progress value={exportProgress} className="h-1.5" />
                  <p className="text-[11px] text-center text-muted-foreground font-medium animate-pulse">
                    Bundling records... {exportProgress}%
                  </p>
                </div>
              )}
            </div>
          </div>
        </CardContent>
        <CardFooter className="bg-destructive/[0.03] border-t border-destructive/10 mt-6 flex flex-col items-start gap-5 p-8">
          <div className="flex items-center gap-2 text-destructive">
            <AlertTriangle className="h-6 w-6" />
            <h4 className="text-lg font-bold uppercase tracking-tight">Danger Zone</h4>
          </div>
          <div className="flex flex-col lg:flex-row items-start lg:items-center justify-between w-full gap-6">
            <div className="text-sm text-muted-foreground max-w-xl leading-relaxed">
              Once you delete your account, there is{' '}
              <span className="text-destructive font-bold underline">no going back</span>. All your
              exam history, profile data, and progress will be{' '}
              <span className="font-bold">permanently removed</span> from our production servers
              within 24 hours.
            </div>
            <Button
              variant="destructive"
              onClick={() => setIsDeleteModalOpen(true)}
              className="w-full lg:w-auto px-10 h-12 text-base shadow-xl shadow-destructive/20 font-bold"
            >
              <Trash2 className="mr-2 h-5 w-5" />
              Delete Account
            </Button>
          </div>
        </CardFooter>
      </Card>

      {/* Multi-step Delete Modal */}
      <Modal
        isOpen={isDeleteModalOpen}
        onClose={() => {
          if (!isDeleting) {
            setIsDeleteModalOpen(false);
            setDeleteStep(1);
            setPassword('');
            setError(null);
          }
        }}
        title="Account Deletion Request"
        size="md"
      >
        <AnimatePresence mode="wait">
          {deleteStep === 1 ? (
            <motion.div
              key="step1"
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="space-y-6"
            >
              <div className="p-5 rounded-xl bg-destructive/10 border border-destructive/20 text-destructive flex items-start gap-4">
                <AlertTriangle className="h-6 w-6 mt-0.5 shrink-0" />
                <div className="space-y-1">
                  <p className="font-bold text-lg">Identity Verification Required</p>
                  <p className="text-sm leading-relaxed opacity-90">
                    To protect your account from accidental or unauthorized deletion, we require
                    your password to continue.
                  </p>
                </div>
              </div>

              <div className="space-y-4">
                <div className="space-y-2">
                  <label className="text-sm font-bold text-muted-foreground uppercase tracking-widest">
                    Confirmation Password
                  </label>
                  <div className="relative group">
                    <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground group-focus-within:text-primary transition-colors" />
                    <Input
                      type="password"
                      placeholder="••••••••"
                      className="pl-10 h-12 bg-muted/30 focus:bg-background transition-colors"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                    />
                  </div>
                  {error && (
                    <motion.p
                      initial={{ opacity: 0, y: -5 }}
                      animate={{ opacity: 1, y: 0 }}
                      className="text-xs text-destructive font-bold flex items-center gap-1 mt-1"
                    >
                      <AlertTriangle className="h-3 w-3" />
                      {error}
                    </motion.p>
                  )}
                </div>
              </div>

              <div className="flex gap-4 pt-4">
                <Button
                  variant="outline"
                  className="flex-1 h-12 font-bold"
                  onClick={() => setIsDeleteModalOpen(false)}
                >
                  Keep Account
                </Button>
                <Button
                  variant="destructive"
                  className="flex-1 h-12 font-bold"
                  disabled={!password || isDeleting}
                  onClick={() => setDeleteStep(2)}
                >
                  Continue
                </Button>
              </div>
            </motion.div>
          ) : (
            <motion.div
              key="step2"
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="space-y-8"
            >
              <div className="text-center space-y-4 py-6">
                <div className="mx-auto h-20 w-20 rounded-full bg-destructive/10 flex items-center justify-center text-destructive animate-pulse shadow-inner shadow-destructive/20">
                  <Trash2 className="h-10 w-10" />
                </div>
                <div className="space-y-2">
                  <h3 className="text-2xl font-black text-destructive tracking-tight">
                    FINAL CONFIRMATION
                  </h3>
                  <p className="text-muted-foreground font-medium max-w-sm mx-auto">
                    This is your absolute last chance. Once you click the button below, the deletion
                    process begins immediately.
                  </p>
                </div>
              </div>

              <div className="flex gap-4">
                <Button
                  variant="outline"
                  className="flex-1 h-12 font-bold"
                  onClick={() => setDeleteStep(1)}
                  disabled={isDeleting}
                >
                  Go Back
                </Button>
                <Button
                  variant="destructive"
                  className="flex-1 h-12 font-bold text-lg"
                  disabled={isDeleting}
                  onClick={handleDelete}
                >
                  {isDeleting ? (
                    <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                  ) : (
                    <AlertTriangle className="mr-2 h-5 w-5" />
                  )}
                  {isDeleting ? 'Processing...' : 'Confirm & Delete'}
                </Button>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </Modal>
    </div>
  );
}
