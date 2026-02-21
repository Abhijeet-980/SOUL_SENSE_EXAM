import { WifiOff } from 'lucide-react';
import Link from 'next/link';

export default function OfflinePage() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-background to-muted p-4">
      <div className="max-w-md w-full text-center space-y-6">
        <div className="flex justify-center">
          <div className="p-6 rounded-full bg-muted">
            <WifiOff className="w-16 h-16 text-muted-foreground" />
          </div>
        </div>

        <div className="space-y-2">
          <h1 className="text-3xl font-bold tracking-tight">
            You&apos;re Offline
          </h1>
          <p className="text-muted-foreground">
            No internet connection detected. Some features may be unavailable.
          </p>
        </div>

        <div className="bg-card rounded-lg p-6 border shadow-sm">
          <h2 className="font-semibold mb-3">What you can do offline:</h2>
          <ul className="text-left space-y-2 text-sm text-muted-foreground">
            <li>View your past assessment results</li>
            <li>Read your journal entries</li>
            <li>Write new journal entries (will sync when online)</li>
            <li>Update your profile information</li>
            <li>View emotional insights and trends</li>
          </ul>
        </div>

        <div className="bg-card rounded-lg p-6 border shadow-sm">
          <h2 className="font-semibold mb-3">What requires internet:</h2>
          <ul className="text-left space-y-2 text-sm text-muted-foreground">
            <li>Taking new EQ assessments</li>
            <li>Community features</li>
            <li>Saving data to the cloud</li>
          </ul>
        </div>

        <div className="flex flex-col gap-3">
          <p className="text-sm text-muted-foreground">
            Your changes will be automatically synced when you reconnect.
          </p>
          <Link
            href="/"
            className="inline-flex items-center justify-center rounded-md bg-primary px-8 py-3 text-sm font-medium text-primary-foreground shadow transition-colors hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
          >
            Return to Home
          </Link>
        </div>
      </div>
    </div>
  );
}
