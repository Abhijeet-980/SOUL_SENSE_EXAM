import type { Metadata, Viewport } from 'next';
import { Inter } from 'next/font/google';
import '@/styles/globals.css';
import { ThemeProvider, NavbarController, BottomNavigation } from '@/components/layout';
import { ToastProvider } from '@/components/ui';
import { NetworkErrorBanner } from '@/components/common';
import { AuthProvider } from '@/hooks/useAuth';
import { WebVitalsMonitor } from '@/components/monitoring';
import { SkipLinks } from '@/components/accessibility';
import { OfflineBanner } from '@/components/offline';
import { register } from '@/lib/offline';

const inter = Inter({ subsets: ['latin'], variable: '--font-sans', display: 'swap' });

export const metadata: Metadata = {
  title: 'Soul Sense | AI-Powered Emotional Intelligence Test',
  description:
    'Discover your emotional intelligence with Soul Sense. Get deep insights into your EQ, build better relationships, and unlock your full potential using our AI-powered analysis.',
  keywords: [
    'EQ Test',
    'Emotional Intelligence',
    'AI Assessment',
    'Self-Awareness',
    'Professional Growth',
  ],
  authors: [{ name: 'Soul Sense Team' }],
  authors: [{ name: 'Soul Sense' }],
  creator: 'Soul Sense',
  publisher: 'Soul Sense',
  formatDetection: {
    email: false,
    address: false,
    telephone: false,
  },
  metadataBase: new URL(process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3005'),
  openGraph: {
    type: 'website',
    locale: 'en_US',
    url: '/',
    title: 'Soul Sense | AI-Powered Emotional Intelligence Test',
    description: 'Discover your emotional intelligence with Soul Sense',
    siteName: 'Soul Sense',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Soul Sense | AI-Powered Emotional Intelligence Test',
    description: 'Discover your emotional intelligence with Soul Sense',
  },
  robots: {
    index: true,
    follow: true,
  },
};

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  maximumScale: 5,
  userScalable: true,
  themeColor: [
    { media: '(prefers-color-scheme: light)', color: '#ffffff' },
    { media: '(prefers-color-scheme: dark)', color: '#0a0a0a' },
  ],
  colorScheme: 'light dark',
  manifest: '/manifest.json',
  themeColor: '#8b5cf6',
  viewport: {
    width: 'device-width',
    initialScale: 1,
    maximumScale: 1,
  },
  appleWebApp: {
    capable: true,
    statusBarStyle: 'default',
    title: 'Soul Sense',
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={`${inter.variable} font-sans antialiased`}>
        <WebVitalsMonitor />
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
        >
          <SkipLinks />
          <ToastProvider>
            <AuthProvider>
              <OfflineBanner />
              <NetworkErrorBanner />
              <NavbarController />
              <div id="main-content" role="main" tabIndex={-1}>
                {children}
              </div>
              <BottomNavigation />
            </AuthProvider>
          </ToastProvider>
        </ThemeProvider>
        <script
          dangerouslySetInnerHTML={{
            __html: `
              if (typeof window !== 'undefined') {
                ${register.toString()}
                register();
              }
            `,
          }}
        />
      </body>
    </html>
  );
}
