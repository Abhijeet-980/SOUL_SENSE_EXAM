import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import '@/styles/globals.css';
import { ThemeProvider, NavbarController } from '@/components/layout';
import { ToastProvider } from '@/components/ui';
import { NetworkErrorBanner } from '@/components/common';
import { AuthProvider } from '@/hooks/useAuth';
import { WebVitalsMonitor } from '@/components/monitoring/WebVitalsMonitor';

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
  viewport: {
    width: 'device-width',
    initialScale: 1,
    maximumScale: 5,
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
          <ToastProvider>
            <AuthProvider>
              <NetworkErrorBanner />
              <NavbarController />
              {children}
            </AuthProvider>
          </ToastProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
