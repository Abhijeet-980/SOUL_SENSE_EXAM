import type { Metadata, Viewport } from 'next';
import { Inter } from 'next/font/google';
import '@/styles/globals.css';
import { ThemeProvider, NavbarController } from '@/components/layout';
import { ToastProvider } from '@/components/ui';
import { NetworkErrorBanner } from '@/components/common';
import { AuthProvider } from '@/hooks/useAuth';
import { SkipLinks } from '@/components/accessibility';

const inter = Inter({ subsets: ['latin'], variable: '--font-sans' });

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
  authors: [{ name: 'Soul Sense' }],
  creator: 'Soul Sense',
  publisher: 'Soul Sense',
  formatDetection: {
    email: false,
    address: false,
    telephone: false,
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
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={`${inter.variable} font-sans antialiased`}>
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
        >
          <SkipLinks />
          <ToastProvider>
            <AuthProvider>
              <NetworkErrorBanner />
              <NavbarController />
              <div id="main-content" role="main" tabIndex={-1}>
                {children}
              </div>
            </AuthProvider>
          </ToastProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
