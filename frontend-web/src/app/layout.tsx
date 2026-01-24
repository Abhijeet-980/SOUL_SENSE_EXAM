import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';

const inter = Inter({
  subsets: ['latin'],
  variable: '--font-inter',
});

export const metadata: Metadata = {
  title: 'Soul Sense - Emotional Intelligence Assessment',
  description:
    'Comprehensive EQ testing platform for personal growth and emotional intelligence development. Take the scientifically-backed assessment to understand your emotional strengths and areas for improvement.',
  keywords: [
    'emotional intelligence',
    'EQ test',
    'personality assessment',
    'emotional awareness',
    'self-improvement',
  ],
  authors: [{ name: 'SoulSense Team' }],
  creator: 'SoulSense',
  publisher: 'SoulSense',
  openGraph: {
    title: 'Soul Sense - EQ Assessment Platform',
    description:
      'Discover your emotional intelligence potential with our comprehensive assessment platform.',
    type: 'website',
    locale: 'en_US',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Soul Sense - EQ Assessment',
    description:
      'Comprehensive emotional intelligence testing for personal growth.',
  },
  robots: {
    index: true,
    follow: true,
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.variable} font-sans antialiased`}>
        {children}
      </body>
    </html>
  );
}
