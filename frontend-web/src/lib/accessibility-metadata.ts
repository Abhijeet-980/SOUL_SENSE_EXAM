/**
 * Accessibility Metadata Helper
 *
 * Utility for generating consistent, accessible metadata across pages
 */

import { Metadata } from 'next';

interface PageMetadataOptions {
  title: string;
  description: string;
  path: string;
  image?: string;
  noindex?: boolean;
}

/**
 * Generate accessible metadata for a page
 */
export function generateAccessibleMetadata(options: PageMetadataOptions): Metadata {
  const {
    title,
    description,
    path,
    image = '/og-default.png',
    noindex = false,
  } = options;

  const fullTitle = `${title} | Soul Sense`;
  const url = `https://soulsense.io${path}`;

  return {
    title: fullTitle,
    description,
    keywords: generateKeywords(title),
    authors: [{ name: 'Soul Sense' }],
    creator: 'Soul Sense',
    publisher: 'Soul Sense',

    openGraph: {
      type: 'website',
      locale: 'en_US',
      url,
      title: fullTitle,
      description,
      siteName: 'Soul Sense',
      images: [
        {
          url: image,
          width: 1200,
          height: 630,
          alt: title,
        },
      ],
    },

    twitter: {
      card: 'summary_large_image',
      title: fullTitle,
      description,
      images: [image],
    },

    robots: {
      index: !noindex,
      follow: !noindex,
      googleBot: {
        index: !noindex,
        follow: !noindex,
        'max-video-preview': -1,
        'max-image-preview': 'large',
        'max-snippet': -1,
      },
    },

    icons: {
      icon: '/favicon.ico',
      apple: '/apple-touch-icon.png',
    },

    metadataBase: new URL('https://soulsense.io'),
    alternates: {
      canonical: url,
    },
  };
}

/**
 * Generate relevant keywords from page title
 */
function generateKeywords(title: string): string[] {
  const baseKeywords = [
    'Emotional Intelligence',
    'EQ Test',
    'Self-Awareness',
    'Mental Health',
    'Personal Growth',
    'AI Assessment',
  ];

  const titleWords = title
    .toLowerCase()
    .split(/\s+/)
    .filter((word) => word.length > 3)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1));

  return [...baseKeywords, ...titleWords];
}

/**
 * Page-specific metadata configurations
 */
export const pageMetadata = {
  home: generateAccessibleMetadata({
    title: 'AI-Powered Emotional Intelligence Test',
    description:
      'Discover your emotional intelligence with Soul Sense. Get comprehensive EQ assessment, AI-powered insights, and personalized growth recommendations.',
    path: '/',
  }),

  dashboard: generateAccessibleMetadata({
    title: 'Dashboard',
    description:
      'Your personal emotional intelligence dashboard. Track your EQ progress, view insights, and access assessments and journaling tools.',
    path: '/dashboard',
  }),

  exam: generateAccessibleMetadata({
    title: 'EQ Assessment',
    description:
      'Take our comprehensive emotional intelligence assessment. Get immediate results and AI-powered insights to understand your emotional strengths.',
    path: '/exam',
  }),

  journal: generateAccessibleMetadata({
    title: 'Emotional Journal',
    description:
      'Daily journaling with AI-powered sentiment analysis. Track your emotional patterns and receive personalized insights for better self-awareness.',
    path: '/journal',
  }),

  results: generateAccessibleMetadata({
    title: 'Assessment Results',
    description:
      'View your emotional intelligence assessment results with detailed breakdowns, recommendations, and historical progress tracking.',
    path: '/results',
  }),

  profile: generateAccessibleMetadata({
    title: 'Profile Settings',
    description:
      'Manage your personal information, preferences, and privacy settings. Customize your Soul Sense experience.',
    path: '/profile',
  }),

  settings: generateAccessibleMetadata({
    title: 'Settings',
    description:
      'Configure your Soul Sense experience. Manage account settings, data preferences, and accessibility options.',
    path: '/settings',
  }),

  login: generateAccessibleMetadata({
    title: 'Sign In',
    description: 'Sign in to your Soul Sense account to access your EQ assessments and journaling tools.',
    path: '/login',
  }),

  register: generateAccessibleMetadata({
    title: 'Create Account',
    description:
      'Create your free Soul Sense account to start your emotional intelligence journey and personalized growth tracking.',
    path: '/register',
  }),
};

/**
 * Accessibility preferences schema
 */
export const accessibilityPreferences = {
  reducedMotion: {
    key: 'prefers-reduced-motion',
    label: 'Reduce Motion',
    description: 'Minimize animations and transitions',
    values: ['no-preference', 'reduce'],
  },

  highContrast: {
    key: 'prefers-contrast',
    label: 'High Contrast',
    description: 'Increase contrast for better visibility',
    values: ['no-preference', 'high', 'low'],
  },

  textSize: {
    key: 'text-size',
    label: 'Text Size',
    description: 'Adjust text size for better readability',
    values: ['small', 'medium', 'large', 'extra-large'],
  },

  screenReader: {
    key: 'screen-reader-optimization',
    label: 'Screen Reader Optimization',
    description: 'Optimize content for screen readers',
    values: ['default', 'enhanced'],
  },

  keyboardNavigation: {
    key: 'keyboard-navigation',
    label: 'Keyboard Navigation',
    description: 'Enhance keyboard navigation features',
    values: ['standard', 'enhanced'],
  },
} as const;

/**
 * WCAG compliance level descriptions
 */
export const wcagLevels = {
  A: {
    name: 'Level A',
    description: 'Minimum level of accessibility',
    requirements: [
      'Text alternatives for non-text content',
      'Video captions',
      'Audio content alternatives',
      'Keyboard accessibility',
      'No content that causes seizures',
    ],
  },

  AA: {
    name: 'Level AA',
    description: 'Recommended level of accessibility',
    requirements: [
      'Color contrast ratio of 4.5:1 for normal text',
      'Color contrast ratio of 3:1 for large text',
      'Text resizable to 200%',
      'No keyboard traps',
      'Focus indicators visible',
      'Error identification and suggestions',
    ],
  },

  AAA: {
    name: 'Level AAA',
    description: 'Highest level of accessibility',
    requirements: [
      'Color contrast ratio of 7:1 for normal text',
      'Color contrast ratio of 4.5:1 for large text',
      'Sign language interpretation for video',
      'Extended audio descriptions',
      'No time limits on user input',
    ],
  },
} as const;

/**
 * ARIA role descriptions for screen readers
 */
export const ariaRoles = {
  navigation: {
    role: 'navigation',
    description: 'Main navigation menu',
    label: 'Navigation',
  },
  main: {
    role: 'main',
    description: 'Main content area',
    label: 'Main Content',
  },
  complementary: {
    role: 'complementary',
    description: 'Supplementary content',
    label: 'Sidebar',
  },
  banner: {
    role: 'banner',
    description: 'Site header',
    label: 'Header',
  },
  contentinfo: {
    role: 'contentinfo',
    description: 'Site footer',
    label: 'Footer',
  },
  form: {
    role: 'form',
    description: 'Input form',
    label: 'Form',
  },
  search: {
    role: 'search',
    description: 'Search functionality',
    label: 'Search',
  },
  alert: {
    role: 'alert',
    description: 'Important message',
    label: 'Alert',
  },
  status: {
    role: 'status',
    description: 'Status update',
    label: 'Status',
  },
  dialog: {
    role: 'dialog',
    description: 'Modal dialog',
    label: 'Dialog',
  },
  alertdialog: {
    role: 'alertdialog',
    description: 'Alert dialog requiring attention',
    label: 'Alert Dialog',
  },
} as const;

/**
 * Common ARIA label templates
 */
export const ariaLabels = {
  close: 'Close',
  menu: 'Menu',
  search: 'Search',
  previous: 'Previous',
  next: 'Next',
  submit: 'Submit form',
  cancel: 'Cancel',
  confirm: 'Confirm',
  save: 'Save changes',
  delete: 'Delete',
  edit: 'Edit',
  loading: 'Loading content',
  error: 'Error',
  success: 'Success',
  warning: 'Warning',
  info: 'Information',
  expand: 'Expand section',
  collapse: 'Collapse section',
  select: 'Select option',
  selected: 'Selected',
  notSelected: 'Not selected',
  required: 'Required field',
  optional: 'Optional field',
  invalid: 'Invalid input',
  valid: 'Valid input',
} as const;
