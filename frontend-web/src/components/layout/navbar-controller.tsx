'use client';

import React from 'react';
import { usePathname } from 'next/navigation';
import { FloatingNavbar } from './floating-navbar';

/**
 * Conditionally renders the floating navbar based on pathname.
 * We hide it on authentication-related pages to prevent visual overlap
 * and redundant "Sign In" CTA on the login/register flows.
 */
export function NavbarController() {
  const pathname = usePathname();

  const hideOnRoutes = new Set(['/login', '/register', '/forgot-password']);
  if (hideOnRoutes.has(pathname)) {
    return null;
  }

  return <FloatingNavbar />;
}
