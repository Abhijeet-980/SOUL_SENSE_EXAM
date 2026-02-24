/**
 * Accessibility Utilities
 *
 * Helper functions for ensuring WCAG 2.1 AA compliance
 */

/**
 * Check if element has sufficient color contrast
 * @param foreground Foreground color (hex)
 * @param background Background color (hex)
 * @param isLargeText Whether the text is large (18pt+ or 14pt+ bold)
 * @returns true if contrast meets WCAG AA standards
 */
export function hasSufficientContrast(
  foreground: string,
  background: string,
  isLargeText: boolean = false
): boolean {
  const contrastRatio = calculateContrastRatio(foreground, background);
  return isLargeText ? contrastRatio >= 3.0 : contrastRatio >= 4.5;
}

/**
 * Calculate contrast ratio between two colors
 * @param color1 First color (hex)
 * @param color2 Second color (hex)
 * @returns Contrast ratio (1-21)
 */
export function calculateContrastRatio(color1: string, color2: string): number {
  const lum1 = getLuminance(color1);
  const lum2 = getLuminance(color2);
  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Get relative luminance of a color
 * @param hex Color in hex format
 * @returns Relative luminance (0-1)
 */
function getLuminance(hex: string): number {
  const rgb = hexToRgb(hex);
  if (!rgb) return 0;

  const [r, g, b] = [rgb.r, rgb.g, rgb.b].map((val) => {
    val = val / 255;
    return val <= 0.03928 ? val / 12.92 : Math.pow((val + 0.055) / 1.055, 2.4);
  });

  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

/**
 * Convert hex to RGB
 * @param hex Hex color string
 * @returns RGB object or null
 */
function hexToRgb(hex: string): { r: number; g: number; b: number } | null {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result
    ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16),
      }
    : null;
}

/**
 * Check if user prefers reduced motion
 * @returns true if user prefers reduced motion
 */
export function prefersReducedMotion(): boolean {
  if (typeof window === 'undefined') return false;
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/**
 * Check if user prefers high contrast
 * @returns true if user prefers high contrast
 */
export function prefersHighContrast(): boolean {
  if (typeof window === 'undefined') return false;
  return window.matchMedia('(prefers-contrast: high)').matches;
}

/**
 * Generate unique ID for accessibility attributes
 * @param prefix Prefix for the ID
 * @returns Unique ID string
 */
export function generateAccessibleId(prefix: string): string {
  return `${prefix}-${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Announce message to screen readers
 * @param message Message to announce
 * @param politeness 'polite' or 'assertive'
 */
export function announceToScreenReader(message: string, politeness: 'polite' | 'assertive' = 'polite'): void {
  const announcement = document.createElement('div');
  announcement.setAttribute('role', 'status');
  announcement.setAttribute('aria-live', politeness);
  announcement.setAttribute('aria-atomic', 'true');
  announcement.className = 'sr-only';
  announcement.textContent = message;

  document.body.appendChild(announcement);

  setTimeout(() => {
    document.body.removeChild(announcement);
  }, 1000);
}

/**
 * Trap focus within a container
 * @param container Element to trap focus within
 * @returns Cleanup function
 */
export function trapFocus(container: HTMLElement): () => void {
  const focusableElements = container.querySelectorAll<HTMLElement>(
    'a[href], button:not([disabled]), textarea:not([disabled]), input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"]), [contenteditable="true"]'
  );

  const firstElement = focusableElements[0];
  const lastElement = focusableElements[focusableElements.length - 1];

  const handleTabKey = (e: KeyboardEvent) => {
    if (e.key !== 'Tab') return;

    if (e.shiftKey) {
      if (document.activeElement === firstElement) {
        e.preventDefault();
        lastElement?.focus();
      }
    } else {
      if (document.activeElement === lastElement) {
        e.preventDefault();
        firstElement?.focus();
      }
    }
  };

  container.addEventListener('keydown', handleTabKey);
  firstElement?.focus();

  return () => {
    container.removeEventListener('keydown', handleTabKey);
  };
}

/**
 * Check if element is focusable
 * @param element Element to check
 * @returns true if element is focusable
 */
export function isFocusable(element: HTMLElement): boolean {
  if (
    element.tabIndex > 0 ||
    (element.tabIndex === 0 && element.getAttribute('tabIndex') !== null)
  ) {
    return true;
  }

  if ('disabled' in element && (element as any).disabled) return false;

  const focusableTags = ['BUTTON', 'INPUT', 'SELECT', 'TEXTAREA', 'A'];
  if (focusableTags.includes(element.tagName)) {
    return true;
  }

  return false;
}

/**
 * Get all focusable elements within a container
 * @param container Container element
 * @returns Array of focusable elements
 */
export function getFocusableElements(container: HTMLElement): HTMLElement[] {
  const focusableSelectors = [
    'a[href]',
    'button:not([disabled])',
    'textarea:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
    '[contenteditable="true"]',
  ].join(', ');

  return Array.from(container.querySelectorAll<HTMLElement>(focusableSelectors));
}

/**
 * Set focus to first focusable element in container
 * @param container Container element
 * @returns true if focus was set successfully
 */
export function focusFirstElement(container: HTMLElement): boolean {
  const focusableElements = getFocusableElements(container);
  if (focusableElements.length > 0) {
    focusableElements[0].focus();
    return true;
  }
  return false;
}

/**
 * Create accessible label from icon and text
 * @param icon Icon element or name
 * @param text Text content
 * @returns Accessible label string
 */
export function createAccessibleLabel(icon: string | React.ReactNode, text: string): string {
  if (typeof icon === 'string') {
    return `${icon}: ${text}`;
  }
  return text;
}

/**
 * Validate ARIA attributes
 * @param element Element to validate
 * @returns Object with validation results
 */
export function validateAriaAttributes(element: HTMLElement): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];

  // Check for invalid ARIA attributes
  const invalidAria = Array.from(element.attributes).filter((attr) =>
    attr.name.startsWith('aria-') && !isValidAriaAttribute(attr.name)
  );

  if (invalidAria.length > 0) {
    errors.push(`Invalid ARIA attributes: ${invalidAria.map((a) => a.name).join(', ')}`);
  }

  // Check for required attributes based on role
  const role = element.getAttribute('role');
  if (role) {
    const required = getRequiredAriaAttributes(role);
    const missing = required.filter((attr) => !element.hasAttribute(attr));
    if (missing.length > 0) {
      errors.push(`Missing required ARIA attributes for role "${role}": ${missing.join(', ')}`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Check if ARIA attribute is valid
 * @param attr Attribute name
 * @returns true if valid
 */
function isValidAriaAttribute(attr: string): boolean {
  const validAttributes = [
    'aria-activedescendant',
    'aria-atomic',
    'aria-autocomplete',
    'aria-busy',
    'aria-checked',
    'aria-colcount',
    'aria-colindex',
    'aria-colspan',
    'aria-controls',
    'aria-current',
    'aria-describedby',
    'aria-details',
    'aria-disabled',
    'aria-dropeffect',
    'aria-errormessage',
    'aria-expanded',
    'aria-flowto',
    'aria-grabbed',
    'aria-haspopup',
    'aria-hidden',
    'aria-invalid',
    'aria-keyshortcuts',
    'aria-label',
    'aria-labelledby',
    'aria-level',
    'aria-live',
    'aria-modal',
    'aria-multiline',
    'aria-multiselectable',
    'aria-orientation',
    'aria-owns',
    'aria-placeholder',
    'aria-posinset',
    'aria-pressed',
    'aria-readonly',
    'aria-relevant',
    'aria-required',
    'aria-roledescription',
    'aria-rowcount',
    'aria-rowindex',
    'aria-rowspan',
    'aria-selected',
    'aria-setsize',
    'aria-sort',
    'aria-valuemax',
    'aria-valuemin',
    'aria-valuenow',
    'aria-valuetext',
  ];
  return validAttributes.includes(attr);
}

/**
 * Get required ARIA attributes for a role
 * @param role ARIA role
 * @returns Array of required attribute names
 */
function getRequiredAriaAttributes(role: string): string[] {
  const requiredAttributes: Record<string, string[]> = {
    dialog: ['aria-labelledby'],
    alertdialog: ['aria-labelledby', 'aria-describedby'],
    button: [],
    link: [],
    textbox: ['aria-label'],
    checkbox: ['aria-checked'],
    radio: ['aria-checked'],
    combobox: ['aria-expanded'],
    menu: ['aria-labelledby'],
    menuitem: [],
    tab: ['aria-selected'],
    tablist: ['aria-label'],
   tabpanel: ['aria-labelledby'],
  };

  return requiredAttributes[role] || [];
}

/**
 * Check if page has proper heading structure
 * @returns Object with validation results
 */
export function validateHeadingStructure(): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];
  const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');

  if (headings.length === 0) {
    errors.push('No headings found on page');
  }

  let previousLevel = 0;
  headings.forEach((heading) => {
    const level = parseInt(heading.tagName[1]);
    if (level - previousLevel > 1) {
      errors.push(`Heading level skipped from ${previousLevel} to ${level}`);
    }
    previousLevel = level;
  });

  return {
    valid: errors.length === 0,
    errors,
  };
}
