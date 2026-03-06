/**
 * Validates that a callback URL is internal to the application.
 * Prevents "Open Redirect" vulnerabilities by ensuring the URL starts with /
 * and doesn't contain external protocols or relative protocols.
 */
export function isValidCallbackUrl(url: string | null): boolean {
  if (!url) return false;
  
  // Must start with exactly one '/' to be an internal path
  // Prevents protocol-relative URLs like '//google.com'
  // and explicit protocols like 'https://'
  if (!url.startsWith('/') || url.startsWith('//')) {
    return false;
  }

  // Basic sanity check: no typical protocol patterns later in the string
  // (though startsWith('/') is usually enough for modern browsers)
  const protocolPattern = /^(?:[a-z+.-]+:)?\/\//i;
  if (protocolPattern.test(url)) {
    return false;
  }

  return true;
}
