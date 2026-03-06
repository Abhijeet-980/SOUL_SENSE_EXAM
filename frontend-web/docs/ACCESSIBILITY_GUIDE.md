# Accessibility Guide (WCAG 2.1 AA Compliance)

## Overview

Soul Sense is committed to ensuring digital accessibility for all users, including those with disabilities. We follow WCAG 2.1 Level AA guidelines to make our platform usable by everyone.

## Table of Contents

1. [Our Commitment](#our-commitment)
2. [Accessibility Features](#accessibility-features)
3. [Keyboard Navigation](#keyboard-navigation)
4. [Screen Reader Support](#screen-reader-support)
5. [Visual Accessibility](#visual-accessibility)
6. [Forms and Inputs](#forms-and-inputs)
7. [Testing Guidelines](#testing-guidelines)
8. [Developer Guidelines](#developer-guidelines)
9. [Known Issues](#known-issues)

---

## Our Commitment

We strive to provide an accessible experience that allows all users to:

- Navigate the application using keyboard only
- Understand content with screen readers
- Perceive information regardless of color vision
- Interact with all features using assistive technologies

## Accessibility Features

### 1. Keyboard Navigation

All interactive elements are keyboard accessible:

- **Tab**: Move focus between interactive elements
- **Shift+Tab**: Move focus backward
- **Enter/Space**: Activate buttons and links
- **Escape**: Close modals and dropdowns
- **Arrow Keys**: Navigate within menus and lists

#### Focus Indicators

Visible focus indicators show which element has keyboard focus:

- 2px ring outline in primary color
- High contrast against backgrounds
- Consistent across all interactive elements

### 2. Screen Reader Support

The application uses semantic HTML and ARIA attributes:

#### Landmarks

- `role="navigation"` for navigation areas
- `role="main"` for main content
- `role="banner"` for headers
- `role="contentinfo"` for footers

#### Live Regions

Dynamic content updates are announced:

- `role="status"` for non-critical updates
- `role="alert"` for important messages
- `aria-live="polite"` for most updates
- `aria-live="assertive"` for urgent notifications

#### Labels

All interactive elements have accessible labels:

- Form inputs with associated `<label>` elements
- Buttons with descriptive text
- Links with meaningful destination descriptions
- Icons with `aria-label` or `aria-hidden` attributes

### 3. Visual Accessibility

#### Color Contrast

All text meets WCAG AA standards:

- Normal text: Minimum 4.5:1 contrast ratio
- Large text (18pt+): Minimum 3:1 contrast ratio
- UI components: Minimum 3:1 contrast ratio

#### Color Independence

Information is never conveyed by color alone:

- Error messages use text + icons + color
- Form validation includes text descriptions
- Charts use patterns + colors

#### Text Sizing

Text is resizable up to 200% without breaking layout:

- Responsive units (rem, em, %)
- No horizontal scrolling at 320px width
- Text containers adapt to content

#### Reduced Motion

Respects user's motion preferences:

- `prefers-reduced-motion` media query support
- Optional animations can be disabled
- Essential animations remain but are simplified

### 4. Forms and Inputs

#### Labels

All form fields have proper labels:

```tsx
<FormLabel htmlFor="email" required={true}>
  Email Address
</FormLabel>
<Input
  id="email"
  name="email"
  type="email"
  aria-required="true"
  aria-describedby="email-description"
/>
<FormMessage
  name="email"
  message="Enter your email address"
  type="description"
/>
```

#### Error Handling

Errors are announced to screen readers:

```tsx
<Input
  aria-invalid={!!error}
  aria-describedby={error ? "email-error" : undefined}
/>
{error && (
  <FormError
    error={error.message}
    className="text-destructive"
  />
)}
```

#### Validation

- Real-time validation with clear messages
- Error summaries for multiple issues
- Suggestions for fixing errors
- Preservation of entered data

### 5. Skip Links

Skip links allow keyboard users to bypass repeated content:

```tsx
<SkipLinks>
  <a href="#main-content">Skip to main content</a>
  <a href="#navigation">Skip to navigation</a>
</SkipLinks>
```

## Testing Guidelines

### Automated Testing

Run accessibility audits regularly:

```bash
# Using axe-core
npm run test:a11y

# Using Lighthouse CI
npm run test:lighthouse
```

### Manual Testing Checklist

#### Keyboard Navigation

- [ ] Can navigate entire site without mouse
- [ ] Tab order is logical
- [ ] Focus indicators are visible
- [ ] Escape closes modals/menus
- [ ] Arrow keys work in menus

#### Screen Reader Testing

Test with multiple screen readers:

- **NVDA** (Windows, Firefox)
- **JAWS** (Windows, Chrome/Edge)
- **VoiceOver** (macOS, Safari)
- **TalkBack** (Android, Chrome)

Verify:

- [ ] All content is readable
- [ ] Landmarks are announced
- [ ] Form labels are read
- [ ] Errors are announced
- [ ] Dynamic updates are announced

#### Visual Accessibility

- [ ] Color contrast meets WCAG AA
- [ ] Text zooms to 200% without issues
- [ ] High contrast mode works
- [ ] Reduced motion is respected
- [ ] Information isn't color-dependent

### Testing Tools

1. **axe DevTools** - Browser extension for accessibility testing
2. **WAVE** - Visual accessibility feedback
3. **Lighthouse** - Integrated Chrome accessibility audit
4. **NVDA/JAWS** - Screen reader testing
5. **Keyboard-only** - Navigation without mouse

## Developer Guidelines

### Component Accessibility

All new components must be accessible:

```tsx
// Good - Accessible Button
<button
  type="button"
  onClick={handleClick}
  aria-label="Close dialog"
  aria-pressed={isPressed}
>
  <X aria-hidden="true" />
  <span className="sr-only">Close</span>
</button>

// Bad - Not Accessible
<div onClick={handleClick}>
  <X />
</div>
```

### ARIA Attributes

Use semantic HTML first, ARIA second:

```tsx
// Prefer semantic HTML
<button>Submit</button>

// If needed, add ARIA
<div role="button" tabIndex={0} aria-label="Submit">
  Submit
</div>
```

### Focus Management

Manage focus appropriately:

```tsx
const previousFocus = usePreviousFocus();

// When opening modal
const dialogRef = useRef<HTMLDivElement>(null);
useEffect(() => {
  dialogRef.current?.focus();
}, []);

// When closing modal
useEffect(() => {
  return () => {
    previousFocus.current?.focus();
  };
}, []);
```

### Live Regions

Announce dynamic changes:

```tsx
import { LiveRegion } from '@/components/accessibility';

<LiveRegion
  message="Assessment submitted successfully"
  role="status"
  politeness="polite"
/>
```

## WCAG 2.1 AA Compliance Status

### Perceivable

| Criterion | Status | Notes |
|-----------|--------|-------|
| Text alternatives | ✅ | All images have alt text |
| Captions | ⚠️ | Video content limited |
| Audio description | ⚠️ | Video content limited |
| Color contrast | ✅ | Meets AA standards |
| Text resize | ✅ | Supports 200% zoom |
| Contrast in UI | ✅ | Forms and controls meet standards |

### Operable

| Criterion | Status | Notes |
|-----------|--------|-------|
| Keyboard accessible | ✅ | Full keyboard support |
| No keyboard traps | ✅ | Focus management implemented |
| Timing adjustable | ✅ | No time limits on assessments |
| Pause/stop controls | ✅ | Animations can be paused |
| Navigation mechanisms | ✅ | Skip links, breadcrumbs, landmarks |

### Understandable

| Criterion | Status | Notes |
|-----------|--------|-------|
| Language of page | ✅ | `lang="en"` attribute set |
| On focus | ✅ | No context changes on focus |
| On input | ✅ | Clear labels and instructions |
| Error identification | ✅ | Descriptive error messages |
| Error suggestion | ✅ | Guidance for fixing errors |

### Robust

| Criterion | Status | Notes |
|-----------|--------|-------|
| Compatible | ✅ | Valid HTML, ARIA attributes |
| Name/Role/Value | ✅ | All elements properly defined |

## Best Practices

### 1. Semantic HTML

```tsx
// Good
<nav aria-label="Main">
  <ul>
    <li><a href="/dashboard">Dashboard</a></li>
  </ul>
</nav>

// Avoid
<div class="nav">
  <div class="nav-item" onclick="goToDashboard()">Dashboard</div>
</div>
```

### 2. Alt Text

```tsx
// Informative alt text
<img src="/chart.png" alt="Bar chart showing emotional intelligence trends over 6 months" />

// Decorative images
<img src="/decoration.svg" alt="" role="presentation" />

// Icons with meaning
<Heart aria-label="Favorite" filled={isFavorite} />
<Heart aria-hidden="true" />  // Decorative
```

### 3. Form Accessibility

```tsx
// Always associate labels
<label htmlFor="email">Email</label>
<input id="email" name="email" type="email" required />

// Or use aria-label
<input
  type="search"
  placeholder="Search..."
  aria-label="Search assessments"
/>

// Group related fields
<fieldset>
  <legend>Personal Information</legend>
  <input id="name" name="name" />
  <input id="email" name="email" />
</fieldset>
```

### 4. Modal Accessibility

```tsx
<Modal
  open={isOpen}
  onClose={() => setIsOpen(false)}
  aria-labelledby="modal-title"
  aria-describedby="modal-description"
>
  <h2 id="modal-title">Confirm Action</h2>
  <p id="modal-description">Are you sure you want to proceed?</p>
  <Button onClick={confirm}>Confirm</Button>
  <Button onClick={cancel}>Cancel</Button>
</Modal>
```

## Known Issues and Roadmap

### Current Status (WCAG 2.1 AA)

- ✅ Core navigation and keyboard access
- ✅ Form labels and error handling
- ✅ Screen reader support for main features
- ✅ Color contrast compliance
- ✅ Skip links and landmarks

### In Progress

- ⚠️ Video captioning (limited video content)
- ⚠️ Advanced chart accessibility
- ⚠️ Mobile gesture alternatives

### Planned Improvements

1. Enhanced chart data tables
2. Voice control compatibility
3. High contrast mode theme
4. Font dyslexia-friendly option
5. More comprehensive ARIA testing

## Resources

- [WCAG 2.1 Guidelines](https://www.w3.org/WAI/WCAG21/quickref/)
- [ARIA Authoring Practices](https://www.w3.org/WAI/ARIA/apg/)
- [WebAIM Accessibility Checklist](https://webaim.org/standards/wcag/checklist)
- [ axe-core Testing Guide](https://www.deque.com/axe/)
- [Radix UI Accessibility](https://www.radix-ui.com/primitives/docs/overview/accessibility)

## Reporting Issues

If you encounter accessibility barriers:

1. Check known issues above
2. Test with different assistive technologies
3. Report via GitHub Issues with label `accessibility`
4. Include steps to reproduce and assistive tech used

## Contact

For accessibility-specific inquiries:
- GitHub: https://github.com/nupurmadaan04/SOUL_SENSE_EXAM/issues
- Label: `a11y` or `accessibility`

---

**Last Updated**: February 2026
**Version**: 1.0.0
**WCAG Level**: AA (Target)
