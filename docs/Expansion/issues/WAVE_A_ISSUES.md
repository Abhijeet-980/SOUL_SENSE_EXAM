# Wave A: Core UI Foundation - Issue Details

This document contains detailed GitHub issue descriptions for Wave A of Phase 2: Feature Porting.

---

## 📦 Group A.1: App Shell & Layout

---

## PORT-001: Create authenticated layout wrapper

### Description

Build the authenticated app layout wrapper that protects all app routes and provides the base structure for the logged-in experience.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/layout.tsx`
- [ ] Check authentication status on mount
- [ ] Redirect to `/login` if user is not authenticated
- [ ] Wrap children with sidebar and header components (placeholders if not yet created)
- [ ] Include a loading state while auth check is in progress

### Technical Details

- Use the existing auth context/hook if available, or create a simple `useAuth` hook
- The `(app)` folder uses Next.js route groups - everything inside requires auth

### Acceptance Criteria

- [ ] Unauthenticated users are redirected to `/login`
- [ ] Authenticated users see the app shell
- [ ] Loading spinner shows during auth check

### Labels

`phase: porting` `wave: A` `group: A.1` `skill: beginner` `area: frontend` `type: page`

---

## PORT-002: Create sidebar navigation component

### Description

Build a responsive sidebar navigation component for the authenticated app area. This will be the primary navigation for desktop users.

### Requirements

- [ ] Create `frontend-web/src/components/app/sidebar.tsx`
- [ ] Include navigation links:
  - Dashboard (home icon)
  - Take Exam (clipboard icon)
  - Journal (book icon)
  - Results (chart icon)
  - Profile (user icon)
  - Settings (gear icon)
- [ ] Highlight the active route
- [ ] Collapsible on desktop (icon-only mode)
- [ ] Hidden on mobile (mobile uses bottom nav instead)

### Design Reference

- Use existing Tailwind classes from the project
- Follow the dark/light theme support
- Icons: Use `lucide-react` (already installed)

### Acceptance Criteria

- [ ] All 6 navigation links are present
- [ ] Active link is visually distinct
- [ ] Sidebar collapses to icons on button click
- [ ] Responsive: hidden on screens < 768px

### Labels

`phase: porting` `wave: A` `group: A.1` `skill: beginner` `area: frontend` `type: component`

---

## PORT-003: Create mobile bottom navigation

### Description

Build a mobile-responsive bottom navigation bar that appears on small screens. This replaces the sidebar for mobile users.

### Requirements

- [ ] Create `frontend-web/src/components/app/mobile-nav.tsx`
- [ ] Fixed to bottom of screen
- [ ] Include 5 main navigation items:
  - Dashboard
  - Exam
  - Journal (center, larger)
  - Results
  - Profile
- [ ] Highlight active route
- [ ] Only visible on screens < 768px

### Design Reference

- Modern app-style bottom tab bar
- Center item can be elevated/larger (like "+" button in many apps)
- Use `lucide-react` icons

### Acceptance Criteria

- [ ] Navigation bar is fixed to bottom
- [ ] 5 icons are visible and tappable
- [ ] Active state is visually clear
- [ ] Hidden on desktop (>= 768px)

### Labels

`phase: porting` `wave: A` `group: A.1` `skill: beginner` `area: frontend` `type: component`

---

## PORT-004: Create app header component

### Description

Build the top header component for the authenticated app area. This shows user info and quick actions.

### Requirements

- [ ] Create `frontend-web/src/components/app/header.tsx`
- [ ] Left side: Logo or app name (can link to dashboard)
- [ ] Right side:
  - Notifications icon (bell) with badge count
  - User avatar dropdown:
    - Profile link
    - Settings link
    - Logout button
- [ ] Sticky at top of page

### Technical Details

- Avatar should show user initials if no image
- Dropdown menu for user actions
- Logout should clear auth state and redirect to `/login`

### Acceptance Criteria

- [ ] Header is sticky at top
- [ ] User avatar and dropdown work correctly
- [ ] Logout clears session and redirects
- [ ] Notifications icon is visible (functionality can be placeholder)

### Labels

`phase: porting` `wave: A` `group: A.1` `skill: beginner` `area: frontend` `type: component`

---

## 📦 Group A.2: Shared UI Components

---

## PORT-005: Create Card component

### Description

Build a reusable Card component that will be used throughout the app for containing content.

### Requirements

- [ ] Create `frontend-web/src/components/ui/card.tsx`
- [ ] Support variants:
  - `default` - subtle border, no shadow
  - `elevated` - shadow for depth
  - `outlined` - stronger border, no fill
- [ ] Accept `className` prop for customization
- [ ] Export sub-components: `CardHeader`, `CardContent`, `CardFooter`

### Example Usage

```tsx
<Card variant="elevated">
  <CardHeader>
    <h2>Title</h2>
  </CardHeader>
  <CardContent>
    <p>Content goes here</p>
  </CardContent>
</Card>
```

### Acceptance Criteria

- [ ] All 3 variants render correctly
- [ ] Sub-components work as expected
- [ ] Supports dark/light mode

### Labels

`phase: porting` `wave: A` `group: A.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-006: Create Progress bar component

### Description

Build a reusable progress bar component for showing completion percentages (exam progress, profile completion, etc.).

### Requirements

- [ ] Create `frontend-web/src/components/ui/progress.tsx`
- [ ] Props:
  - `value` (number 0-100)
  - `showLabel` (boolean, optional)
  - `size` ('sm' | 'md' | 'lg')
  - `color` ('primary' | 'success' | 'warning' | 'danger')
- [ ] Smooth animation on value change

### Example Usage

```tsx
<Progress value={75} showLabel size="md" color="primary" />
```

### Acceptance Criteria

- [ ] Progress fills correctly based on value
- [ ] Label shows percentage when enabled
- [ ] Animates smoothly on changes
- [ ] All color variants work

### Labels

`phase: porting` `wave: A` `group: A.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-007: Create Slider component

### Description

Build a range slider component for mood ratings, settings, and other numeric inputs.

### Requirements

- [ ] Create `frontend-web/src/components/ui/slider.tsx`
- [ ] Props:
  - `min`, `max`, `step`
  - `value`, `onChange`
  - `label` (optional)
  - `showValue` (boolean)
- [ ] Accessible: keyboard navigable, screen reader labels
- [ ] Touch-friendly for mobile

### Example Usage

```tsx
<Slider
  min={1}
  max={10}
  value={mood}
  onChange={setMood}
  label="How are you feeling?"
  showValue
/>
```

### Acceptance Criteria

- [ ] Slider moves smoothly
- [ ] Value updates correctly
- [ ] Works with keyboard (arrow keys)
- [ ] Touch works on mobile

### Labels

`phase: porting` `wave: A` `group: A.2` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-008: Create Modal/Dialog component

### Description

Build an accessible modal dialog component for confirmations, forms, and detailed views.

### Requirements

- [ ] Create `frontend-web/src/components/ui/modal.tsx`
- [ ] Props:
  - `isOpen`, `onClose`
  - `title` (optional)
  - `size` ('sm' | 'md' | 'lg' | 'full')
- [ ] Features:
  - Backdrop click to close (configurable)
  - Close button (X)
  - Escape key to close
  - Focus trap inside modal
  - Prevent body scroll when open

### Example Usage

```tsx
<Modal
  isOpen={showModal}
  onClose={() => setShowModal(false)}
  title="Confirm Action"
>
  <p>Are you sure?</p>
  <Button onClick={handleConfirm}>Yes</Button>
</Modal>
```

### Acceptance Criteria

- [ ] Opens and closes correctly
- [ ] Backdrop and escape work
- [ ] Focus is trapped inside
- [ ] Accessible (role="dialog", aria-labels)

### Labels

`phase: porting` `wave: A` `group: A.2` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-009: Create Toast/Notification component

### Description

Build a toast notification system for showing success, error, and info messages.

### Requirements

- [ ] Create `frontend-web/src/components/ui/toast.tsx`
- [ ] Create a toast context/hook: `useToast`
- [ ] Toast types: `success`, `error`, `warning`, `info`
- [ ] Auto-dismiss after configurable duration (default 5s)
- [ ] Stackable (multiple toasts can show)
- [ ] Position: top-right (configurable)

### Example Usage

```tsx
const { toast } = useToast();

toast({
  type: "success",
  message: "Entry saved successfully!",
});
```

### Acceptance Criteria

- [ ] Toasts appear and auto-dismiss
- [ ] Multiple toasts stack
- [ ] All 4 types have distinct styling
- [ ] Can be manually dismissed

### Labels

`phase: porting` `wave: A` `group: A.2` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-010: Create Loading skeleton components

### Description

Build skeleton loader components for displaying loading states across the app.

### Requirements

- [ ] Create `frontend-web/src/components/ui/skeleton.tsx`
- [ ] Export variants:
  - `Skeleton` - base rectangular skeleton
  - `SkeletonText` - text line skeleton
  - `SkeletonAvatar` - circular skeleton
  - `SkeletonCard` - full card skeleton
- [ ] Animated shimmer effect
- [ ] Configurable width/height

### Example Usage

```tsx
// Single line
<Skeleton className="h-4 w-48" />

// Card placeholder
<SkeletonCard />

// Avatar
<SkeletonAvatar size="lg" />
```

### Acceptance Criteria

- [ ] All variants render correctly
- [ ] Shimmer animation works
- [ ] Matches app's color scheme (dark/light)

### Labels

`phase: porting` `wave: A` `group: A.2` `skill: beginner` `area: frontend` `type: component`

---

## 📊 Wave A Summary

| Issue    | Title                        | Skill           | Group |
| -------- | ---------------------------- | --------------- | ----- |
| PORT-001 | Authenticated layout wrapper | 🟢 Beginner     | A.1   |
| PORT-002 | Sidebar navigation           | 🟢 Beginner     | A.1   |
| PORT-003 | Mobile bottom navigation     | 🟢 Beginner     | A.1   |
| PORT-004 | App header                   | 🟢 Beginner     | A.1   |
| PORT-005 | Card component               | 🟢 Beginner     | A.2   |
| PORT-006 | Progress bar                 | 🟢 Beginner     | A.2   |
| PORT-007 | Slider component             | 🟡 Intermediate | A.2   |
| PORT-008 | Modal/Dialog                 | 🟡 Intermediate | A.2   |
| PORT-009 | Toast notifications          | 🟡 Intermediate | A.2   |
| PORT-010 | Skeleton loaders             | 🟢 Beginner     | A.2   |

**Total: 10 issues** (7 Beginner, 3 Intermediate)
