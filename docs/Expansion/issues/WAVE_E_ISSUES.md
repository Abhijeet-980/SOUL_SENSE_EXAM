# Wave E: Profile & Settings - Issue Details

This document contains detailed GitHub issue descriptions for Wave E of Phase 2: Feature Porting.

**Prerequisite:** Wave A must be completed before starting Wave E. (Wave E can run in parallel with Waves B/C/D)

---

## 📦 Group E.1: Profile System

---

## PORT-041: Create profile API service

### Description

Build a React hook to handle user profile operations with the backend API.

### Requirements

- [ ] Create `frontend-web/src/hooks/useProfile.ts`
- [ ] Endpoints to support:
  - `GET /api/v1/profiles/me` - get current user profile
  - `PUT /api/v1/profiles/me` - update profile
  - `POST /api/v1/profiles/me/avatar` - upload avatar image
  - `DELETE /api/v1/profiles/me/avatar` - remove avatar
- [ ] Return: `profile`, `isLoading`, `error`, `updateProfile`, `uploadAvatar`

### API Response Format

```json
{
  "id": 1,
  "user_id": 1,
  "first_name": "John",
  "last_name": "Doe",
  "bio": "Emotional intelligence enthusiast",
  "age": 28,
  "gender": "male",
  "avatar_url": "/uploads/avatars/user_1.jpg",
  "goals": {
    "short_term": "Improve self-awareness",
    "long_term": "Become emotionally resilient"
  },
  "preferences": {
    "notification_frequency": "daily",
    "theme": "dark"
  },
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

### Acceptance Criteria

- [ ] Can fetch current profile
- [ ] Can update profile fields
- [ ] Avatar upload works
- [ ] Error handling implemented

### Labels

`phase: porting` `wave: E` `group: E.1` `skill: intermediate` `area: api-integration` `type: hook`

---

## PORT-042: Create ProfileCard component

### Description

Build a card component displaying user's profile summary.

### Requirements

- [ ] Create `frontend-web/src/components/profile/profile-card.tsx`
- [ ] Props:
  - `profile` - Profile object
  - `variant` - 'compact' | 'full'
  - `editable` - boolean (show edit button)
  - `onEdit` - callback when edit clicked
- [ ] Display:
  - Avatar image (or initials fallback)
  - Full name
  - Bio (truncated in compact mode)
  - Member since date
  - EQ stats summary (optional: last score, total assessments)
- [ ] Hover state with edit overlay (if editable)

### Design Reference

- Clean, centered layout
- Avatar is prominent
- Subtle shadow/elevation

### Acceptance Criteria

- [ ] Both variants render correctly
- [ ] Avatar fallback works
- [ ] Edit button triggers callback
- [ ] Responsive on mobile

### Labels

`phase: porting` `wave: E` `group: E.1` `skill: beginner` `area: frontend` `type: component`

---

## PORT-043: Create ProfileForm component

### Description

Build an editable form for updating user profile information.

### Requirements

- [ ] Create `frontend-web/src/components/profile/profile-form.tsx`
- [ ] Props:
  - `profile` - initial profile data
  - `onSubmit` - callback with updated data
  - `isSubmitting` - loading state
- [ ] Form fields:
  - Avatar upload (with preview)
  - First name (required)
  - Last name (required)
  - Bio (textarea, 500 char limit)
  - Age (number input)
  - Gender (dropdown)
  - Short-term goals (textarea)
  - Long-term goals (textarea)
- [ ] Validation:
  - Required fields
  - Age range (13-120)
  - Bio character limit

### Technical Details

- Use `react-hook-form` for form handling
- Use `zod` for validation
- Handle image preview before upload

### Acceptance Criteria

- [ ] All fields work correctly
- [ ] Validation errors display
- [ ] Form submits successfully
- [ ] Avatar preview works

### Labels

`phase: porting` `wave: E` `group: E.1` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-044: Create Profile page

### Description

Build the user profile page with view and edit modes.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/profile/page.tsx`
- [ ] Integrate:
  - `useProfile` hook
  - `ProfileCard` component
  - `ProfileForm` component
- [ ] Layout:
  - Header with "Edit Profile" button
  - Profile card (view mode)
  - Profile form (edit mode)
  - Stats section (total exams, journal entries, member since)
- [ ] Features:
  - Toggle between view/edit modes
  - Success toast on save
  - Loading state while fetching

### Design Reference

- Clean, spacious layout
- Edit mode is inline (not a modal)
- Stats displayed as cards or metrics

### Acceptance Criteria

- [ ] Profile loads correctly
- [ ] Edit mode toggle works
- [ ] Updates save successfully
- [ ] View mode shows updated data

### Labels

`phase: porting` `wave: E` `group: E.1` `skill: intermediate` `area: frontend` `type: page`

---

## 📦 Group E.2: Settings System

---

## PORT-045: Create settings API service

### Description

Build a React hook to handle user settings operations.

### Requirements

- [ ] Create `frontend-web/src/hooks/useSettings.ts`
- [ ] Endpoints to support:
  - `GET /api/v1/settings` - get user settings
  - `PUT /api/v1/settings` - update settings
  - `POST /api/v1/settings/sync` - sync local settings to cloud
- [ ] Return: `settings`, `isLoading`, `error`, `updateSettings`, `syncSettings`

### API Response Format

```json
{
  "theme": "dark",
  "notifications": {
    "enabled": true,
    "email": true,
    "push": false,
    "frequency": "daily"
  },
  "privacy": {
    "share_analytics": true,
    "data_retention_days": 365
  },
  "accessibility": {
    "high_contrast": false,
    "reduced_motion": false,
    "font_size": "medium"
  },
  "sync": {
    "enabled": true,
    "last_synced": "2024-01-15T10:30:00Z"
  }
}
```

### Acceptance Criteria

- [ ] Settings load correctly
- [ ] Updates persist
- [ ] Sync functionality works
- [ ] Error handling implemented

### Labels

`phase: porting` `wave: E` `group: E.2` `skill: intermediate` `area: api-integration` `type: hook`

---

## PORT-046: Create ThemeToggle component

### Description

Build a component for switching between dark and light themes.

### Requirements

- [ ] Create `frontend-web/src/components/settings/theme-toggle.tsx`
- [ ] Props:
  - `value` - 'light' | 'dark' | 'system'
  - `onChange` - callback with new value
- [ ] Display:
  - Toggle switch OR segmented control
  - Icons: sun (light), moon (dark), laptop (system)
  - Current theme label
- [ ] Features:
  - Animate transition between themes
  - Persist preference to localStorage
  - Respect system preference when set to 'system'

### Technical Details

- Integrate with Tailwind's dark mode
- Update `document.documentElement` class
- Use `prefers-color-scheme` media query for system

### Acceptance Criteria

- [ ] All three options work
- [ ] Theme applies immediately
- [ ] Persists on refresh
- [ ] System preference respects OS setting

### Labels

`phase: porting` `wave: E` `group: E.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-047: Create NotificationSettings component

### Description

Build a component for managing notification preferences.

### Requirements

- [ ] Create `frontend-web/src/components/settings/notification-settings.tsx`
- [ ] Props:
  - `settings` - notification settings object
  - `onChange` - callback with updated settings
- [ ] Controls:
  - Master toggle: Enable/disable all notifications
  - Email notifications toggle
  - Push notifications toggle (if supported)
  - Frequency selector: 'instant' | 'daily' | 'weekly'
  - Quiet hours: start/end time pickers
- [ ] Display:
  - Section header with icon
  - Description for each setting
  - Visual feedback when disabled

### Design Reference

- Toggle switches with labels
- Disabled state when master is off
- Clean section layout

### Acceptance Criteria

- [ ] All toggles work correctly
- [ ] Frequency selector works
- [ ] Master toggle disables others
- [ ] Changes trigger callback

### Labels

`phase: porting` `wave: E` `group: E.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-048: Create PrivacySettings component

### Description

Build a component for managing privacy and data settings.

### Requirements

- [ ] Create `frontend-web/src/components/settings/privacy-settings.tsx`
- [ ] Props:
  - `settings` - privacy settings object
  - `onChange` - callback with updated settings
  - `onExportData` - callback for data export
  - `onDeleteAccount` - callback for account deletion
- [ ] Controls:
  - Analytics sharing toggle
  - Data retention period selector
  - Export my data button
  - Delete account button (with confirmation)
- [ ] Display:
  - Clear descriptions for each privacy option
  - Warning styling for destructive actions
  - Progress indicator for data export

### Technical Details

- Data export should trigger backend job
- Delete account requires password confirmation
- Show data usage summary if available

### Acceptance Criteria

- [ ] All settings work correctly
- [ ] Export data initiates download
- [ ] Delete account has multi-step confirmation
- [ ] Proper warning UI for destructive actions

### Labels

`phase: porting` `wave: E` `group: E.2` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-049: Create Settings page

### Description

Build the main settings page with tabbed sections.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/settings/page.tsx`
- [ ] Integrate:
  - `useSettings` hook
  - `ThemeToggle` component
  - `NotificationSettings` component
  - `PrivacySettings` component
- [ ] Tabs/Sections:
  1. **Appearance** - theme, font size, high contrast
  2. **Notifications** - email, push, frequency
  3. **Privacy & Data** - analytics, retention, export, delete
  4. **Account** - change password, connected accounts
  5. **About** - app version, credits, open source licenses
- [ ] Features:
  - Auto-save on change (with debounce)
  - Success/error feedback
  - URL hash for direct tab links (#notifications)

### Design Reference

- Sidebar tabs on desktop, top tabs on mobile
- Each section clearly separated
- Destructive actions at bottom with warnings

### Acceptance Criteria

- [ ] All tabs/sections render
- [ ] Settings save automatically
- [ ] Tab navigation works
- [ ] URL hash updates correctly

### Labels

`phase: porting` `wave: E` `group: E.2` `skill: intermediate` `area: frontend` `type: page`

---

## 📊 Wave E Summary

| Issue    | Title                          | Skill           | Group |
| -------- | ------------------------------ | --------------- | ----- |
| PORT-041 | Profile API service            | 🟡 Intermediate | E.1   |
| PORT-042 | ProfileCard component          | 🟢 Beginner     | E.1   |
| PORT-043 | ProfileForm component          | 🟡 Intermediate | E.1   |
| PORT-044 | Profile page                   | 🟡 Intermediate | E.1   |
| PORT-045 | Settings API service           | 🟡 Intermediate | E.2   |
| PORT-046 | ThemeToggle component          | 🟢 Beginner     | E.2   |
| PORT-047 | NotificationSettings component | 🟢 Beginner     | E.2   |
| PORT-048 | PrivacySettings component      | 🟡 Intermediate | E.2   |
| PORT-049 | Settings page                  | 🟡 Intermediate | E.2   |

**Total: 9 issues** (3 Beginner, 6 Intermediate)

---

## 🔗 Dependencies

```
Group E.1 (Profile)         Group E.2 (Settings)
      ↓                           ↓
 Profile Page              Settings Page
```

- **E.1** and **E.2** can be worked on **in parallel**
- No dependencies between groups
- **Wave E can run in parallel with Waves B, C, and D** (all depend on Wave A only)
