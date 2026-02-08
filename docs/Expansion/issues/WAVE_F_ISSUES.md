# Wave F: Dashboard & Home - Issue Details

This document contains detailed GitHub issue descriptions for Wave F of Phase 2: Feature Porting.

**Prerequisite:** Waves B, C, D, and E should be mostly complete before starting Wave F (Dashboard aggregates data from all features).

---

## 📦 Group F.1: Dashboard Components

---

## PORT-050: Create WelcomeCard component

### Description

Build a personalized greeting card for the dashboard header.

### Requirements

- [ ] Create `frontend-web/src/components/dashboard/welcome-card.tsx`
- [ ] Props:
  - `userName` - user's first name
  - `lastActivity` - date of last app usage (optional)
- [ ] Display:
  - Time-based greeting: "Good morning/afternoon/evening, {name}!"
  - Motivational sub-message (rotating)
  - Optional: days since last activity, streak count
- [ ] Examples:
  - "Good morning, John! Ready to check in today?"
  - "Welcome back, Sarah! You've been journaling for 5 days straight!"

### Design Reference

- Large, warm typography
- Subtle gradient or image background
- Can include a small illustration

### Acceptance Criteria

- [ ] Greeting changes based on time of day
- [ ] User name displays correctly
- [ ] Message varies (not always same)
- [ ] Looks welcoming and personal

### Labels

`phase: porting` `wave: F` `group: F.1` `skill: beginner` `area: frontend` `type: component`

---

## PORT-051: Create QuickActions component

### Description

Build a grid of quick action buttons for common tasks.

### Requirements

- [ ] Create `frontend-web/src/components/dashboard/quick-actions.tsx`
- [ ] Actions to include:
  - 📝 **New Journal Entry** - navigates to `/journal/new`
  - 📊 **Take Assessment** - navigates to `/exam`
  - 📈 **View Results** - navigates to `/results`
  - 👤 **Update Profile** - navigates to `/profile`
- [ ] Display:
  - Grid of cards/buttons (2x2 on mobile, 4x1 on desktop)
  - Icon + label for each action
  - Hover/tap animation
- [ ] Optional: show count/status (e.g., "3 new insights")

### Design Reference

- Large touch targets
- Vibrant but consistent colors
- Icons from `lucide-react`

### Acceptance Criteria

- [ ] All 4 actions display
- [ ] Navigation works correctly
- [ ] Responsive grid layout
- [ ] Accessible (keyboard nav, focus states)

### Labels

`phase: porting` `wave: F` `group: F.1` `skill: beginner` `area: frontend` `type: component`

---

## PORT-052: Create RecentActivity component

### Description

Build a timeline/list showing recent exams and journal entries.

### Requirements

- [ ] Create `frontend-web/src/components/dashboard/recent-activity.tsx`
- [ ] Props:
  - `activities` - array of recent items (exams + journals mixed)
  - `limit` - max items to show (default 5)
- [ ] Display:
  - Icon indicating type (exam vs journal)
  - Title/summary
  - Relative timestamp ("2 hours ago", "Yesterday")
  - Click to navigate to detail
- [ ] Empty state: "No recent activity. Start by taking an assessment!"

### Technical Details

- Fetch from combined endpoint or merge client-side
- Sort by date descending
- Use relative time formatting (date-fns or similar)

### Acceptance Criteria

- [ ] Both exam and journal items display
- [ ] Timestamps are relative
- [ ] Click navigates correctly
- [ ] Empty state shows for new users

### Labels

`phase: porting` `wave: F` `group: F.1` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-053: Create MoodWidget component

### Description

Build a compact widget showing today's mood status with quick-add option.

### Requirements

- [ ] Create `frontend-web/src/components/dashboard/mood-widget.tsx`
- [ ] Display states:
  - **Not logged today**: "How are you feeling?" with quick mood selector
  - **Already logged**: Today's mood with emoji and "View Journal" link
- [ ] Quick mood selector:
  - 5 emoji options (😢 😕 😐 🙂 😄)
  - Click to quick-log mood (optional: opens full journal form)
- [ ] Show mini trend (last 7 days as small sparkline or dots)

### Design Reference

- Compact card design
- Emoji should be large and inviting
- Encourage daily check-in

### Acceptance Criteria

- [ ] Correct state shows based on today's data
- [ ] Quick mood selection works
- [ ] Mini trend displays if data exists
- [ ] Smooth transitions between states

### Labels

`phase: porting` `wave: F` `group: F.1` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-054: Create InsightCard component

### Description

Build a card for displaying AI-generated insights and tips.

### Requirements

- [ ] Create `frontend-web/src/components/dashboard/insight-card.tsx`
- [ ] Props:
  - `insight` - insight object with title, content, type
  - `onDismiss` - optional callback to dismiss
  - `onAction` - optional callback for CTA
- [ ] Types of insights:
  - 💡 **Tip** - general EQ advice
  - 📊 **Pattern** - detected from user data
  - 🎯 **Goal** - progress toward user goals
  - ⚠️ **Alert** - important notification
- [ ] Display:
  - Icon based on type
  - Title (bold)
  - Content (1-3 sentences)
  - Optional CTA button
  - Dismiss button (X)

### Design Reference

- Card with type-based accent color
- Concise, actionable content
- Non-intrusive but noticeable

### Acceptance Criteria

- [ ] All insight types render with correct styling
- [ ] Dismiss button works
- [ ] CTA button triggers action
- [ ] Looks professional and trustworthy

### Labels

`phase: porting` `wave: F` `group: F.1` `skill: beginner` `area: frontend` `type: component`

---

## 📦 Group F.2: Dashboard Page

---

## PORT-055: Create Dashboard page

### Description

Build the main dashboard page composing all dashboard widgets.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/dashboard/page.tsx`
- [ ] Integrate all components:
  - `WelcomeCard`
  - `QuickActions`
  - `MoodWidget`
  - `RecentActivity`
  - `InsightCard` (multiple)
- [ ] Layout (responsive):
  - **Desktop**: 2-column or 3-column grid
  - **Mobile**: single column, stacked
- [ ] Data fetching:
  - Profile (for name)
  - Recent exams
  - Recent journals
  - Today's mood (if logged)
  - AI insights

### Design Reference

- Bento-box style grid layout
- Cards of varying sizes
- Welcoming, informative overview
- Similar to modern app dashboards (Notion, Linear, etc.)

### Acceptance Criteria

- [ ] All widgets display correctly
- [ ] Data loads from APIs
- [ ] Responsive on all screen sizes
- [ ] Loading states for each section
- [ ] Error boundaries for failed sections

### Labels

`phase: porting` `wave: F` `group: F.2` `skill: intermediate` `area: frontend` `type: page`

---

## PORT-056: Create Dashboard loading state

### Description

Build skeleton loading states for the dashboard page.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/dashboard/loading.tsx`
- [ ] Show skeleton placeholders for:
  - Welcome card (text skeleton)
  - Quick actions (4 skeleton buttons)
  - Mood widget (skeleton circle + lines)
  - Recent activity (skeleton list)
  - Insight cards (skeleton cards)
- [ ] Match exact layout of actual dashboard
- [ ] Animate with shimmer effect

### Technical Details

- Use `Skeleton` components from Wave A (PORT-010)
- Matches the dashboard layout
- Next.js will auto-use this during page load

### Acceptance Criteria

- [ ] Skeleton layout matches actual dashboard
- [ ] Shimmer animation works
- [ ] Displayed during initial load
- [ ] Smooth transition to real content

### Labels

`phase: porting` `wave: F` `group: F.2` `skill: beginner` `area: frontend` `type: page`

---

## 📊 Wave F Summary

| Issue    | Title                    | Skill           | Group |
| -------- | ------------------------ | --------------- | ----- |
| PORT-050 | WelcomeCard component    | 🟢 Beginner     | F.1   |
| PORT-051 | QuickActions component   | 🟢 Beginner     | F.1   |
| PORT-052 | RecentActivity component | 🟡 Intermediate | F.1   |
| PORT-053 | MoodWidget component     | 🟡 Intermediate | F.1   |
| PORT-054 | InsightCard component    | 🟢 Beginner     | F.1   |
| PORT-055 | Dashboard page           | 🟡 Intermediate | F.2   |
| PORT-056 | Dashboard loading state  | 🟢 Beginner     | F.2   |

**Total: 7 issues** (4 Beginner, 3 Intermediate)

---

## 🔗 Dependencies

```
Waves B, C, D, E (Features)
           ↓
    Group F.1 (Widgets)
           ↓
    Group F.2 (Page)
```

- **Wave F should be started last** as it aggregates all features
- **F.1** can be started once most of B/C/D/E are complete
- **F.2** depends on F.1 widgets

---

## 🎉 Phase 2 Complete!

After Wave F, the core feature porting is complete. The web app will have:

- ✅ Authentication (already done)
- ✅ EQ Assessments (Wave B)
- ✅ Results & Analytics (Wave C)
- ✅ Journal System (Wave D)
- ✅ Profile & Settings (Wave E)
- ✅ Dashboard Home (Wave F)

**Total Phase 2 Issues: 56**
