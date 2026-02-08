# Phase 2: Feature Porting - Complete Contributor Roadmap

This document outlines **Phase 2: Feature Porting** for the Soul Sense project. The goal is to port core features from the Python/Tkinter desktop app (`app/`) to the Next.js/Tauri web app (`frontend-web/`).

---

## 📋 Project Context

### What We're Building

**Soul Sense** is an emotional intelligence assessment platform that helps users:

- Take EQ (Emotional Quotient) assessments
- Journal their daily thoughts and moods
- Track emotional trends over time
- Get personalized insights and recommendations

### Current Architecture

| Layer              | Technology            | Status                         |
| ------------------ | --------------------- | ------------------------------ |
| **Desktop App**    | Python + Tkinter      | ✅ Feature Complete            |
| **Web Frontend**   | Next.js 15 + Tailwind | 🟡 Auth Done, Features Pending |
| **Desktop Bridge** | Tauri (Rust)          | ✅ Setup Complete              |
| **Backend API**    | FastAPI + PostgreSQL  | ✅ Most Endpoints Ready        |

### What Needs Porting

The desktop app has these core features that need web equivalents:

| Feature                 | Desktop File           | Web Status      | Priority    |
| ----------------------- | ---------------------- | --------------- | ----------- |
| **EQ Assessment/Exam**  | `app/ui/exam.py`       | ❌ Not Started  | 🔴 Critical |
| **Journal System**      | `app/ui/journal.py`    | ❌ Not Started  | 🔴 Critical |
| **Results Dashboard**   | `app/ui/results.py`    | ❌ Not Started  | 🔴 Critical |
| **User Profile**        | `app/ui/profile.py`    | ❌ Not Started  | 🟡 High     |
| **Settings**            | `app/ui/settings.py`   | ❌ Not Started  | 🟡 High     |
| **Analytics Dashboard** | `app/ui/dashboard.py`  | 🟡 Shell Exists | 🟢 Medium   |
| **Daily View**          | `app/ui/daily_view.py` | ❌ Not Started  | 🟢 Medium   |

---

## 🌊 WAVE STRUCTURE

Issues are organized into **Waves** (major phases) and **Groups** (parallel work sets).

### Release Strategy

1. Release one **Group** at a time
2. Multiple contributors can work on issues within the same Group **without conflicts**
3. Wait for all issues in a Group to merge before releasing the next Group
4. Each Wave builds on the previous one

### Skill Levels

- 🟢 **Beginner**: HTML/CSS/React basics, simple components
- 🟡 **Intermediate**: API integration, state management, complex UI
- 🔴 **Advanced**: Architecture decisions, complex business logic

---

## 🌊 WAVE A: Core UI Foundation

**Goal**: Build the authenticated app shell and navigation before feature work.

### 📦 Group A.1: App Shell & Layout

_These issues create the foundation. Release first._

| Issue #    | Title                               | Description                                                                        | Skill       | Files to Create/Modify                           |
| ---------- | ----------------------------------- | ---------------------------------------------------------------------------------- | ----------- | ------------------------------------------------ |
| `PORT-001` | Create authenticated layout wrapper | Build `(app)/layout.tsx` with auth check, redirect to login if not authenticated   | 🟢 Beginner | `frontend-web/src/app/(app)/layout.tsx`          |
| `PORT-002` | Create sidebar navigation component | Build responsive sidebar with links to Dashboard, Exam, Journal, Profile, Settings | 🟢 Beginner | `frontend-web/src/components/app/sidebar.tsx`    |
| `PORT-003` | Create mobile bottom navigation     | Build mobile-responsive bottom nav bar (visible on small screens)                  | 🟢 Beginner | `frontend-web/src/components/app/mobile-nav.tsx` |
| `PORT-004` | Create app header component         | Build header with user avatar, notifications icon, and logout                      | 🟢 Beginner | `frontend-web/src/components/app/header.tsx`     |

### 📦 Group A.2: Shared UI Components

_These are reusable components needed by feature pages. Can be done in parallel with A.1._

| Issue #    | Title                               | Description                                               | Skill           | Files to Create/Modify                        |
| ---------- | ----------------------------------- | --------------------------------------------------------- | --------------- | --------------------------------------------- |
| `PORT-005` | Create Card component               | Reusable card with variants (default, elevated, outlined) | 🟢 Beginner     | `frontend-web/src/components/ui/card.tsx`     |
| `PORT-006` | Create Progress bar component       | Animated progress bar with percentage display             | 🟢 Beginner     | `frontend-web/src/components/ui/progress.tsx` |
| `PORT-007` | Create Slider component             | Range slider with labels (for mood ratings, etc.)         | 🟡 Intermediate | `frontend-web/src/components/ui/slider.tsx`   |
| `PORT-008` | Create Modal/Dialog component       | Accessible modal with close button, backdrop              | 🟡 Intermediate | `frontend-web/src/components/ui/modal.tsx`    |
| `PORT-009` | Create Toast/Notification component | Toast notifications for success/error/info messages       | 🟡 Intermediate | `frontend-web/src/components/ui/toast.tsx`    |
| `PORT-010` | Create Loading skeleton components  | Skeleton loaders for cards, text, avatars                 | 🟢 Beginner     | `frontend-web/src/components/ui/skeleton.tsx` |

---

## 🌊 WAVE B: EQ Assessment System

**Goal**: Port the core exam/assessment functionality.

### 📦 Group B.1: Assessment API & State

_Backend integration layer. Release after Wave A._

| Issue #    | Title                          | Description                                                               | Skill           | Files to Create/Modify                    |
| ---------- | ------------------------------ | ------------------------------------------------------------------------- | --------------- | ----------------------------------------- |
| `PORT-011` | Create questions API service   | Build `useQuestions` hook to fetch questions from `/api/v1/questions`     | 🟡 Intermediate | `frontend-web/src/hooks/useQuestions.ts`  |
| `PORT-012` | Create exam submission service | Build `useExamSubmit` hook to POST answers to `/api/v1/exams`             | 🟡 Intermediate | `frontend-web/src/hooks/useExamSubmit.ts` |
| `PORT-013` | Create exam state store        | Zustand/Context store for exam progress (current question, answers, time) | 🟡 Intermediate | `frontend-web/src/stores/examStore.ts`    |

### 📦 Group B.2: Assessment UI Components

_Can be done in parallel with B.1._

| Issue #    | Title                           | Description                                               | Skill           | Files to Create/Modify                                 |
| ---------- | ------------------------------- | --------------------------------------------------------- | --------------- | ------------------------------------------------------ |
| `PORT-014` | Create QuestionCard component   | Display question text with 5 radio options (Likert scale) | 🟢 Beginner     | `frontend-web/src/components/exam/question-card.tsx`   |
| `PORT-015` | Create ExamProgress component   | Show progress bar, question number (e.g., "5 of 20")      | 🟢 Beginner     | `frontend-web/src/components/exam/exam-progress.tsx`   |
| `PORT-016` | Create ExamNavigation component | Previous/Next/Submit buttons with keyboard support        | 🟢 Beginner     | `frontend-web/src/components/exam/exam-navigation.tsx` |
| `PORT-017` | Create ExamTimer component      | Optional countdown timer (if timed exams are enabled)     | 🟡 Intermediate | `frontend-web/src/components/exam/exam-timer.tsx`      |

### 📦 Group B.3: Assessment Pages

_Depends on B.1 and B.2. Release after both complete._

| Issue #    | Title                     | Description                                          | Skill           | Files to Create/Modify                                |
| ---------- | ------------------------- | ---------------------------------------------------- | --------------- | ----------------------------------------------------- |
| `PORT-018` | Create Exam Start page    | Landing page with exam instructions, Start button    | 🟢 Beginner     | `frontend-web/src/app/(app)/exam/page.tsx`            |
| `PORT-019` | Create Exam Question page | Main exam flow - render questions, handle navigation | 🟡 Intermediate | `frontend-web/src/app/(app)/exam/[id]/page.tsx`       |
| `PORT-020` | Create Reflection screen  | Post-exam reflection prompt (optional text input)    | 🟢 Beginner     | `frontend-web/src/app/(app)/exam/reflection/page.tsx` |
| `PORT-021` | Create Exam Complete page | Success screen with "View Results" CTA               | 🟢 Beginner     | `frontend-web/src/app/(app)/exam/complete/page.tsx`   |

---

## 🌊 WAVE C: Results & Analytics

**Goal**: Display assessment results and emotional trends.

### 📦 Group C.1: Results API & Types

| Issue #    | Title                          | Description                                                        | Skill           | Files to Create/Modify                 |
| ---------- | ------------------------------ | ------------------------------------------------------------------ | --------------- | -------------------------------------- |
| `PORT-022` | Create results API service     | Build `useResults` hook to fetch from `/api/v1/exams/{id}/results` | 🟡 Intermediate | `frontend-web/src/hooks/useResults.ts` |
| `PORT-023` | Define result TypeScript types | Create interfaces for ExamResult, CategoryScore, Recommendation    | 🟢 Beginner     | `frontend-web/src/types/results.ts`    |

### 📦 Group C.2: Result Visualization Components

| Issue #    | Title                               | Description                                                          | Skill           | Files to Create/Modify                                        |
| ---------- | ----------------------------------- | -------------------------------------------------------------------- | --------------- | ------------------------------------------------------------- |
| `PORT-024` | Create ScoreGauge component         | Circular gauge showing overall EQ score (0-100)                      | 🟡 Intermediate | `frontend-web/src/components/results/score-gauge.tsx`         |
| `PORT-025` | Create CategoryBreakdown component  | Bar chart showing scores by category (Self-Awareness, Empathy, etc.) | 🟡 Intermediate | `frontend-web/src/components/results/category-breakdown.tsx`  |
| `PORT-026` | Create RecommendationCard component | Display personalized recommendation with icon                        | 🟢 Beginner     | `frontend-web/src/components/results/recommendation-card.tsx` |
| `PORT-027` | Create HistoryChart component       | Line chart showing score trends over time                            | 🔴 Advanced     | `frontend-web/src/components/results/history-chart.tsx`       |

### 📦 Group C.3: Results Pages

| Issue #    | Title                       | Description                            | Skill           | Files to Create/Modify                               |
| ---------- | --------------------------- | -------------------------------------- | --------------- | ---------------------------------------------------- |
| `PORT-028` | Create Results Detail page  | Full breakdown of a single exam result | 🟡 Intermediate | `frontend-web/src/app/(app)/results/[id]/page.tsx`   |
| `PORT-029` | Create Results History page | List of all past exams with scores     | 🟢 Beginner     | `frontend-web/src/app/(app)/results/page.tsx`        |
| `PORT-030` | Create PDF Export button    | Client-side PDF generation of results  | 🔴 Advanced     | `frontend-web/src/components/results/export-pdf.tsx` |

---

## 🌊 WAVE D: Journal System

**Goal**: Port the journaling feature with mood tracking.

### 📦 Group D.1: Journal API & State

| Issue #    | Title                           | Description                                                      | Skill           | Files to Create/Modify                 |
| ---------- | ------------------------------- | ---------------------------------------------------------------- | --------------- | -------------------------------------- |
| `PORT-031` | Create journal API service      | Build `useJournal` hook for CRUD operations on `/api/v1/journal` | 🟡 Intermediate | `frontend-web/src/hooks/useJournal.ts` |
| `PORT-032` | Define journal TypeScript types | Create interfaces for JournalEntry, MoodRating, Tag              | 🟢 Beginner     | `frontend-web/src/types/journal.ts`    |

### 📦 Group D.2: Journal UI Components

| Issue #    | Title                             | Description                                                   | Skill           | Files to Create/Modify                                   |
| ---------- | --------------------------------- | ------------------------------------------------------------- | --------------- | -------------------------------------------------------- |
| `PORT-033` | Create MoodSlider component       | Slider for rating mood (1-10) with emoji feedback             | 🟡 Intermediate | `frontend-web/src/components/journal/mood-slider.tsx`    |
| `PORT-034` | Create JournalEditor component    | Rich text editor for journal entries                          | 🟡 Intermediate | `frontend-web/src/components/journal/journal-editor.tsx` |
| `PORT-035` | Create TagSelector component      | Multi-select for tagging entries (work, family, health, etc.) | 🟢 Beginner     | `frontend-web/src/components/journal/tag-selector.tsx`   |
| `PORT-036` | Create JournalEntryCard component | Card displaying a past journal entry summary                  | 🟢 Beginner     | `frontend-web/src/components/journal/entry-card.tsx`     |
| `PORT-037` | Create MoodTrendChart component   | Visualize mood over time (last 7/30 days)                     | 🟡 Intermediate | `frontend-web/src/components/journal/mood-trend.tsx`     |

### 📦 Group D.3: Journal Pages

| Issue #    | Title                         | Description                                         | Skill           | Files to Create/Modify                             |
| ---------- | ----------------------------- | --------------------------------------------------- | --------------- | -------------------------------------------------- |
| `PORT-038` | Create New Journal Entry page | Form for creating a new entry with mood, text, tags | 🟡 Intermediate | `frontend-web/src/app/(app)/journal/new/page.tsx`  |
| `PORT-039` | Create Journal List page      | Display all entries with search/filter              | 🟡 Intermediate | `frontend-web/src/app/(app)/journal/page.tsx`      |
| `PORT-040` | Create Journal Detail page    | View a single entry with option to edit/delete      | 🟢 Beginner     | `frontend-web/src/app/(app)/journal/[id]/page.tsx` |

---

## 🌊 WAVE E: Profile & Settings

**Goal**: Port user profile and app settings.

### 📦 Group E.1: Profile System

| Issue #    | Title                        | Description                                    | Skill           | Files to Create/Modify                                 |
| ---------- | ---------------------------- | ---------------------------------------------- | --------------- | ------------------------------------------------------ |
| `PORT-041` | Create profile API service   | Build `useProfile` hook for `/api/v1/profiles` | 🟡 Intermediate | `frontend-web/src/hooks/useProfile.ts`                 |
| `PORT-042` | Create ProfileCard component | Display user avatar, name, bio                 | 🟢 Beginner     | `frontend-web/src/components/profile/profile-card.tsx` |
| `PORT-043` | Create ProfileForm component | Editable form for name, age, bio, goals        | 🟡 Intermediate | `frontend-web/src/components/profile/profile-form.tsx` |
| `PORT-044` | Create Profile page          | Full profile view with edit mode               | 🟡 Intermediate | `frontend-web/src/app/(app)/profile/page.tsx`          |

### 📦 Group E.2: Settings System

| Issue #    | Title                                 | Description                                     | Skill           | Files to Create/Modify                                           |
| ---------- | ------------------------------------- | ----------------------------------------------- | --------------- | ---------------------------------------------------------------- |
| `PORT-045` | Create settings API service           | Build `useSettings` hook for `/api/v1/settings` | 🟡 Intermediate | `frontend-web/src/hooks/useSettings.ts`                          |
| `PORT-046` | Create ThemeToggle component          | Dark/Light mode switch                          | 🟢 Beginner     | `frontend-web/src/components/settings/theme-toggle.tsx`          |
| `PORT-047` | Create NotificationSettings component | Toggle notifications on/off, frequency          | 🟢 Beginner     | `frontend-web/src/components/settings/notification-settings.tsx` |
| `PORT-048` | Create PrivacySettings component      | Data consent, export data, delete account       | 🟡 Intermediate | `frontend-web/src/components/settings/privacy-settings.tsx`      |
| `PORT-049` | Create Settings page                  | Tabbed settings interface                       | 🟡 Intermediate | `frontend-web/src/app/(app)/settings/page.tsx`                   |

---

## 🌊 WAVE F: Dashboard & Home

**Goal**: Create the main dashboard experience.

### 📦 Group F.1: Dashboard Components

| Issue #    | Title                           | Description                                                         | Skill           | Files to Create/Modify                                      |
| ---------- | ------------------------------- | ------------------------------------------------------------------- | --------------- | ----------------------------------------------------------- |
| `PORT-050` | Create WelcomeCard component    | Personalized greeting with username and time-of-day                 | 🟢 Beginner     | `frontend-web/src/components/dashboard/welcome-card.tsx`    |
| `PORT-051` | Create QuickActions component   | Grid of quick action buttons (New Journal, Take Exam, View Results) | 🟢 Beginner     | `frontend-web/src/components/dashboard/quick-actions.tsx`   |
| `PORT-052` | Create RecentActivity component | List of recent exams and journals                                   | 🟡 Intermediate | `frontend-web/src/components/dashboard/recent-activity.tsx` |
| `PORT-053` | Create MoodWidget component     | Today's mood status with quick-add                                  | 🟡 Intermediate | `frontend-web/src/components/dashboard/mood-widget.tsx`     |
| `PORT-054` | Create InsightCard component    | Display AI-generated insight/tip                                    | 🟢 Beginner     | `frontend-web/src/components/dashboard/insight-card.tsx`    |

### 📦 Group F.2: Dashboard Page

| Issue #    | Title                          | Description                                          | Skill           | Files to Create/Modify                             |
| ---------- | ------------------------------ | ---------------------------------------------------- | --------------- | -------------------------------------------------- |
| `PORT-055` | Create Dashboard page          | Compose all dashboard widgets into responsive layout | 🟡 Intermediate | `frontend-web/src/app/(app)/dashboard/page.tsx`    |
| `PORT-056` | Create Dashboard loading state | Skeleton loaders for all widgets                     | 🟢 Beginner     | `frontend-web/src/app/(app)/dashboard/loading.tsx` |

---

## 📊 Summary

| Wave  | Group | Issues               | Prerequisite |
| ----- | ----- | -------------------- | ------------ |
| **A** | A.1   | PORT-001 to PORT-004 | None         |
| **A** | A.2   | PORT-005 to PORT-010 | None         |
| **B** | B.1   | PORT-011 to PORT-013 | Wave A       |
| **B** | B.2   | PORT-014 to PORT-017 | Wave A       |
| **B** | B.3   | PORT-018 to PORT-021 | B.1, B.2     |
| **C** | C.1   | PORT-022 to PORT-023 | Wave B       |
| **C** | C.2   | PORT-024 to PORT-027 | Wave B       |
| **C** | C.3   | PORT-028 to PORT-030 | C.1, C.2     |
| **D** | D.1   | PORT-031 to PORT-032 | Wave A       |
| **D** | D.2   | PORT-033 to PORT-037 | Wave A       |
| **D** | D.3   | PORT-038 to PORT-040 | D.1, D.2     |
| **E** | E.1   | PORT-041 to PORT-044 | Wave A       |
| **E** | E.2   | PORT-045 to PORT-049 | Wave A       |
| **F** | F.1   | PORT-050 to PORT-054 | Waves B-E    |
| **F** | F.2   | PORT-055 to PORT-056 | F.1          |

**Total Issues: 56**

---

## 🏷️ Issue Labels

When creating GitHub issues, use these labels:

| Label                   | Description                              |
| ----------------------- | ---------------------------------------- |
| `phase: porting`        | All Phase 2 issues                       |
| `wave: A/B/C/D/E/F`     | Which wave                               |
| `group: X.Y`            | Which group                              |
| `skill: beginner`       | 🟢 New contributors welcome              |
| `skill: intermediate`   | 🟡 Some React/API experience needed      |
| `skill: advanced`       | 🔴 Complex architecture knowledge needed |
| `area: frontend`        | Web UI work                              |
| `area: api-integration` | Hooks and API services                   |
| `type: component`       | Reusable UI component                    |
| `type: page`            | Next.js page/route                       |
| `type: hook`            | React hook                               |

---

## 📋 How to Contribute

1. **Check the current Wave** - Look for issues labeled with the active wave
2. **Pick an issue from an open Group** - All issues in a Group can be worked on simultaneously
3. **Comment to claim** - Write "I'll take this" on the issue
4. **Fork and branch** - Create a branch named `feat/PORT-XXX-short-description`
5. **Submit PR** - Reference the issue number in your PR description
6. **Wait for review** - Maintainers will review and merge

---

## 🎯 Definition of Done

Each issue is complete when:

- [ ] Code is written and compiles without errors
- [ ] Component has TypeScript types
- [ ] Basic responsive design (mobile-first)
- [ ] Matches the design system (uses existing Tailwind classes)
- [ ] Has loading and error states where applicable
- [ ] PR passes CI checks
