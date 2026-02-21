# Wave D: Journal System - Issue Details

This document contains detailed GitHub issue descriptions for Wave D of Phase 2: Feature Porting.

**Prerequisite:** Wave A must be completed before starting Wave D. (Wave D can run in parallel with Waves B/C)

---

## 📦 Group D.1: Journal API & State

---

## PORT-031: Create journal API service

### Description

Build a React hook to handle all journal CRUD operations with the backend API.

### Requirements

- [ ] Create `frontend-web/src/hooks/useJournal.ts`
- [ ] Endpoints to support:
  - `GET /api/v1/journal` - list entries (paginated)
  - `GET /api/v1/journal/{id}` - single entry
  - `POST /api/v1/journal` - create entry
  - `PUT /api/v1/journal/{id}` - update entry
  - `DELETE /api/v1/journal/{id}` - delete entry
- [ ] Support query parameters for list:
  - `page`, `per_page`
  - `start_date`, `end_date`
  - `mood_min`, `mood_max`
  - `tags` (comma-separated)
  - `search` (text search)
- [ ] Return: `entries`, `entry`, `isLoading`, `error`, `createEntry`, `updateEntry`, `deleteEntry`

### API Response Format

```json
{
  "entries": [
    {
      "id": 1,
      "content": "Today I felt really productive...",
      "mood_rating": 8,
      "energy_level": 7,
      "stress_level": 3,
      "tags": ["work", "productivity"],
      "sentiment_score": 0.75,
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-15T10:30:00Z"
    }
  ],
  "total": 50,
  "page": 1,
  "per_page": 10
}
```

### Acceptance Criteria

- [ ] All CRUD operations work correctly
- [ ] Pagination works
- [ ] Filtering and search work
- [ ] Optimistic updates for better UX

### Labels

`phase: porting` `wave: D` `group: D.1` `skill: intermediate` `area: api-integration` `type: hook`

---

## PORT-032: Define journal TypeScript types

### Description

Create TypeScript interfaces for journal entries, mood ratings, and tags.

### Requirements

- [ ] Create `frontend-web/src/types/journal.ts`
- [ ] Define interfaces:

  ```typescript
  interface JournalEntry {
    id: number;
    content: string;
    mood_rating: number; // 1-10
    energy_level?: number; // 1-10
    stress_level?: number; // 1-10
    tags: string[];
    sentiment_score?: number; // -1 to 1
    patterns?: string[]; // detected emotional patterns
    created_at: string;
    updated_at: string;
  }

  interface JournalEntryCreate {
    content: string;
    mood_rating: number;
    energy_level?: number;
    stress_level?: number;
    tags?: string[];
  }

  interface JournalEntryUpdate extends Partial<JournalEntryCreate> {}

  interface JournalFilters {
    startDate?: string;
    endDate?: string;
    moodMin?: number;
    moodMax?: number;
    tags?: string[];
    search?: string;
  }

  type MoodLevel = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10;

  const PRESET_TAGS = [
    "work",
    "family",
    "health",
    "relationships",
    "personal",
    "gratitude",
    "goals",
    "stress",
    "achievement",
    "learning",
  ] as const;
  ```

- [ ] Export all types

### Acceptance Criteria

- [ ] All interfaces properly typed
- [ ] Types match API format
- [ ] Preset tags are defined

### Labels

`phase: porting` `wave: D` `group: D.1` `skill: beginner` `area: frontend` `type: types`

---

## 📦 Group D.2: Journal UI Components

---

## PORT-033: Create MoodSlider component

### Description

Build an interactive slider for rating mood (1-10) with emoji feedback.

### Requirements

- [ ] Create `frontend-web/src/components/journal/mood-slider.tsx`
- [ ] Props:
  - `value` - current mood (1-10)
  - `onChange` - callback with new value
  - `label` - optional label text
  - `showEmoji` - boolean (default true)
- [ ] Display:
  - Slider from 1-10
  - Current value displayed
  - Emoji changes based on value:
    - 1-2: 😢 (very sad)
    - 3-4: 😕 (sad)
    - 5-6: 😐 (neutral)
    - 7-8: 🙂 (happy)
    - 9-10: 😄 (very happy)
  - Optional color gradient (red → yellow → green)

### Design Reference

- Large, touch-friendly slider
- Emoji animates on change
- Color reflects mood level

### Acceptance Criteria

- [ ] Slider moves smoothly
- [ ] Emoji updates correctly
- [ ] Value is accurate
- [ ] Touch-friendly on mobile

### Labels

`phase: porting` `wave: D` `group: D.2` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-034: Create JournalEditor component

### Description

Build a rich text editor for writing journal entries.

### Requirements

- [ ] Create `frontend-web/src/components/journal/journal-editor.tsx`
- [ ] Props:
  - `value` - current content
  - `onChange` - callback with new content
  - `placeholder` - placeholder text
  - `minHeight` - minimum editor height
  - `maxLength` - optional character limit
- [ ] Features:
  - Basic formatting: bold, italic, bullet points
  - Character counter (if maxLength set)
  - Auto-save indicator
  - Expandable height as user types
- [ ] Optional: markdown support

### Technical Details

- Use a library like `@tiptap/react`, `react-quill`, or plain `textarea`
- Keep it simple - don't over-engineer
- Consider mobile keyboard behavior

### Acceptance Criteria

- [ ] Text input works correctly
- [ ] Basic formatting works (if implemented)
- [ ] Character counter accurate
- [ ] Responsive on mobile

### Labels

`phase: porting` `wave: D` `group: D.2` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-035: Create TagSelector component

### Description

Build a multi-select component for tagging journal entries.

### Requirements

- [ ] Create `frontend-web/src/components/journal/tag-selector.tsx`
- [ ] Props:
  - `selected` - array of selected tags
  - `onChange` - callback with updated selection
  - `presets` - optional preset tags (use PRESET_TAGS by default)
  - `allowCustom` - boolean (allow creating new tags)
  - `maxTags` - optional limit
- [ ] Display:
  - Pill/chip style for selected tags
  - Dropdown or inline list for available tags
  - Remove button on selected tags
  - Add custom tag input (if enabled)

### Design Reference

- Compact, horizontal chips
- Color-coded tags (optional)
- Easy to add/remove

### Acceptance Criteria

- [ ] Can select multiple tags
- [ ] Can remove selected tags
- [ ] Custom tag creation works
- [ ] Max tags limit enforced

### Labels

`phase: porting` `wave: D` `group: D.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-036: Create JournalEntryCard component

### Description

Build a card component for displaying a past journal entry summary in lists.

### Requirements

- [ ] Create `frontend-web/src/components/journal/entry-card.tsx`
- [ ] Props:
  - `entry` - JournalEntry object
  - `onClick` - callback when clicked
  - `variant` - 'compact' | 'expanded'
- [ ] Display:
  - Date (formatted nicely, e.g., "January 15, 2024")
  - Mood emoji and rating
  - Content preview (truncated, ~100 chars)
  - Tags as chips
  - Sentiment indicator (optional subtle color)
- [ ] Hover state with slight elevation

### Design Reference

- Clean card design matching app theme
- Visual mood indicator (color accent or emoji)
- Clickable to view full entry

### Acceptance Criteria

- [ ] All entry info displays
- [ ] Content truncates properly
- [ ] Click handler works
- [ ] Both variants render correctly

### Labels

`phase: porting` `wave: D` `group: D.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-037: Create MoodTrendChart component

### Description

Build a chart component showing mood trends over time.

### Requirements

- [ ] Create `frontend-web/src/components/journal/mood-trend.tsx`
- [ ] Props:
  - `entries` - array of JournalEntry
  - `timeRange` - '7d' | '14d' | '30d'
  - `showAverage` - boolean (show average line)
- [ ] Display:
  - X-axis: dates
  - Y-axis: mood rating (1-10)
  - Points for each entry
  - Line connecting points
  - Optional: area fill gradient
  - Average mood line (dotted)

### Technical Details

- Use charting library (Recharts, Chart.js)
- Handle missing days gracefully
- Tooltip on hover

### Acceptance Criteria

- [ ] Chart renders correctly
- [ ] Time range filter works
- [ ] Tooltips show entry details
- [ ] Responsive sizing

### Labels

`phase: porting` `wave: D` `group: D.2` `skill: intermediate` `area: frontend` `type: component`

---

## 📦 Group D.3: Journal Pages

---

## PORT-038: Create New Journal Entry page

### Description

Build the page for creating a new journal entry.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/journal/new/page.tsx`
- [ ] Integrate:
  - `JournalEditor` component
  - `MoodSlider` component
  - `TagSelector` component
  - Optional: energy level slider, stress level slider
- [ ] Layout:
  1. Header with date and cancel button
  2. Mood slider (prominent)
  3. Text editor (main area)
  4. Tags selector
  5. Optional fields (energy, stress)
  6. Save button (sticky at bottom)
- [ ] Features:
  - Auto-save to localStorage as draft
  - Confirm before discarding unsaved changes
  - Success toast on save
  - Navigate to journal list after save

### Design Reference

- Clean, distraction-free writing experience
- Mood selector is prominent (encourages reflection)
- Mobile-optimized for on-the-go journaling

### Acceptance Criteria

- [ ] All fields work correctly
- [ ] Entry saves to API
- [ ] Drafts persist on refresh
- [ ] Navigation works properly

### Labels

`phase: porting` `wave: D` `group: D.3` `skill: intermediate` `area: frontend` `type: page`

---

## PORT-039: Create Journal List page

### Description

Build the main journal page showing all entries with search and filter.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/journal/page.tsx`
- [ ] Integrate:
  - `useJournal` hook
  - `JournalEntryCard` component
  - `MoodTrendChart` component
- [ ] Layout:
  1. Header with "New Entry" button
  2. Mood trend chart (collapsible)
  3. Search and filter bar
  4. Entry list (cards)
  5. Pagination or infinite scroll
- [ ] Filters:
  - Date range picker
  - Mood range (min/max sliders)
  - Tag filter (multi-select)
  - Text search

### Design Reference

- Timeline-like view of entries
- Filters easily accessible but not overwhelming
- Empty state for new users

### Acceptance Criteria

- [ ] All entries display in cards
- [ ] Search filters work correctly
- [ ] Pagination/scroll works
- [ ] Empty state shows for new users
- [ ] Click navigates to detail

### Labels

`phase: porting` `wave: D` `group: D.3` `skill: intermediate` `area: frontend` `type: page`

---

## PORT-040: Create Journal Detail page

### Description

Build a page for viewing a single journal entry with option to edit or delete.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/journal/[id]/page.tsx`
- [ ] Display:
  - Full date and time
  - Mood rating with emoji
  - Full entry content
  - Tags
  - Optional: energy/stress levels
  - Sentiment analysis results (if available)
  - AI-detected patterns (if available)
- [ ] Actions:
  - Edit button (navigates to edit mode)
  - Delete button (with confirmation)
  - Back button
  - Share button (optional)
- [ ] Edit mode:
  - Same form as "New Entry"
  - Pre-filled with existing data
  - Save updates entry

### Acceptance Criteria

- [ ] Entry displays fully
- [ ] Edit mode works
- [ ] Delete with confirmation works
- [ ] Navigation works correctly

### Labels

`phase: porting` `wave: D` `group: D.3` `skill: beginner` `area: frontend` `type: page`

---

## 📊 Wave D Summary

| Issue    | Title                      | Skill           | Group |
| -------- | -------------------------- | --------------- | ----- |
| PORT-031 | Journal API service        | 🟡 Intermediate | D.1   |
| PORT-032 | Journal TypeScript types   | 🟢 Beginner     | D.1   |
| PORT-033 | MoodSlider component       | 🟡 Intermediate | D.2   |
| PORT-034 | JournalEditor component    | 🟡 Intermediate | D.2   |
| PORT-035 | TagSelector component      | 🟢 Beginner     | D.2   |
| PORT-036 | JournalEntryCard component | 🟢 Beginner     | D.2   |
| PORT-037 | MoodTrendChart component   | 🟡 Intermediate | D.2   |
| PORT-038 | New Journal Entry page     | 🟡 Intermediate | D.3   |
| PORT-039 | Journal List page          | 🟡 Intermediate | D.3   |
| PORT-040 | Journal Detail page        | 🟢 Beginner     | D.3   |

**Total: 10 issues** (4 Beginner, 6 Intermediate)

---

## 🔗 Dependencies

```
Group D.1 (API & Types)     Group D.2 (UI Components)
        ↘                    ↙
              Group D.3 (Pages)
```

- **D.1** and **D.2** can be worked on **in parallel**
- **D.3** depends on both D.1 and D.2
- **Wave D can run in parallel with Waves B and C** (all depend on Wave A only)
