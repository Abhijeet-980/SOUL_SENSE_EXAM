# Wave C: Results & Analytics - Issue Details

This document contains detailed GitHub issue descriptions for Wave C of Phase 2: Feature Porting.

**Prerequisite:** Wave B must be completed before starting Wave C.

---

## 📦 Group C.1: Results API & Types

---

## PORT-022: Create results API service

### Description

Build a React hook to fetch exam results from the backend API.

### Requirements

- [ ] Create `frontend-web/src/hooks/useResults.ts`
- [ ] Endpoints to support:
  - `GET /api/v1/exams/{id}/results` - single result
  - `GET /api/v1/exams/history` - all user's results
- [ ] Return: `result`, `results`, `isLoading`, `error`, `refetch`
- [ ] Support pagination for history

### API Response Format (Single Result)

```json
{
  "id": 123,
  "overall_score": 78,
  "categories": [
    { "name": "Self-Awareness", "score": 82, "max_score": 100 },
    { "name": "Self-Regulation", "score": 75, "max_score": 100 },
    { "name": "Motivation", "score": 80, "max_score": 100 },
    { "name": "Empathy", "score": 72, "max_score": 100 },
    { "name": "Social Skills", "score": 81, "max_score": 100 }
  ],
  "recommendations": [
    { "id": 1, "title": "Practice mindfulness", "description": "..." }
  ],
  "completed_at": "2024-01-15T10:30:00Z",
  "duration_seconds": 890
}
```

### Acceptance Criteria

- [ ] Can fetch single result by ID
- [ ] Can fetch paginated history
- [ ] Error handling works correctly
- [ ] TypeScript types are defined

### Labels

`phase: porting` `wave: C` `group: C.1` `skill: intermediate` `area: api-integration` `type: hook`

---

## PORT-023: Define result TypeScript types

### Description

Create TypeScript interfaces for exam results, category scores, and recommendations.

### Requirements

- [ ] Create `frontend-web/src/types/results.ts`
- [ ] Define interfaces:

  ```typescript
  interface ExamResult {
    id: number;
    overall_score: number;
    categories: CategoryScore[];
    recommendations: Recommendation[];
    completed_at: string;
    duration_seconds: number;
    reflection?: string;
  }

  interface CategoryScore {
    name: string;
    score: number;
    max_score: number;
    description?: string;
  }

  interface Recommendation {
    id: number;
    title: string;
    description: string;
    category?: string;
    priority?: "high" | "medium" | "low";
  }

  interface ResultsHistory {
    results: ExamResult[];
    total: number;
    page: number;
    per_page: number;
  }
  ```

- [ ] Export all types

### Acceptance Criteria

- [ ] All interfaces are properly typed
- [ ] Types match API response format
- [ ] No TypeScript errors when importing

### Labels

`phase: porting` `wave: C` `group: C.1` `skill: beginner` `area: frontend` `type: types`

---

## 📦 Group C.2: Result Visualization Components

---

## PORT-024: Create ScoreGauge component

### Description

Build a circular gauge component to display the overall EQ score (0-100).

### Requirements

- [ ] Create `frontend-web/src/components/results/score-gauge.tsx`
- [ ] Props:
  - `score` - number 0-100
  - `size` - 'sm' | 'md' | 'lg'
  - `showLabel` - boolean
  - `animated` - boolean (animate on mount)
- [ ] Display:
  - Circular progress ring
  - Score number in center
  - Optional label below (e.g., "Your EQ Score")
- [ ] Color coding:
  - 0-40: Red (needs work)
  - 41-60: Yellow (developing)
  - 61-80: Green (good)
  - 81-100: Blue/Purple (excellent)

### Technical Details

- Use SVG for the circular gauge
- CSS animations for the fill effect
- Consider using a library like `react-circular-progressbar` or build custom

### Acceptance Criteria

- [ ] Gauge displays score correctly
- [ ] Color changes based on score
- [ ] Animation works on mount
- [ ] All sizes render correctly

### Labels

`phase: porting` `wave: C` `group: C.2` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-025: Create CategoryBreakdown component

### Description

Build a bar chart component showing scores by EQ category.

### Requirements

- [ ] Create `frontend-web/src/components/results/category-breakdown.tsx`
- [ ] Props:
  - `categories` - array of CategoryScore
  - `showLabels` - boolean
  - `animated` - boolean
- [ ] Display:
  - Horizontal bar for each category
  - Category name on left
  - Score value on right
  - Bar fills proportionally
- [ ] Support 5 EQ categories:
  - Self-Awareness
  - Self-Regulation
  - Motivation
  - Empathy
  - Social Skills

### Design Reference

- Clean horizontal bars
- Subtle color variations per category
- Hover state shows more detail
- Consider comparison with average (dotted line)

### Acceptance Criteria

- [ ] All 5 categories display
- [ ] Bars fill to correct percentage
- [ ] Animations work smoothly
- [ ] Responsive on mobile

### Labels

`phase: porting` `wave: C` `group: C.2` `skill: intermediate` `area: frontend` `type: component`

---

## PORT-026: Create RecommendationCard component

### Description

Build a card component for displaying personalized recommendations.

### Requirements

- [ ] Create `frontend-web/src/components/results/recommendation-card.tsx`
- [ ] Props:
  - `recommendation` - Recommendation object
  - `isExpanded` - boolean (optional)
  - `onToggle` - callback (optional)
- [ ] Display:
  - Icon based on category
  - Title (bold)
  - Description (truncated, expandable)
  - Priority badge (if high priority)
  - Related category tag

### Design Reference

- Card with slight elevation
- Expandable on click (accordion style)
- Icon should be relevant to category (e.g., brain for self-awareness)

### Acceptance Criteria

- [ ] Card displays all information
- [ ] Expand/collapse works
- [ ] Priority badge shows for high priority
- [ ] Accessible (keyboard, screen reader)

### Labels

`phase: porting` `wave: C` `group: C.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-027: Create HistoryChart component

### Description

Build a line chart component showing score trends over time (multiple exams).

### Requirements

- [ ] Create `frontend-web/src/components/results/history-chart.tsx`
- [ ] Props:
  - `results` - array of ExamResult (historical data)
  - `timeRange` - '7d' | '30d' | '90d' | 'all'
  - `showCategories` - boolean (overlay category trends)
- [ ] Display:
  - X-axis: dates
  - Y-axis: scores (0-100)
  - Line for overall score
  - Optional: multiple lines for categories
  - Tooltip on hover showing details

### Technical Details

- Use a charting library: Recharts, Chart.js, or Visx
- Responsive sizing
- Handle empty state (no history yet)

### Acceptance Criteria

- [ ] Chart renders with correct data
- [ ] Time range filter works
- [ ] Tooltips show on hover
- [ ] Responsive on all screen sizes
- [ ] Empty state displays message

### Labels

`phase: porting` `wave: C` `group: C.2` `skill: advanced` `area: frontend` `type: component`

---

## 📦 Group C.3: Results Pages

---

## PORT-028: Create Results Detail page

### Description

Build the full results page showing detailed breakdown of a single exam.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/results/[id]/page.tsx`
- [ ] Integrate:
  - `useResults` hook
  - `ScoreGauge` component
  - `CategoryBreakdown` component
  - `RecommendationCard` component
- [ ] Sections:
  1. Header with date and duration
  2. Overall score gauge
  3. Category breakdown chart
  4. Personalized recommendations
  5. Reflection (if submitted)
  6. "Retake Exam" and "Export PDF" buttons

### Design Reference

- Single scrollable page
- Clear visual hierarchy
- Print-friendly layout

### Acceptance Criteria

- [ ] All sections display correctly
- [ ] Data loads from API
- [ ] Loading and error states handled
- [ ] Mobile responsive

### Labels

`phase: porting` `wave: C` `group: C.3` `skill: intermediate` `area: frontend` `type: page`

---

## PORT-029: Create Results History page

### Description

Build a page showing all past exam results for the user.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/results/page.tsx`
- [ ] Display:
  - List/grid of past results
  - Each item shows: date, score, duration
  - Click to view full details
  - Empty state for new users
- [ ] Features:
  - Sort by date or score
  - Filter by date range
  - Pagination or infinite scroll
- [ ] Include `HistoryChart` at top for trends

### Design Reference

- Clean list view with cards
- Score indicator on each card (color coded)
- Trend chart at top (collapsible)

### Acceptance Criteria

- [ ] All past results display
- [ ] Clicking navigates to detail page
- [ ] Sorting and filtering work
- [ ] Empty state shows for new users

### Labels

`phase: porting` `wave: C` `group: C.3` `skill: beginner` `area: frontend` `type: page`

---

## PORT-030: Create PDF Export button

### Description

Build a button/function to export exam results as a PDF document.

### Requirements

- [ ] Create `frontend-web/src/components/results/export-pdf.tsx`
- [ ] Props:
  - `resultId` - ID of the result to export
  - OR `result` - full result object (to avoid refetching)
- [ ] PDF should include:
  - Soul Sense header/branding
  - User name and date
  - Overall score with gauge visualization
  - Category breakdown
  - Top recommendations
  - Footer with disclaimer

### Technical Details

- Use a PDF library: `jspdf`, `react-pdf`, or `html2pdf`
- Consider server-side generation via API if complex
- Handle loading state during generation

### Acceptance Criteria

- [ ] PDF generates and downloads
- [ ] Contains all required sections
- [ ] Looks professional and branded
- [ ] Works on all browsers

### Labels

`phase: porting` `wave: C` `group: C.3` `skill: advanced` `area: frontend` `type: component`

---

## 📊 Wave C Summary

| Issue    | Title                        | Skill           | Group |
| -------- | ---------------------------- | --------------- | ----- |
| PORT-022 | Results API service          | 🟡 Intermediate | C.1   |
| PORT-023 | Result TypeScript types      | 🟢 Beginner     | C.1   |
| PORT-024 | ScoreGauge component         | 🟡 Intermediate | C.2   |
| PORT-025 | CategoryBreakdown component  | 🟡 Intermediate | C.2   |
| PORT-026 | RecommendationCard component | 🟢 Beginner     | C.2   |
| PORT-027 | HistoryChart component       | 🔴 Advanced     | C.2   |
| PORT-028 | Results Detail page          | 🟡 Intermediate | C.3   |
| PORT-029 | Results History page         | 🟢 Beginner     | C.3   |
| PORT-030 | PDF Export button            | 🔴 Advanced     | C.3   |

**Total: 9 issues** (3 Beginner, 4 Intermediate, 2 Advanced)

---

## 🔗 Dependencies

```
Group C.1 (API & Types)     Group C.2 (Visualizations)
        ↘                    ↙
              Group C.3 (Pages)
```

- **C.1** and **C.2** can be worked on **in parallel**
- **C.3** depends on both C.1 and C.2
