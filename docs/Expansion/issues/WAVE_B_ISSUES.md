# Wave B: EQ Assessment System - Issue Details

This document contains detailed GitHub issue descriptions for Wave B of Phase 2: Feature Porting.

**Prerequisite:** Wave A must be completed before starting Wave B.

---

## 📦 Group B.1: Assessment API & State

---

## PORT-011: Create questions API service

### Description

Build a React hook to fetch assessment questions from the backend API. This will be used by the exam pages to load questions dynamically.

### Requirements

- [ ] Create `frontend-web/src/hooks/useQuestions.ts`
- [ ] Fetch questions from `GET /api/v1/questions`
- [ ] Support query parameters:
  - `category` (optional) - filter by category
  - `count` (optional) - limit number of questions
- [ ] Return states: `questions`, `isLoading`, `error`
- [ ] Cache questions to avoid refetching

### API Response Format

```json
{
  "questions": [
    {
      "id": 1,
      "text": "I can easily identify my emotions as I experience them.",
      "category": "self_awareness",
      "options": [
        { "value": 1, "label": "Strongly Disagree" },
        { "value": 2, "label": "Disagree" },
        { "value": 3, "label": "Neutral" },
        { "value": 4, "label": "Agree" },
        { "value": 5, "label": "Strongly Agree" }
      ]
    }
  ]
}
```

### Acceptance Criteria

- [ ] Hook fetches questions successfully
- [ ] Loading state works correctly
- [ ] Error handling displays user-friendly message
- [ ] Questions are typed with TypeScript

### Labels

`phase: porting` `wave: B` `group: B.1` `skill: intermediate` `area: api-integration` `type: hook`

---

## PORT-012: Create exam submission service

### Description

Build a React hook to submit exam answers to the backend and receive results.

### Requirements

- [ ] Create `frontend-web/src/hooks/useExamSubmit.ts`
- [ ] POST answers to `/api/v1/exams`
- [ ] Request body format:
  ```json
  {
    "answers": [
      { "question_id": 1, "value": 4 },
      { "question_id": 2, "value": 3 }
    ],
    "reflection": "Optional reflection text",
    "duration_seconds": 300
  }
  ```
- [ ] Return: `submitExam` function, `isSubmitting`, `error`, `result`
- [ ] Handle validation errors from backend

### Acceptance Criteria

- [ ] Can submit answers successfully
- [ ] Returns exam result ID on success
- [ ] Handles network errors gracefully
- [ ] Shows loading state during submission

### Labels

`phase: porting` `wave: B` `group: B.1` `skill: intermediate` `area: api-integration` `type: hook`

---

## PORT-013: Create exam state store

### Description

Build a state management store to track exam progress (current question, answers, timer).

### Requirements

- [ ] Create `frontend-web/src/stores/examStore.ts`
- [ ] Use Zustand or React Context (Zustand preferred)
- [ ] State shape:
  ```typescript
  interface ExamState {
    questions: Question[];
    currentIndex: number;
    answers: Record<number, number>; // questionId -> answer value
    startTime: Date | null;
    isCompleted: boolean;

    // Actions
    setQuestions: (questions: Question[]) => void;
    setAnswer: (questionId: number, value: number) => void;
    nextQuestion: () => void;
    previousQuestion: () => void;
    completeExam: () => void;
    resetExam: () => void;
  }
  ```
- [ ] Persist to sessionStorage (exam progress survives refresh)

### Acceptance Criteria

- [ ] All state updates work correctly
- [ ] Navigation between questions works
- [ ] Answers are saved as user selects them
- [ ] State persists on page refresh

### Labels

`phase: porting` `wave: B` `group: B.1` `skill: intermediate` `area: frontend` `type: hook`

---

## 📦 Group B.2: Assessment UI Components

---

## PORT-014: Create QuestionCard component

### Description

Build the main question display component showing the question text and answer options.

### Requirements

- [ ] Create `frontend-web/src/components/exam/question-card.tsx`
- [ ] Props:
  - `question` - the question object
  - `selectedValue` - currently selected answer (optional)
  - `onSelect` - callback when user selects an option
  - `disabled` - disable interaction (optional)
- [ ] Display:
  - Question number and category badge
  - Question text (large, readable)
  - 5 radio button options (Likert scale 1-5)
- [ ] Visual feedback on selection
- [ ] Keyboard accessible (arrow keys to navigate options)

### Design Reference

- Clean card design with subtle shadow
- Options should be large touch targets
- Selected option should be clearly highlighted
- Smooth transition animations

### Acceptance Criteria

- [ ] Question text displays correctly
- [ ] All 5 options are selectable
- [ ] Selected option is visually distinct
- [ ] Keyboard navigation works (Tab, Arrow keys, Enter)

### Labels

`phase: porting` `wave: B` `group: B.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-015: Create ExamProgress component

### Description

Build a component showing the user's progress through the exam.

### Requirements

- [ ] Create `frontend-web/src/components/exam/exam-progress.tsx`
- [ ] Props:
  - `current` - current question index (1-based)
  - `total` - total number of questions
  - `answeredCount` - number of questions answered
- [ ] Display:
  - Progress bar (visual)
  - Text: "Question 5 of 20"
  - Answered count: "12 answered"
- [ ] Optional: clickable dots/numbers to jump to specific questions

### Acceptance Criteria

- [ ] Progress bar fills correctly
- [ ] Text updates with current/total
- [ ] Answered count is accurate
- [ ] Responsive on mobile

### Labels

`phase: porting` `wave: B` `group: B.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-016: Create ExamNavigation component

### Description

Build the navigation controls for moving between questions and submitting the exam.

### Requirements

- [ ] Create `frontend-web/src/components/exam/exam-navigation.tsx`
- [ ] Props:
  - `onPrevious` - callback for previous button
  - `onNext` - callback for next button
  - `onSubmit` - callback for submit button
  - `canGoPrevious` - disable previous button
  - `canGoNext` - disable next button
  - `isLastQuestion` - show Submit instead of Next
  - `isSubmitting` - show loading state on submit
- [ ] Buttons:
  - Previous (left arrow icon)
  - Next (right arrow icon) OR Submit (checkmark icon)
- [ ] Keyboard shortcuts: Left/Right arrow keys

### Acceptance Criteria

- [ ] Previous/Next buttons work correctly
- [ ] Submit button appears on last question
- [ ] Disabled states work correctly
- [ ] Keyboard shortcuts work

### Labels

`phase: porting` `wave: B` `group: B.2` `skill: beginner` `area: frontend` `type: component`

---

## PORT-017: Create ExamTimer component

### Description

Build an optional countdown timer for timed exams.

### Requirements

- [ ] Create `frontend-web/src/components/exam/exam-timer.tsx`
- [ ] Props:
  - `durationMinutes` - total exam duration
  - `onTimeUp` - callback when timer reaches 0
  - `isPaused` - pause the timer
- [ ] Display:
  - MM:SS format
  - Color changes: green > yellow (< 5 min) > red (< 1 min)
  - Visual warning animation when low
- [ ] Optional: pause/resume functionality

### Technical Details

- Use `useEffect` with `setInterval`
- Clean up interval on unmount
- Consider using a dedicated timer hook

### Acceptance Criteria

- [ ] Timer counts down correctly
- [ ] Color changes at thresholds
- [ ] Callback fires when time reaches 0
- [ ] Doesn't drift over long periods

### Labels

`phase: porting` `wave: B` `group: B.2` `skill: intermediate` `area: frontend` `type: component`

---

## 📦 Group B.3: Assessment Pages

---

## PORT-018: Create Exam Start page

### Description

Build the exam landing page where users see instructions and can start the assessment.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/exam/page.tsx`
- [ ] Content:
  - Title: "EQ Assessment"
  - Description of what the exam measures
  - Instructions (read each question carefully, no right/wrong answers)
  - Estimated time (e.g., "15-20 minutes")
  - Number of questions
  - "Start Assessment" button
- [ ] Check if user has incomplete exam and offer to resume
- [ ] Show history of past exams (if any)

### Design Reference

- Clean, calming design (not intimidating)
- Use Card component for content sections
- Prominent CTA button

### Acceptance Criteria

- [ ] All information displays correctly
- [ ] Start button navigates to first question
- [ ] Resume option works if applicable
- [ ] Mobile responsive

### Labels

`phase: porting` `wave: B` `group: B.3` `skill: beginner` `area: frontend` `type: page`

---

## PORT-019: Create Exam Question page

### Description

Build the main exam flow page that displays questions and handles navigation.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/exam/[id]/page.tsx`
- [ ] Integrate:
  - `useQuestions` hook to fetch questions
  - `examStore` for state management
  - `QuestionCard` component
  - `ExamProgress` component
  - `ExamNavigation` component
  - `ExamTimer` component (if timed)
- [ ] Flow:
  1. Load questions on mount
  2. Display current question
  3. Save answer to store on selection
  4. Navigate with Previous/Next
  5. Submit on last question
  6. Redirect to reflection or results

### Technical Details

- Handle loading state with skeleton
- Handle error state with retry option
- Confirm before leaving page if exam in progress
- Use route parameter for exam type/id

### Acceptance Criteria

- [ ] Questions load and display
- [ ] Navigation works correctly
- [ ] Answers are saved to store
- [ ] Submit triggers API call
- [ ] Redirects to next step on completion

### Labels

`phase: porting` `wave: B` `group: B.3` `skill: intermediate` `area: frontend` `type: page`

---

## PORT-020: Create Reflection screen

### Description

Build the post-exam reflection screen where users can optionally share their thoughts.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/exam/reflection/page.tsx`
- [ ] Content:
  - Title: "Take a moment to reflect"
  - Prompt: "How did you feel during this assessment?"
  - Text area for reflection (optional, 500 char limit)
  - "Skip" button
  - "Submit Reflection" button
- [ ] Save reflection with exam submission
- [ ] Navigate to results on submit/skip

### Design Reference

- Calm, spacious design
- Non-pressuring copy (it's optional)
- Character counter for text area

### Acceptance Criteria

- [ ] Text area accepts input
- [ ] Character limit enforced
- [ ] Skip navigates to results
- [ ] Submit saves reflection and navigates

### Labels

`phase: porting` `wave: B` `group: B.3` `skill: beginner` `area: frontend` `type: page`

---

## PORT-021: Create Exam Complete page

### Description

Build the success screen shown after exam submission.

### Requirements

- [ ] Create `frontend-web/src/app/(app)/exam/complete/page.tsx`
- [ ] Content:
  - Success animation (checkmark or confetti)
  - Title: "Assessment Complete!"
  - Message: "Your results are ready"
  - "View Results" button (primary)
  - "Return to Dashboard" button (secondary)
- [ ] Show basic summary if available (time taken, questions answered)
- [ ] Clear exam state from store

### Design Reference

- Celebratory but calm design
- Use motion/animation for the success icon
- Confetti optional but nice-to-have

### Acceptance Criteria

- [ ] Success animation plays
- [ ] Both navigation buttons work
- [ ] Exam state is cleared
- [ ] Prevents going back to exam questions

### Labels

`phase: porting` `wave: B` `group: B.3` `skill: beginner` `area: frontend` `type: page`

---

## 📊 Wave B Summary

| Issue    | Title                    | Skill           | Group |
| -------- | ------------------------ | --------------- | ----- |
| PORT-011 | Questions API service    | 🟡 Intermediate | B.1   |
| PORT-012 | Exam submission service  | 🟡 Intermediate | B.1   |
| PORT-013 | Exam state store         | 🟡 Intermediate | B.1   |
| PORT-014 | QuestionCard component   | 🟢 Beginner     | B.2   |
| PORT-015 | ExamProgress component   | 🟢 Beginner     | B.2   |
| PORT-016 | ExamNavigation component | 🟢 Beginner     | B.2   |
| PORT-017 | ExamTimer component      | 🟡 Intermediate | B.2   |
| PORT-018 | Exam Start page          | 🟢 Beginner     | B.3   |
| PORT-019 | Exam Question page       | 🟡 Intermediate | B.3   |
| PORT-020 | Reflection screen        | 🟢 Beginner     | B.3   |
| PORT-021 | Exam Complete page       | 🟢 Beginner     | B.3   |

**Total: 11 issues** (6 Beginner, 5 Intermediate)

---

## 🔗 Dependencies

```
Group B.1 (API & State)     Group B.2 (UI Components)
        ↘                    ↙
              Group B.3 (Pages)
```

- **B.1** and **B.2** can be worked on **in parallel**
- **B.3** depends on both B.1 and B.2
