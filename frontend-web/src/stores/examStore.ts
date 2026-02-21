import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';
import { Question } from '@/lib/api/questions';

interface ExamState {
  questions: Question[];
  currentIndex: number;
  answers: Record<number, number>; // questionId -> answer value
  startTime: string | null; // ISO string for easy storage
  isCompleted: boolean;
  _hasHydrated: boolean; // For handling Next.js hydration

  // Actions
  setQuestions: (questions: Question[]) => void;
  setAnswer: (questionId: number, value: number) => void;
  nextQuestion: () => void;
  previousQuestion: () => void;
  completeExam: () => void;
  resetExam: () => void;
  setHasHydrated: (state: boolean) => void;

  // Selectors (Getters)
  getCurrentQuestion: () => Question | null;
  getIsFirstQuestion: () => boolean;
  getIsLastQuestion: () => boolean;
  getAnsweredCount: () => number;
  getProgressPercentage: () => number;
  getResults: () => Array<{ question: Question; selectedValue: number | null }>;
}

export const useExamStore = create<ExamState>()(
  persist(
    (set, get) => ({
      questions: [],
      currentIndex: 0,
      answers: {},
      startTime: null,
      isCompleted: false,
      _hasHydrated: false,

      setQuestions: (questions) =>
        set({
          questions,
          currentIndex: 0,
          answers: {},
          startTime: new Date().toISOString(),
          isCompleted: false,
        }),

      setAnswer: (questionId, value) =>
        set((state) => ({
          answers: {
            ...state.answers,
            [questionId]: value,
          },
        })),

      nextQuestion: () =>
        set((state) => ({
          currentIndex: Math.min(state.currentIndex + 1, Math.max(0, state.questions.length - 1)),
        })),

      previousQuestion: () =>
        set((state) => ({
          currentIndex: Math.max(state.currentIndex - 1, 0),
        })),

      completeExam: () =>
        set({
          isCompleted: true,
        }),

      resetExam: () =>
        set({
          questions: [],
          currentIndex: 0,
          answers: {},
          startTime: null,
          isCompleted: false,
        }),

      setHasHydrated: (state) => set({ _hasHydrated: state }),

      // Selectors
      getCurrentQuestion: () => {
        const { questions, currentIndex } = get();
        return questions[currentIndex] || null;
      },

      getIsFirstQuestion: () => get().currentIndex === 0,

      getIsLastQuestion: () => {
        const { questions, currentIndex } = get();
        return questions.length > 0 && currentIndex === questions.length - 1;
      },

      getAnsweredCount: () => Object.keys(get().answers).length,

      getProgressPercentage: () => {
        const { questions, answers } = get();
        if (questions.length === 0) return 0;
        return Math.round((Object.keys(answers).length / questions.length) * 100);
      },
      getResults: () => {
        const { questions, answers } = get();
        return questions.map((q) => ({
          question: q,
          selectedValue: answers[q.id] ?? null,
        }));
      },
    }),
    {
      name: 'exam-storage',
      storage: createJSONStorage(() => sessionStorage),
      onRehydrateStorage: () => (state: ExamState | undefined) => {
        state?.setHasHydrated(true);
      },
    }
  )
);
