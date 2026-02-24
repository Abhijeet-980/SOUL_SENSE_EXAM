import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';
import { Question } from '@/lib/api/questions';

interface ExamState {
  questions: Question[];
  currentQuestionIndex: number;
  answers: Record<number, number>; // questionId -> answer value
  startTime: string | null; // ISO string for easy storage
  isCompleted: boolean;
  examError: string | null;
  isLoading: boolean;
  currentExamId: string | null; // For namespacing storage
  _hasHydrated: boolean; // For handling Next.js hydration

  // Actions
  setQuestions: (questions: Question[], examId: string) => void;
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
      currentQuestionIndex: 0,
      answers: {},
      startTime: null,
      isCompleted: false,
      examError: null,
      isLoading: false,
      currentExamId: null,
      _hasHydrated: false,

      setQuestions: (questions, examId) => {
        const currentState = get();
        if (currentState.currentExamId !== examId) {
          // Different exam, reset state
          set({
            questions,
            currentQuestionIndex: 0,
            answers: {},
            startTime: new Date().toISOString(),
            isCompleted: false,
            currentExamId: examId,
          });
        } else {
          // Same exam, just update questions
          set({
            questions,
            currentQuestionIndex: 0,
            answers: currentState.answers, // Keep existing answers if same exam
            startTime: currentState.startTime || new Date().toISOString(),
            isCompleted: false,
          });
        }
      },

      setAnswer: (questionId, value) =>
        set((state) => ({
          answers: {
            ...state.answers,
            [questionId]: value,
          },
        })),

      nextQuestion: () =>
        set((state) => ({
          currentQuestionIndex: Math.min(state.currentQuestionIndex + 1, Math.max(0, state.questions.length - 1)),
        })),

      previousQuestion: () =>
        set((state) => ({
          currentQuestionIndex: Math.max(state.currentQuestionIndex - 1, 0),
        })),

      completeExam: () =>
        set({
          isCompleted: true,
        }),

      resetExam: () =>
        set((state) => ({
          questions: [],
          currentQuestionIndex: 0,
          answers: {},
          startTime: null,
          isCompleted: false,
          examError: null,
          isLoading: false,
          currentExamId: null,
        })),

      setHasHydrated: (state) => set({ _hasHydrated: state }),

      // Selectors
      getCurrentQuestion: () => {
        const { questions, currentQuestionIndex } = get();
        return questions[currentQuestionIndex] || null;
      },

      getIsFirstQuestion: () => get().currentQuestionIndex === 0,

      getIsLastQuestion: () => {
        const { questions, currentQuestionIndex } = get();
        return questions.length > 0 && currentQuestionIndex === questions.length - 1;
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
