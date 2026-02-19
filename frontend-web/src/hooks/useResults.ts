import { useState, useCallback } from 'react';
import { resultsApi } from '../lib/api/results';
import { DetailedExamResult, AssessmentResponse } from '../types/results';
import { ApiError } from '../lib/api/errors';

interface UseResultsOptions {
  initialPage?: number;
  initialPageSize?: number;
  autoFetch?: boolean;
}

/**
 * Custom hook for managing assessment results and history.
 * Provides state for history list, detailed breakdowns, and loading/error status.
 */
export function useResults(options: UseResultsOptions = {}) {
  const [history, setHistory] = useState<AssessmentResponse[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [currentPage, setCurrentPage] = useState(options.initialPage || 1);
  const [pageSize, setPageSize] = useState(options.initialPageSize || 10);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detailedResult, setDetailedResult] = useState<DetailedExamResult | null>(null);

  /**
   * Fetches paginated assessment history.
   */
  const fetchHistory = useCallback(
    async (page = currentPage, size = pageSize) => {
      setLoading(true);
      setError(null);
      try {
        const data = await resultsApi.getHistory(page, size);
        setHistory(data.assessments);
        setTotalCount(data.total);
        setCurrentPage(page);
        setPageSize(size);
        return data;
      } catch (err) {
        const message =
          err instanceof ApiError ? err.message : 'Failed to fetch assessment history';
        setError(message);
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [currentPage, pageSize]
  );

  /**
   * Fetches detailed results for a specific assessment.
   */
  const fetchDetailedResult = useCallback(async (assessmentId: number) => {
    setLoading(true);
    setError(null);
    try {
      const data = await resultsApi.getDetailedResult(assessmentId);
      setDetailedResult(data);
      return data;
    } catch (err) {
      const message = err instanceof ApiError ? err.message : 'Failed to fetch detailed results';
      setError(message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  /**
   * Resets the detailed result state.
   */
  const clearDetailedResult = useCallback(() => {
    setDetailedResult(null);
  }, []);

  return {
    // State
    history,
    totalCount,
    currentPage,
    pageSize,
    loading,
    error,
    detailedResult,

    // Actions
    fetchHistory,
    fetchDetailedResult,
    clearDetailedResult,
    setCurrentPage,
    setPageSize,
  };
}
