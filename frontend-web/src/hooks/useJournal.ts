import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiClient } from '@/lib/api/client';

export interface JournalEntry {
  id: number;
  content: string;
  mood_rating: number;
  energy_level: number;
  stress_level: number;
  tags: string[];
  sentiment_score: number;
  created_at: string;
  updated_at: string;
  timestamp: string; // Add timestamp as it's used for keyset pagination
}

export interface JournalQueryParams {
  limit?: number;
  cursor?: string;
  startDate?: string;
  endDate?: string;
  moodMin?: number;
  moodMax?: number;
  tags?: string[];
  search?: string;
}

interface JournalCursorResponse {
  data: JournalEntry[];
  next_cursor: string | null;
  has_more: boolean;
}

const API_BASE = '/journal'; // apiClient prepends the rest

export function useJournal(filters: JournalQueryParams = {}) {
  const queryClient = useQueryClient();

  // Helper to build query string
  const buildQueryString = (params: Record<string, any>) => {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        if (Array.isArray(value)) {
          if (value.length > 0) query.append(key, value.join(','));
        } else {
          query.append(key, String(value));
        }
      }
    });
    return query.toString();
  };

  // Infinite Scroll Query
  const {
    data,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    isLoading,
    isError,
    error,
    refetch,
  } = useInfiniteQuery({
    queryKey: ['journals', filters],
    queryFn: async ({ pageParam = null }) => {
      const params: any = {
        limit: filters.limit || 25,
        cursor: pageParam as string | null,
      };

      if (filters.startDate) params.start_date = filters.startDate;
      if (filters.endDate) params.end_date = filters.endDate;
      if (filters.search) params.search = filters.search;
      if (filters.tags && filters.tags.length > 0) params.tags = filters.tags;

      const queryString = buildQueryString(params);
      return apiClient<JournalCursorResponse>(`${API_BASE}/?${queryString}`);
    },
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    initialPageParam: null as string | null,
    staleTime: 30000, // 30 seconds
  });

  // Flatten entries from all pages
  const entries = data?.pages.flatMap((page) => page.data) ?? [];

  // Mutations
  const createMutation = useMutation({
    mutationFn: async (newEntry: any) => {
      return apiClient.post(API_BASE + '/', newEntry);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['journals'] });
    },
  });

  const updateMutation = useMutation({
    mutationFn: async ({ id, updates }: { id: number; updates: any }) => {
      return apiClient.put(`${API_BASE}/${id}`, updates);
    },
    onSuccess: (updatedData: any) => {
      queryClient.invalidateQueries({ queryKey: ['journals'] });
      queryClient.setQueryData(['journal', updatedData.id], updatedData);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: number) => {
      return apiClient.delete(`${API_BASE}/${id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['journals'] });
    },
  });

  // Single Entry Fetch (standard query)
  const fetchEntry = async (id: number) => {
    return queryClient.fetchQuery({
      queryKey: ['journal', id],
      queryFn: async () => {
        return apiClient(`${API_BASE}/${id}`);
      }
    });
  };

  return {
    entries,
    isLoading,
    isError,
    error: error instanceof Error ? error.message : 'Unknown error',
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
    refetch,
    loadMore: fetchNextPage,
    createEntry: createMutation.mutateAsync,
    updateEntry: (id: number, updates: any) =>
      updateMutation.mutateAsync({ id, updates }),
    deleteEntry: deleteMutation.mutateAsync,
    fetchEntry,
    total: entries.length,
  };
}
