import { useState, useCallback } from 'react';
import { useQuery, useQueryClient, useMutation } from '@tanstack/react-query';
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
}
export interface JournalQueryParams {
  page?: number;
  per_page?: number;
  start_date?: string;
  end_date?: string;
  mood_min?: number;
  mood_max?: number;
  tags?: string[];
  search?: string;
}
interface JournalResponse {
  entries: JournalEntry[];
  total: number;
  page: number;
  per_page: number;
}

const API_BASE = '/journal';

export function useJournal(initialParams: JournalQueryParams = {}, suspense = false) {
  const queryClient = useQueryClient();
  const [entry, setEntry] = useState<JournalEntry | null>(null);
  const [params, setParams] = useState<JournalQueryParams>(initialParams);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [entries, setEntries] = useState<JournalEntry[]>([]);

  //build query string
  const buildQueryString = (params: JournalQueryParams) => {
    const query = new URLSearchParams();

    (Object.keys(params) as (keyof JournalQueryParams)[]).forEach((key) => {
      const value = params[key];
      if (value === undefined || value === null) return;

      if (key === 'tags' && Array.isArray(value)) {
        query.append('tags', value.join(','));
      } else {
        query.append(key, String(value));
      }
    });
    return query.toString();
  };

  const {
    data: journalData,
    isLoading: isQueryLoading,
    refetch,
  } = useQuery({
    queryKey: ['journal', 'entries', params],
    queryFn: async () => {
      const queryString = buildQueryString(params);
      return await apiClient<JournalResponse>(`${API_BASE}?${queryString}`);
    },
  });

  const queryEntries = journalData?.entries || [];
  const queryTotal = journalData?.total || 0;

  //Fetch single entry
  const fetchEntry = async (id: number) => {
    setIsLoading(true);
    setError(null);

    try {
      const data = await apiClient<JournalEntry>(`${API_BASE}/${id}`);
      setEntry(data);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  //create entry
  const createEntry = async (newEntry: Partial<JournalEntry>) => {
    const tempId = Date.now();

    const optimisticEntry: JournalEntry = {
      id: tempId,
      content: newEntry.content || '',
      mood_rating: newEntry.mood_rating || 0,
      energy_level: newEntry.energy_level || 0,
      stress_level: newEntry.stress_level || 0,
      tags: newEntry.tags || [],
      sentiment_score: 0,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    setEntries((prev: JournalEntry[]) => [optimisticEntry, ...prev]);

    try {
      const saved = await apiClient<JournalEntry>(API_BASE, {
        method: 'POST',
        body: JSON.stringify(newEntry),
      });

      setEntries((prev: JournalEntry[]) =>
        prev.map((e: JournalEntry) => (e.id === tempId ? saved : e))
      );
      // Let React Query know the data is stale
      queryClient.invalidateQueries({ queryKey: ['journal', 'entries'] });
      return saved;
    } catch (err: any) {
      setEntries((prev: JournalEntry[]) => prev.filter((e) => e.id !== tempId));
      setError(err.message);
      throw err;
    }
  };

  //update entry
  const updateEntry = async (id: number, updates: Partial<JournalEntry>) => {
    const previous = entries;

    setEntries((prev: JournalEntry[]) =>
      prev.map((e: JournalEntry) => (e.id === id ? { ...e, ...updates } : e))
    );

    try {
      const updated = await apiClient<JournalEntry>(`${API_BASE}/${id}`, {
        method: 'PUT',
        body: JSON.stringify(updates),
      });

      setEntries((prev: JournalEntry[]) =>
        prev.map((e: JournalEntry) => (e.id === id ? updated : e))
      );
      // Let React Query know the data is stale
      queryClient.invalidateQueries({ queryKey: ['journal', 'entries'] });
      return updated;
    } catch (err: any) {
      setEntries(previous);
      setError(err.message);
      throw err;
    }
  };

  //delete entry
  const deleteEntry = async (id: number) => {
    const previous = entries;

    setEntries((prev: JournalEntry[]) => prev.filter((e) => e.id !== id));
    try {
      await apiClient(`${API_BASE}/${id}`, {
        method: 'DELETE',
      });
      // Let React Query know the data is stale
      queryClient.invalidateQueries({ queryKey: ['journal', 'entries'] });
    } catch (err: any) {
      setEntries(previous);
      setError(err.message);
      throw err;
    }
  };
  return {
    entries: queryEntries,
    entry,
    total: queryTotal,
    page: params.page || 1,
    per_page: params.per_page || 10,
    totalPages: Math.ceil(queryTotal / (params.per_page || 10)),
    hasNextPage: (params.page || 1) * (params.per_page || 10) < queryTotal,
    hasPrevPage: (params.page || 1) > 1,
    isLoading,
    error,
    setParams,
    setPage: (p: number) => setParams((prev) => ({ ...prev, page: p })),
    setFilters: (f: Partial<JournalQueryParams>) =>
      setParams((prev) => ({ ...prev, ...f, page: 1 })),
    refetch,
    loadMore: () => setParams((prev) => ({ ...prev, page: (prev.page || 1) + 1 })),
    fetchEntry,
    createEntry,
    updateEntry,
    deleteEntry,
  };
}
