import { useState, useEffect, useCallback } from 'react';

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

const API_BASE = '/api/v1/journal';

export function useJournal(initialParams: JournalQueryParams = {}) {
  const [entries, setEntries] = useState<JournalEntry[]>([]);
  const [entry, setEntry] = useState<JournalEntry | null>(null);
  const [total, setTotal] = useState(0);
  const [params, setParams] = useState<JournalQueryParams>(initialParams);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
  //Fetch list
  const fetchEntries = useCallback(async () => {
    setIsLoading(true);
    setError(null);

    try {
      const queryString = buildQueryString(params);
      const res = await fetch(`${API_BASE}?${queryString}`);
      if (!res.ok) throw new Error('Failed to fetch entries');

      const data: JournalResponse = await res.json();
      setEntries(data.entries);
      setTotal(data.total);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  }, [params]);

  //Fetch single entry
  const fetchEntry = async (id: number) => {
    setIsLoading(true);
    setError(null);

    try {
      //const queryString = buildQueryString(params);
      const res = await fetch(`${API_BASE}/${id}`);
      if (!res.ok) throw new Error('Failed to fetch entry');

      const data = await res.json();
      setEntry(data);
      //setTotal(data.total);
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
      const res = await fetch(API_BASE, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newEntry),
      });
      if (!res.ok) throw new Error('Failed to create entry');

      const saved = await res.json();

      setEntries((prev: JournalEntry[]) =>
        prev.map((e: JournalEntry) => (e.id === tempId ? saved : e))
      );
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
      const res = await fetch(`${API_BASE}/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates),
      });
      if (!res.ok) throw new Error('Failed to update entry');

      const updated = await res.json();

      setEntries((prev: JournalEntry[]) =>
        prev.map((e: JournalEntry) => (e.id === id ? updated : e))
      );
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
      const res = await fetch(`${API_BASE}/${id}`, {
        method: 'DELETE',
      });
      if (!res.ok) throw new Error('Failed to delete entry');
    } catch (err: any) {
      setEntries(previous);
      setError(err.message);
      throw err;
    }
  };
  useEffect(() => {
    fetchEntries();
  }, [fetchEntries]);
  return {
    entries,
    entry,
    total,
    page: params.page || 1,
    per_page: params.per_page || 10,
    totalPages: Math.ceil(total / (params.per_page || 10)),
    hasNextPage: (params.page || 1) * (params.per_page || 10) < total,
    hasPrevPage: (params.page || 1) > 1,
    isLoading,
    error,
    setParams,
    setPage: (p: number) => setParams((prev) => ({ ...prev, page: p })),
    setFilters: (f: Partial<JournalQueryParams>) =>
      setParams((prev) => ({ ...prev, ...f, page: 1 })),
    refetch: fetchEntries,
    loadMore: () => setParams((prev) => ({ ...prev, page: (prev.page || 1) + 1 })),
    fetchEntry,
    createEntry,
    updateEntry,
    deleteEntry,
  };
}
