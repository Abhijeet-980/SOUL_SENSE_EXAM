import { useState, useEffect } from 'react';
import { useSearchParams } from 'next/navigation';
import { apiClient } from '@/lib/api/client';

interface DashboardStatistics {
  historical_trends: Array<{
    id: number;
    timestamp: string;
    total_score: number;
    sentiment_score?: number;
  }>;
}

export function useFetchDashboardData() {
  const searchParams = useSearchParams();
  const [data, setData] = useState<DashboardStatistics | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const timeframe = searchParams.get('timeframe') || '30d';
  const examType = searchParams.get('exam_type');
  const sentiment = searchParams.get('sentiment');

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const params = new URLSearchParams();
        params.set('timeframe', timeframe);
        if (examType) params.set('exam_type', examType);
        if (sentiment) params.set('sentiment', sentiment);

        const response = await apiClient<DashboardStatistics>(
          `/analytics/statistics?${params.toString()}`
        );
        setData(response);
      } catch (err) {
        console.error('Error fetching dashboard data:', err);
        setError(err instanceof Error ? err.message : 'Failed to fetch data');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [timeframe, examType, sentiment]);

  return { data, loading, error };
}