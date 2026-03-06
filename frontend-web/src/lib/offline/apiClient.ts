import { syncQueue } from './syncQueue';
import { networkMonitor } from './network';
import { db, AssessmentRecord, JournalRecord } from './db';

interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  body?: any;
  headers?: Record<string, string>;
  queueOffline?: boolean;
  priority?: 'high' | 'medium' | 'low';
}

class OfflineFirstClient {
  private baseUrl: string;

  constructor(baseUrl: string = 'http://localhost:8000/api/v1') {
    this.baseUrl = baseUrl;
  }

  async request<T>(endpoint: string, options: RequestOptions = {}): Promise<T> {
    const {
      method = 'GET',
      body,
      headers,
      queueOffline = true,
      priority = 'medium',
    } = options;

    const url = `${this.baseUrl}${endpoint}`;
    const requestHeaders: Record<string, string> = {
      'Content-Type': 'application/json',
      ...headers,
    };

    const token = this.getAuthToken();
    if (token) {
      requestHeaders['Authorization'] = `Bearer ${token}`;
    }

    if (networkMonitor.isOnline()) {
      try {
        const response = await fetch(url, {
          method,
          headers: requestHeaders,
          body: body ? JSON.stringify(body) : undefined,
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        return await response.json();
      } catch (error) {
        if (!queueOffline || method === 'GET') {
          throw error;
        }

        return await this.queueForOfflineSync(url, method, body, requestHeaders, priority);
      }
    } else {
      if (method === 'GET') {
        return await this.getFromCacheOrThrow<T>(endpoint, body);
      }

      if (queueOffline) {
        return await this.queueForOfflineSync(url, method, body, requestHeaders, priority);
      }

      throw new Error('Offline and queueOffline is disabled');
    }
  }

  private async queueForOfflineSync<T>(
    url: string,
    method: string,
    body: any,
    headers: Record<string, string>,
    priority: 'high' | 'medium' | 'low'
  ): Promise<T> {
    await syncQueue.addItem(url, method, body, headers, priority);

    throw new Error('Request queued for offline sync');
  }

  private async getFromCacheOrThrow<T>(endpoint: string, params?: any): Promise<T> {
    if (endpoint.includes('/questions')) {
      const age = params?.age || 25;
      const language = params?.language || 'en';

      const cached = await db.questionsCache
        .where('age')
        .equals(age)
        .and(item => item.language === language)
        .first();

      if (cached) {
        return cached.questions as T;
      }
    }

    if (endpoint.includes('/journal')) {
      const username = this.getCurrentUsername();
      if (username) {
        const journals = await db.journals
          .where('username')
          .equals(username)
          .reverse()
          .limit(50)
          .toArray();

        return journals as T;
      }
    }

    if (endpoint.includes('/results') || endpoint.includes('/assessments')) {
      const username = this.getCurrentUsername();
      if (username) {
        const assessments = await db.assessments
          .where('username')
          .equals(username)
          .reverse()
          .limit(20)
          .toArray();

        return assessments as T;
      }
    }

    throw new Error('No cached data available');
  }

  async saveAssessmentOffline(data: {
    username: string;
    assessmentId: string;
    answers: Record<string, number>;
    score: number;
    categoryScores: Record<string, number>;
  }): Promise<void> {
    const record: AssessmentRecord = {
      username: data.username,
      assessmentId: data.assessmentId,
      answers: data.answers,
      score: data.score,
      categoryScores: data.categoryScores,
      completedAt: new Date(),
      synced: false,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    await db.assessments.add(record);

    const queueData = {
      url: `${this.baseUrl}/assessments`,
      method: 'POST',
      body: data,
      priority: 'high' as const,
    };

    if (networkMonitor.isOnline()) {
      try {
        await this.request('/assessments', {
          method: 'POST',
          body: data,
          priority: 'high',
        });
        await db.markAsSynced('assessments', record.id!);
      } catch (error) {
        await syncQueue.addItem(
          queueData.url,
          queueData.method,
          queueData.body,
          {},
          queueData.priority
        );
      }
    } else {
      await syncQueue.addItem(
        queueData.url,
        queueData.method,
        queueData.body,
        {},
        queueData.priority
      );
    }
  }

  async saveJournalOffline(data: {
    username: string;
    journalId: string;
    content: string;
    mood?: string;
    tags?: string[];
    sentiment?: number;
  }): Promise<void> {
    const record: JournalRecord = {
      username: data.username,
      journalId: data.journalId,
      content: data.content,
      mood: data.mood,
      tags: data.tags,
      sentiment: data.sentiment,
      synced: false,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    await db.journals.add(record);

    const queueData = {
      url: `${this.baseUrl}/journal`,
      method: 'POST',
      body: data,
      priority: 'medium' as const,
    };

    if (networkMonitor.isOnline()) {
      try {
        await this.request('/journal', {
          method: 'POST',
          body: data,
          priority: 'medium',
        });
        await db.markAsSynced('journals', record.id!);
      } catch (error) {
        await syncQueue.addItem(
          queueData.url,
          queueData.method,
          queueData.body,
          {},
          queueData.priority
        );
      }
    } else {
      await syncQueue.addItem(
        queueData.url,
        queueData.method,
        queueData.body,
        {},
        queueData.priority
      );
    }
  }

  async syncPendingData(): Promise<void> {
    const pendingAssessments = await db.getPendingAssessments();
    const pendingJournals = await db.getPendingJournals();

    for (const assessment of pendingAssessments) {
      try {
        await this.request('/assessments', {
          method: 'POST',
          body: {
            assessmentId: assessment.assessmentId,
            answers: assessment.answers,
          },
          queueOffline: false,
        });

        await db.markAsSynced('assessments', assessment.id!);
      } catch (error) {
        console.error('Failed to sync assessment:', assessment.id, error);
      }
    }

    for (const journal of pendingJournals) {
      try {
        await this.request('/journal', {
          method: 'POST',
          body: {
            journalId: journal.journalId,
            content: journal.content,
            mood: journal.mood,
            tags: journal.tags,
          },
          queueOffline: false,
        });

        await db.markAsSynced('journals', journal.id!);
      } catch (error) {
        console.error('Failed to sync journal:', journal.id, error);
      }
    }

    await syncQueue.processQueue();
  }

  private getAuthToken(): string | null {
    if (typeof window === 'undefined') return null;

    return localStorage.getItem('access_token');
  }

  private getCurrentUsername(): string | null {
    if (typeof window === 'undefined') return null;

    return localStorage.getItem('username');
  }

  get<T>(endpoint: string, params?: any): Promise<T> {
    const queryString = params ? new URLSearchParams(params).toString() : '';
    return this.request<T>(queryString ? `${endpoint}?${queryString}` : endpoint);
  }

  post<T>(endpoint: string, body: any, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(endpoint, { ...options, method: 'POST', body });
  }

  put<T>(endpoint: string, body: any, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(endpoint, { ...options, method: 'PUT', body });
  }

  patch<T>(endpoint: string, body: any, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(endpoint, { ...options, method: 'PATCH', body });
  }

  delete<T>(endpoint: string, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(endpoint, { ...options, method: 'DELETE' });
  }
}

export const offlineClient = new OfflineFirstClient();
