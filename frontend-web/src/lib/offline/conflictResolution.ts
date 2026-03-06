import { db, AssessmentRecord, JournalRecord } from './db';

export interface Conflict<T> {
  id: string;
  type: 'assessment' | 'journal' | 'settings';
  local: T;
  remote: T;
  conflictReason: string;
}

export type ConflictResolution = 'local' | 'remote' | 'merge';

export interface ResolverResult {
  resolved: boolean;
  data?: any;
  resolution?: ConflictResolution;
}

class ConflictResolver {
  async resolveAssessment(
    local: AssessmentRecord,
    remote: any
  ): Promise<ResolverResult> {
    if (local.assessmentId !== remote.id) {
      return {
        resolved: false,
      };
    }

    if (local.synced) {
      return {
        resolved: true,
        data: remote,
        resolution: 'remote',
      };
    }

    const localTime = new Date(local.updatedAt).getTime();
    const remoteTime = new Date(remote.updated_at || remote.updatedAt).getTime();

    if (remoteTime > localTime + 60000) {
      return {
        resolved: true,
        data: remote,
        resolution: 'remote',
      };
    }

    if (localTime > remoteTime + 60000) {
      return {
        resolved: true,
        data: {
          ...remote,
          answers: local.answers,
          score: local.score,
          categoryScores: local.categoryScores,
        },
        resolution: 'local',
      };
    }

    return {
      resolved: true,
      data: {
        ...remote,
        answers: local.answers,
        score: local.score,
        categoryScores: local.categoryScores,
      },
      resolution: 'local',
    };
  }

  async resolveJournal(
    local: JournalRecord,
    remote: any
  ): Promise<ResolverResult> {
    if (local.journalId !== remote.id) {
      return {
        resolved: false,
      };
    }

    if (local.synced) {
      return {
        resolved: true,
        data: remote,
        resolution: 'remote',
      };
    }

    const localTime = new Date(local.updatedAt).getTime();
    const remoteTime = new Date(remote.updated_at || remote.updatedAt).getTime();

    if (Math.abs(localTime - remoteTime) < 1000) {
      return {
        resolved: true,
        data: {
          ...remote,
          content: local.content,
          mood: local.mood,
        },
        resolution: 'local',
      };
    }

    if (remoteTime > localTime) {
      return {
        resolved: true,
        data: remote,
        resolution: 'remote',
      };
    }

    const mergedContent = this.mergeJournalContent(local.content, remote.content);

    return {
      resolved: true,
      data: {
        ...remote,
        content: mergedContent,
        mood: local.mood,
        tags: [...new Set([...(local.tags || []), ...(remote.tags || [])])],
      },
      resolution: 'merge',
    };
  }

  async resolveSettings(
    local: any,
    remote: any
  ): Promise<ResolverResult> {
    const localTime = new Date(local.updatedAt).getTime();
    const remoteTime = new Date(remote.updated_at || remote.updatedAt).getTime();

    if (remoteTime > localTime) {
      return {
        resolved: true,
        data: remote,
        resolution: 'remote',
      };
    }

    if (localTime > remoteTime) {
      return {
        resolved: true,
        data: local.settings,
        resolution: 'local',
      };
    }

    const mergedSettings = {
      ...remote,
      ...local.settings,
    };

    return {
      resolved: true,
      data: mergedSettings,
      resolution: 'merge',
    };
  }

  private mergeJournalContent(local: string, remote: string): string {
    if (local === remote) {
      return local;
    }

    const localLength = local.length;
    const remoteLength = remote.length;

    if (remoteLength > localLength * 1.5) {
      return remote;
    }

    if (localLength > remoteLength * 1.5) {
      return local;
    }

    return local;
  }

  async detectConflicts(): Promise<Conflict<any>[]> {
    const conflicts: Conflict<any>[] = [];

    const pendingAssessments = await db.getPendingAssessments();
    const pendingJournals = await db.getPendingJournals();

    return conflicts;
  }

  async autoResolveAll(): Promise<ResolverResult[]> {
    const results: ResolverResult[] = [];

    const pendingAssessments = await db.getPendingAssessments();
    const pendingJournals = await db.getPendingJournals();

    for (const assessment of pendingAssessments) {
      const result = await this.resolveAssessment(assessment, assessment);
      results.push(result);

      if (result.resolved && result.data) {
        await db.assessments.update(assessment.id!, {
          synced: true,
        });
      }
    }

    for (const journal of pendingJournals) {
      const result = await this.resolveJournal(journal, journal);
      results.push(result);

      if (result.resolved && result.data) {
        await db.journals.update(journal.id!, {
          synced: true,
        });
      }
    }

    return results;
  }
}

export const conflictResolver = new ConflictResolver();
