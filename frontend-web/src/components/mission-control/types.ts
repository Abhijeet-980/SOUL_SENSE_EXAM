export type MissionItemType = 'issue' | 'pr';
export type MissionStatus = 'Backlog' | 'Ready' | 'In Progress' | 'In Review' | 'Done';
export type MissionPriority = 'High' | 'Normal' | 'Low';

export interface MissionAssignee {
  login: string;
  avatar: string;
}

export interface MissionItem {
  id: string;
  number: number;
  type: MissionItemType;
  title: string;
  status: MissionStatus;
  priority: MissionPriority;
  domain: string;
  assignee: MissionAssignee | null;
  labels: string[];
  url: string;
  updated_at: string;
  source_branch?: string;
}

export interface MissionStats {
  total: number;
  backlog: number;
  in_progress: number;
  done: number;
}

export interface MissionControlData {
  items: MissionItem[];
  stats: MissionStats;
}
