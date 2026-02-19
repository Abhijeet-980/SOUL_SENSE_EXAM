export interface CategoryScore {
  category_name: string;
  score: number;
  max_score: number;
  percentage: number;
}

export interface Recommendation {
  category_name: string;
  message: string;
  priority: 'high' | 'medium' | 'low';
}

export interface DetailedExamResult {
  assessment_id: number;
  total_score: number;
  max_possible_score: number;
  overall_percentage: number;
  timestamp: string;
  category_breakdown: CategoryScore[];
  recommendations: Recommendation[];
}

export interface AssessmentResponse {
  id: number;
  username: string;
  total_score: number;
  sentiment_score?: number;
  age?: number;
  detailed_age_group?: string;
  timestamp: string;
}

export interface AssessmentListResponse {
  total: number;
  assessments: AssessmentResponse[];
  page: number;
  page_size: number;
}
