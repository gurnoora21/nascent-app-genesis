export interface ProcessingItem {
  id: string;
  batch_id: string;
  item_type: string;
  item_id: string;
  status: 'pending' | 'processing' | 'completed' | 'error';
  priority?: number;
  retry_count?: number;
  last_error?: string;
  metadata?: any;
}

export interface RateLimit {
  api_name: string;
  endpoint: string;
  requests_limit?: number;
  requests_remaining?: number;
  reset_at?: string;
  last_response?: any;
}

export interface DataQualityScore {
  entity_type: string;
  entity_id: string;
  source: string;
  quality_score: number;
  completeness_score?: number;
  accuracy_score?: number;
  metadata?: any;
}

export interface WorkerHeartbeat {
  worker_id: string;
  worker_type: string;
  status: string;
  current_batch_id?: string;
  metadata?: any;
}

export interface SystemLogEntry {
  level: 'info' | 'warning' | 'error' | 'debug';
  component: string;
  message: string;
  context?: any;
  trace_id?: string;
}

// Add MetricDimensions type for metrics tracking
export type MetricDimensions = {
  [key: string]: string | number | boolean | null;
};

// Add DeadLetterItem type
export type DeadLetterItem = {
  id: string;
  original_item_id: string;
  original_batch_id: string;
  item_type: string;
  item_id: string;
  error_message?: string;
  metadata?: any;
  retry_count?: number;
  created_at: string;
  updated_at: string;
};
