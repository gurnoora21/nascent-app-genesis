
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

// NEW TYPES for paginated batch system

// Paginated batch types
export type BatchType = 
  | 'process_artists'       // Main artist processing batch
  | 'process_albums'        // Main album processing batch 
  | 'process_tracks'        // Main tracks processing batch
  | 'process_album_page'    // Process a single page of albums for an artist
  | 'process_track_page'    // Process a single page of tracks for an album
  | 'process_producers';    // Main producer processing batch

// Batch status type
export type BatchStatus = 
  | 'pending'       // Waiting to be processed
  | 'processing'    // Currently being processed
  | 'completed'     // Successfully processed
  | 'partial'       // Partially completed (some items failed)
  | 'error'         // Failed to process
  | 'cancelled';    // Manually cancelled

// Item type for processing queue
export type ItemType =
  | 'artist'            // Main artist record
  | 'album'             // Album record 
  | 'album_for_tracks'  // Album to process for tracks
  | 'track'             // Track record
  | 'producer';         // Producer record

// Data sources
export type DataSource =
  | 'spotify'
  | 'discogs'
  | 'genius'
  | 'manual'
  | 'pipeline';

// Confidence level for attributions
export type ConfidenceLevel =
  | 'high'
  | 'medium'
  | 'low'
  | 'unknown';

// Error classification for better retry policies
export type ErrorType =
  | 'transient'     // Temporary error, can retry (network issues, timeouts)
  | 'rate_limit'    // API rate limit hit, retry after backoff
  | 'validation'    // Data validation error, fix before retry
  | 'authentication' // Auth error, needs credential refresh
  | 'authorization'  // Permission error, likely won't succeed with retry
  | 'not_found'      // Resource not found, won't succeed with retry
  | 'server'         // Server error, might succeed with retry
  | 'unknown';       // Unclassified error

// Pipeline alert type
export type AlertType =
  | 'error_rate'       // High error rate detected
  | 'rate_limit'       // API rate limit hit
  | 'circuit_breaker'  // Circuit breaker opened
  | 'backpressure'     // Backpressure detected
  | 'dlq_threshold'    // Dead letter queue threshold exceeded
  | 'stalled_batches'; // Stalled batches detected
