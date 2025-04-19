
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";
import { 
  ProcessingItem, 
  WorkerHeartbeat, 
  RateLimit, 
  DataQualityScore, 
  SystemLogEntry,
  MetricDimensions
} from "./types.ts";

const SUPABASE_URL = "https://nsxxzhhbcwzatvlulfyp.supabase.co";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";

// Initialize Supabase client with service role for admin access to DB
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Batch Management Utils
export async function claimProcessingBatch(
  batchType: string,
  workerId: string,
  claimTtlSeconds = 3600
): Promise<string | null> {
  try {
    const { data: claimedBatchId } = await supabase.rpc('claim_processing_batch', {
      p_batch_type: batchType,
      p_worker_id: workerId,
      p_claim_ttl_seconds: claimTtlSeconds
    });
    
    return claimedBatchId || null;
  } catch (error) {
    console.error('Error claiming batch:', error);
    return null;
  }
}

export async function releaseProcessingBatch(
  batchId: string,
  workerId: string,
  status: 'completed' | 'processing' | 'error'
): Promise<boolean> {
  try {
    const { data: success } = await supabase.rpc('release_processing_batch', {
      p_batch_id: batchId,
      p_worker_id: workerId,
      p_status: status
    });
    
    return success || false;
  } catch (error) {
    console.error('Error releasing batch:', error);
    return false;
  }
}

// Rate Limiting Utils
export async function isApiRateLimited(
  apiName: string,
  endpoint: string
): Promise<boolean> {
  try {
    const { data: rateLimitInfo } = await supabase
      .from('api_rate_limits')
      .select('*')
      .eq('api_name', apiName)
      .eq('endpoint', endpoint)
      .single();

    if (!rateLimitInfo) return false;

    if (rateLimitInfo.reset_at && new Date(rateLimitInfo.reset_at) <= new Date()) {
      return false;
    }

    return Boolean(
      rateLimitInfo.requests_remaining !== null && 
      rateLimitInfo.requests_remaining <= 0
    );
  } catch (error) {
    console.error('Error checking rate limit:', error);
    return false;
  }
}

export async function updateRateLimit(
  apiName: string,
  endpoint: string,
  headers: Headers,
  response: any
): Promise<void> {
  try {
    let limit = null;
    let remaining = null;
    let resetAt = null;

    // Parse rate limit headers based on API
    if (apiName === 'spotify') {
      limit = parseInt(headers.get('x-ratelimit-limit') || '0');
      remaining = parseInt(headers.get('x-ratelimit-remaining') || '0');
      const resetSeconds = parseInt(headers.get('Retry-After') || '0');
      resetAt = resetSeconds > 0 
        ? new Date(Date.now() + resetSeconds * 1000).toISOString()
        : null;
    } else if (apiName === 'discogs') {
      limit = parseInt(headers.get('x-discogs-ratelimit') || '0');
      remaining = parseInt(headers.get('x-discogs-ratelimit-remaining') || '0');
      const resetSeconds = parseInt(headers.get('x-discogs-ratelimit-used') || '0');
      resetAt = new Date(Date.now() + resetSeconds * 1000).toISOString();
    }

    if (limit !== null || remaining !== null || resetAt !== null) {
      await supabase
        .from('api_rate_limits')
        .upsert({
          api_name: apiName,
          endpoint: endpoint,
          requests_limit: limit,
          requests_remaining: remaining,
          reset_at: resetAt,
          last_response: response,
          updated_at: new Date().toISOString()
        }, {
          onConflict: 'api_name,endpoint'
        });
    }
  } catch (error) {
    console.error('Error updating rate limit:', error);
  }
}

// Error Handling Utils
export async function logProcessingError(
  errorType: string,
  source: string,
  message: string,
  error: Error,
  context?: any,
  itemId?: string,
  itemType?: string
): Promise<string> {
  try {
    const { data } = await supabase.rpc('log_error', {
      p_error_type: errorType,
      p_source: source,
      p_message: message,
      p_stack_trace: error.stack || '',
      p_context: context || {},
      p_item_id: itemId,
      p_item_type: itemType
    });
    
    return data || '';
  } catch (err) {
    console.error('Error logging to database:', err);
    return '';
  }
}

// Item Processing Utils
export async function getItemsToProcess(
  batchId: string,
  batchSize = 10
): Promise<any[]> {
  try {
    const { data: items } = await supabase
      .from('processing_items')
      .select('*')
      .eq('batch_id', batchId)
      .eq('status', 'pending')
      .order('priority', { ascending: false })
      .order('created_at', { ascending: true })
      .limit(batchSize);

    return items || [];
  } catch (error) {
    console.error('Error getting items to process:', error);
    return [];
  }
}

export async function updateItemStatus(
  itemId: string,
  status: string,
  metadata?: any,
  lastError?: string
): Promise<void> {
  try {
    const updates: Record<string, any> = {
      status,
      updated_at: new Date().toISOString()
    };

    if (metadata) {
      updates.metadata = metadata;
    }

    if (lastError) {
      updates.last_error = lastError;
      updates.retry_count = supabase.rpc('increment', { row_id: itemId });
    }

    await supabase
      .from('processing_items')
      .update(updates)
      .eq('id', itemId);
    
    // If item has reached max retries and status is error, move to dead letter queue
    if (status === 'error') {
      const { data: item } = await supabase
        .from('processing_items')
        .select('retry_count')
        .eq('id', itemId)
        .single();
      
      if (item && item.retry_count >= 3) {
        await supabase.rpc('move_to_dead_letter_queue', { p_item_id: itemId });
        await incrementMetric('items_moved_to_dlq', 1, { 
          item_id: itemId, 
          error: lastError?.substring(0, 100) 
        });
      }
    }
  } catch (error) {
    console.error('Error updating item status:', error);
  }
}

// Worker Management Utils
export async function updateWorkerHeartbeat(
  workerId: string,
  workerType: string,
  status: string,
  currentBatchId?: string,
  metadata?: any
): Promise<void> {
  try {
    await supabase
      .from('worker_heartbeats')
      .upsert({
        worker_id: workerId,
        worker_type: workerType,
        last_heartbeat: new Date().toISOString(),
        status,
        current_batch_id: currentBatchId,
        metadata
      }, {
        onConflict: 'worker_id'
      });
  } catch (error) {
    console.error('Error updating worker heartbeat:', error);
  }
}

// Data Quality Utils
export async function updateDataQualityScore(
  entityType: string,
  entityId: string,
  source: string,
  qualityScore: number,
  completenessScore?: number,
  accuracyScore?: number,
  metadata?: any
): Promise<void> {
  try {
    await supabase
      .from('data_quality_scores')
      .upsert({
        entity_type: entityType,
        entity_id: entityId,
        source,
        quality_score: qualityScore,
        completeness_score: completenessScore,
        accuracy_score: accuracyScore,
        metadata,
        last_verified_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }, {
        onConflict: 'entity_type,entity_id,source'
      });
  } catch (error) {
    console.error('Error updating data quality score:', error);
  }
}

// Structured Logging Utils
export async function logSystemEvent(
  level: 'info' | 'warning' | 'error' | 'debug',
  component: string,
  message: string,
  context?: any,
  traceId?: string
): Promise<void> {
  try {
    await supabase
      .from('system_logs')
      .insert({
        log_level: level,
        component,
        message,
        context,
        trace_id: traceId
      });
  } catch (error) {
    console.error('Error logging system event:', error);
  }
}

// Add function for validating batch integrity
export async function validateBatchIntegrity(batchId: string): Promise<{ valid: boolean; reason?: string }> {
  try {
    // Check if batch exists
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", batchId)
      .single();
    
    if (batchError || !batch) {
      return { valid: false, reason: `Batch ${batchId} not found or error retrieving it: ${batchError?.message}` };
    }
    
    // Check batch status
    if (batch.status !== 'pending' && batch.status !== 'processing') {
      return { valid: false, reason: `Batch status is ${batch.status}, not pending or processing` };
    }
    
    // Check item count
    const { count, error: countError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId);
    
    if (countError) {
      return { valid: false, reason: `Error checking item count: ${countError.message}` };
    }
    
    if (count === 0) {
      return { valid: false, reason: `Batch has no items to process` };
    }
    
    return { valid: true };
  } catch (error) {
    console.error('Error validating batch integrity:', error);
    return { valid: false, reason: `Exception during validation: ${error.message}` };
  }
}

// New Dead Letter Queue functions
export async function moveToDeadLetterQueue(
  itemId: string,
  errorMessage: string
): Promise<boolean> {
  try {
    const { data, error } = await supabase.rpc('move_to_dead_letter_queue', {
      p_item_id: itemId
    });
    
    if (error) {
      console.error('Error moving item to dead letter queue:', error);
      return false;
    }
    
    return data || false;
  } catch (error) {
    console.error('Error calling move_to_dead_letter_queue:', error);
    return false;
  }
}

export async function requeueDeadLetterItem(
  deadLetterId: string,
  batchId?: string
): Promise<string | null> {
  try {
    const { data, error } = await supabase.rpc('requeue_dead_letter_item', {
      p_dead_letter_id: deadLetterId,
      p_batch_id: batchId
    });
    
    if (error) {
      console.error('Error requeuing dead letter item:', error);
      return null;
    }
    
    return data;
  } catch (error) {
    console.error('Error calling requeue_dead_letter_item:', error);
    return null;
  }
}

// Metrics tracking functions
export async function incrementMetric(
  metricName: string,
  increment: number = 1,
  dimensions?: MetricDimensions
): Promise<void> {
  try {
    await supabase.rpc('increment_metric', {
      p_metric_name: metricName,
      p_increment: increment,
      p_dimensions: dimensions
    });
  } catch (error) {
    console.error(`Error incrementing metric ${metricName}:`, error);
  }
}

// Sleep utility for controlled throttling
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Config object for centralized configuration
export const config = {
  // Batch sizing
  BATCH_SIZE_ALBUMS: Number(Deno.env.get("BATCH_SIZE_ALBUMS") || 25),
  BATCH_SIZE_TRACKS: Number(Deno.env.get("BATCH_SIZE_TRACKS") || 50),
  BATCH_SIZE_ARTISTS: Number(Deno.env.get("BATCH_SIZE_ARTISTS") || 10),
  
  // Concurrency controls
  SPOTIFY_CONCURRENCY: Number(Deno.env.get("SPOTIFY_CONCURRENCY") || 2),
  DB_WRITE_BATCH_SIZE: Number(Deno.env.get("DB_WRITE_BATCH_SIZE") || 50),
  
  // Rate limiting
  BASE_THROTTLE_MS: Number(Deno.env.get("RATE_LIMIT_MS") || 1000),
  MAX_THROTTLE_MS: Number(Deno.env.get("MAX_RATE_LIMIT_MS") || 5000),
  THROTTLE_INCREMENT_MS: Number(Deno.env.get("THROTTLE_INCREMENT_MS") || 500),
  
  // Retry settings
  MAX_RETRIES: Number(Deno.env.get("MAX_RETRIES") || 3),
  DB_WRITE_THROTTLE_MS: Number(Deno.env.get("DB_WRITE_THROTTLE_MS") || 50),
  
  // TTLs
  BATCH_CLAIM_TTL_SECONDS: Number(Deno.env.get("BATCH_CLAIM_TTL_SECONDS") || 3600),
  
  // Worker settings
  WORKER_HEARTBEAT_INTERVAL_MS: Number(Deno.env.get("WORKER_HEARTBEAT_INTERVAL_MS") || 30000),
  
  // NEW SETTINGS - Added for paginated worker queue
  PAGE_SIZE_ALBUMS: Number(Deno.env.get("PAGE_SIZE_ALBUMS") || 20),
  PAGE_SIZE_TRACKS: Number(Deno.env.get("PAGE_SIZE_TRACKS") || 50),
  MAX_WORKERS_PER_TYPE: Number(Deno.env.get("MAX_WORKERS_PER_TYPE") || 5),
  CIRCUIT_BREAKER_THRESHOLD: Number(Deno.env.get("CIRCUIT_BREAKER_THRESHOLD") || 5),
  CIRCUIT_BREAKER_RESET_SECONDS: Number(Deno.env.get("CIRCUIT_BREAKER_RESET_SECONDS") || 300)
};

// Reset expired batches (for scheduled jobs)
export async function resetExpiredBatches(): Promise<number> {
  try {
    const { data, error } = await supabase.rpc('reset_expired_batches');
    
    if (error) {
      console.error('Error resetting expired batches:', error);
      return 0;
    }
    
    return data || 0;
  } catch (error) {
    console.error('Error calling reset_expired_batches:', error);
    return 0;
  }
}

// NEW FUNCTIONS - Added for paginated worker queue system

// Calculate backoff time with exponential delay and jitter
export function calculateBackoffMs(
  retryCount: number, 
  baseDelayMs: number = config.BASE_THROTTLE_MS, 
  maxDelayMs: number = config.MAX_THROTTLE_MS
): number {
  // Base exponential delay: baseDelay * 2^retryCount
  const expDelay = baseDelayMs * Math.pow(2, retryCount);
  
  // Add random jitter (±25%)
  const jitterRange = expDelay * 0.25;
  const jitter = Math.random() * jitterRange * 2 - jitterRange;
  
  // Apply jitter and cap at max delay
  return Math.min(expDelay + jitter, maxDelayMs);
}

// Create paginated batches for large jobs
export async function createPaginatedBatches(
  batchType: string,
  parentBatchId: string | null,
  totalItems: number,
  itemsPerPage: number,
  baseMetadata: Record<string, any> = {}
): Promise<string[]> {
  try {
    const totalPages = Math.ceil(totalItems / itemsPerPage);
    const batchIds: string[] = [];
    
    // Create a batch for each page
    for (let pageIndex = 0; pageIndex < totalPages; pageIndex++) {
      const offset = pageIndex * itemsPerPage;
      
      // Create metadata with pagination info
      const metadata = {
        ...baseMetadata,
        parent_batch_id: parentBatchId,
        page_index: pageIndex,
        total_pages: totalPages,
        offset: offset,
        limit: itemsPerPage,
        pagination: true,
        created_at: new Date().toISOString()
      };
      
      // Create the batch
      const { data: newBatch, error } = await supabase
        .from('processing_batches')
        .insert({
          batch_type: batchType,
          status: 'pending',
          metadata
        })
        .select('id')
        .single();
      
      if (error || !newBatch) {
        console.error(`Error creating paginated batch for page ${pageIndex}:`, error);
        continue;
      }
      
      batchIds.push(newBatch.id);
      
      // Track metrics
      await incrementMetric('paginated_batches_created', 1, {
        batch_type: batchType,
        page_index: pageIndex,
        total_pages: totalPages,
        items_per_page: itemsPerPage
      });
    }
    
    return batchIds;
  } catch (error) {
    console.error('Error creating paginated batches:', error);
    return [];
  }
}

// Check if all child batches of a parent are complete
export async function checkBatchCompletion(
  parentBatchId: string
): Promise<{ complete: boolean; total: number; completed: number; failed: number }> {
  try {
    // Find all child batches
    const { data: childBatches, error } = await supabase
      .from('processing_batches')
      .select('id, status, items_total, items_processed, items_failed')
      .filter('metadata->parent_batch_id', 'eq', parentBatchId);
    
    if (error || !childBatches || childBatches.length === 0) {
      return { complete: false, total: 0, completed: 0, failed: 0 };
    }
    
    const total = childBatches.length;
    const completed = childBatches.filter(b => b.status === 'completed').length;
    const failed = childBatches.filter(b => b.status === 'error').length;
    
    // Complete when all batches are either completed or error
    const complete = (completed + failed) === total;
    
    // Log completion status
    if (complete) {
      await logSystemEvent('info', 'batch_completion', 
        `Batch ${parentBatchId} - All child batches complete: ${completed} successful, ${failed} failed`,
        { parentBatchId, total, completed, failed }
      );
      
      // Update the parent batch with completion stats
      await supabase
        .from('processing_batches')
        .update({
          items_total: total,
          items_processed: completed,
          items_failed: failed,
          status: failed === 0 ? 'completed' : (completed > 0 ? 'partial' : 'error'),
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        })
        .eq('id', parentBatchId);
    }
    
    return { complete, total, completed, failed };
  } catch (error) {
    console.error('Error checking batch completion:', error);
    return { complete: false, total: 0, completed: 0, failed: 0 };
  }
}

// Track circuit breaker state to avoid overwhelming APIs
export async function checkCircuitBreaker(
  serviceName: string
): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from('api_rate_limits')
      .select('*')
      .eq('api_name', serviceName)
      .order('updated_at', { ascending: false })
      .limit(config.CIRCUIT_BREAKER_THRESHOLD);
    
    if (error || !data || data.length < config.CIRCUIT_BREAKER_THRESHOLD) {
      return false; // Not enough data to decide, assume circuit is closed
    }
    
    // Check if we've had consecutive rate limit issues or errors
    const recentIssues = data.filter(item => 
      (item.requests_remaining !== null && item.requests_remaining <= 1) || 
      (item.last_response && 
       (item.last_response.status >= 429 || 
        (item.last_response.status >= 500 && item.last_response.status < 600)))
    );
    
    // If enough recent issues, open the circuit breaker
    const shouldOpen = recentIssues.length >= config.CIRCUIT_BREAKER_THRESHOLD / 2;
    
    if (shouldOpen) {
      // Log circuit breaker activation
      await logSystemEvent('warning', 'circuit_breaker', 
        `Circuit breaker opened for ${serviceName} due to ${recentIssues.length} recent issues`,
        { serviceName, issueCount: recentIssues.length, samples: data.length }
      );
      
      // Track metrics
      await incrementMetric('circuit_breaker_activations', 1, { service: serviceName });
    }
    
    return shouldOpen;
  } catch (error) {
    console.error('Error checking circuit breaker:', error);
    return false; // On error, assume circuit is closed
  }
}

// Generate a correlation ID for tracing requests through the system
export function generateCorrelationId(): string {
  return crypto.randomUUID();
}

// Add monitoring for batch hierarchy to see overall progress
export async function getBatchHierarchy(
  batchId: string
): Promise<any> {
  try {
    // Get the root batch
    const { data: rootBatch, error: rootError } = await supabase
      .from('processing_batches')
      .select('*')
      .eq('id', batchId)
      .single();
    
    if (rootError || !rootBatch) {
      return null;
    }
    
    // Find all child batches
    let children = [];
    if (rootBatch.batch_type !== 'process_album_page' && 
        rootBatch.batch_type !== 'process_track_page') {
      const { data: childBatches, error: childrenError } = await supabase
        .from('processing_batches')
        .select('*')
        .filter('metadata->parent_batch_id', 'eq', batchId);
      
      if (!childrenError && childBatches) {
        children = childBatches;
      }
    }
    
    // Return the full hierarchy
    return {
      ...rootBatch,
      children
    };
  } catch (error) {
    console.error('Error getting batch hierarchy:', error);
    return null;
  }
}

// Check if a system is experiencing backpressure (too many pending items)
export async function checkBackpressure(
  targetTable: string,
  threshold: number = 1000
): Promise<boolean> {
  try {
    // Check how many pending items exist for a specific table
    const { count, error } = await supabase
      .from('processing_items')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'pending')
      .eq('item_type', targetTable);
    
    if (error) {
      return false; // On error, assume no backpressure
    }
    
    const underPressure = (count || 0) > threshold;
    
    if (underPressure) {
      // Log backpressure detection
      await logSystemEvent('warning', 'backpressure', 
        `Backpressure detected for ${targetTable} with ${count} pending items`,
        { targetTable, pendingCount: count, threshold }
      );
      
      // Track metrics
      await incrementMetric('backpressure_events', 1, { 
        table: targetTable, 
        pending_count: count 
      });
    }
    
    return underPressure;
  } catch (error) {
    console.error('Error checking backpressure:', error);
    return false; // On error, assume no backpressure
  }
}

// Track and report retry and error rates to detect problems
export async function trackErrorRates(
  batchType: string,
  timeWindowMinutes: number = 15
): Promise<{ errorRate: number; retryRate: number; alertThresholdExceeded: boolean }> {
  try {
    // Get recent items for this batch type
    const startTime = new Date(Date.now() - timeWindowMinutes * 60 * 1000).toISOString();
    
    const { data: items, error } = await supabase
      .from('processing_items')
      .select('id, status, retry_count')
      .eq('batch_type', batchType)
      .gte('updated_at', startTime);
    
    if (error || !items || items.length === 0) {
      return { errorRate: 0, retryRate: 0, alertThresholdExceeded: false };
    }
    
    // Calculate rates
    const total = items.length;
    const errors = items.filter(item => item.status === 'error').length;
    const retries = items.filter(item => (item.retry_count || 0) > 0).length;
    
    const errorRate = errors / total;
    const retryRate = retries / total;
    
    // Alert threshold (configurable)
    const alertThreshold = 0.25; // 25% error rate is concerning
    const alertThresholdExceeded = errorRate > alertThreshold;
    
    if (alertThresholdExceeded) {
      // Log alert
      await logSystemEvent('warning', 'error_rate_alert', 
        `High error rate detected for ${batchType}: ${(errorRate * 100).toFixed(1)}%`,
        { batchType, errorRate, retryRate, timeWindowMinutes, total, errors, retries }
      );
      
      // Track metrics
      await incrementMetric('error_rate_alerts', 1, { 
        batch_type: batchType, 
        error_rate: errorRate,
        retry_rate: retryRate
      });
    }
    
    return { errorRate, retryRate, alertThresholdExceeded };
  } catch (error) {
    console.error('Error tracking error rates:', error);
    return { errorRate: 0, retryRate: 0, alertThresholdExceeded: false };
  }
}
