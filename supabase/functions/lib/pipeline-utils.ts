
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
  WORKER_HEARTBEAT_INTERVAL_MS: Number(Deno.env.get("WORKER_HEARTBEAT_INTERVAL_MS") || 30000)
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
