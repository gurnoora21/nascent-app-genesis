
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";
import { Database } from "../../../src/integrations/supabase/types.ts";

const SUPABASE_URL = "https://nsxxzhhbcwzatvlulfyp.supabase.co";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";

// Initialize Supabase client with service role for admin access to DB
const supabase = createClient<Database>(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

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
      const resetSeconds = parseInt(headers.get('x-ratelimit-reset') || '0');
      resetAt = new Date(Date.now() + resetSeconds * 1000).toISOString();
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

// Retry and Backoff Utils
export function calculateBackoff(
  retryCount: number,
  baseDelay = 1000,
  maxDelay = 30000
): number {
  const delay = Math.min(
    baseDelay * Math.pow(2, retryCount) * (0.5 + Math.random() * 0.5),
    maxDelay
  );
  return delay;
}

export function isTransientError(error: Error): boolean {
  const transientErrorPatterns = [
    /network/i,
    /timeout/i,
    /rate limit/i,
    /(429|503|504)/,
    /temporarily unavailable/i,
    /too many requests/i
  ];
  
  const errorString = error.message + (error.stack || '');
  return transientErrorPatterns.some(pattern => pattern.test(errorString));
}
