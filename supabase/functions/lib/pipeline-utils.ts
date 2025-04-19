
import { supabase } from "./api-clients.ts";
import { MetricDimensions } from "./types.ts";

// Log a system event to the database
export async function logSystemEvent(
  level: 'info' | 'warning' | 'error' | 'debug',
  component: string,
  message: string,
  context: any = {},
  trace_id?: string
): Promise<void> {
  try {
    await supabase
      .from('system_logs')
      .insert({
        log_level: level,
        component,
        message,
        context,
        trace_id
      });
  } catch (error) {
    console.error(`Error logging system event: ${error.message}`, {
      level, component, message, context
    });
  }
}

// Update API rate limits based on response headers
export async function updateRateLimit(
  apiName: string,
  endpoint: string,
  headers: Headers,
  metadata: any = {}
): Promise<void> {
  try {
    // Extract rate limit headers - different APIs use different header formats
    const requestsRemaining = parseInt(
      headers.get('x-ratelimit-remaining') || 
      headers.get('X-RateLimit-Remaining') || 
      headers.get('ratelimit-remaining') || 
      '-1'
    );

    const requestsLimit = parseInt(
      headers.get('x-ratelimit-limit') || 
      headers.get('X-RateLimit-Limit') || 
      headers.get('ratelimit-limit') || 
      '-1'
    );

    // Parse reset time (could be in seconds or as a timestamp)
    let resetAt: Date | null = null;
    const resetValue = headers.get('x-ratelimit-reset') || 
                     headers.get('X-RateLimit-Reset') ||
                     headers.get('ratelimit-reset');
    
    if (resetValue) {
      // Check if it's a timestamp or seconds from now
      const resetNum = parseInt(resetValue);
      if (resetNum > 0) {
        // If it's a Unix timestamp (usually > 1600000000)
        if (resetNum > 1600000000) {
          resetAt = new Date(resetNum * 1000); // Convert from Unix timestamp
        } else {
          // It's seconds from now
          resetAt = new Date(Date.now() + (resetNum * 1000));
        }
      }
    }

    // Log the rate limit if found
    if (requestsLimit > 0 || requestsRemaining >= 0 || resetAt) {
      await supabase
        .from('api_rate_limits')
        .upsert({
          api_name: apiName,
          endpoint: endpoint,
          requests_limit: requestsLimit > 0 ? requestsLimit : undefined,
          requests_remaining: requestsRemaining >= 0 ? requestsRemaining : undefined,
          reset_at: resetAt ? resetAt.toISOString() : undefined,
          last_response: metadata
        }, {
          onConflict: 'api_name,endpoint'
        });
    }
  } catch (error) {
    console.error(`Error updating rate limits: ${error.message}`, {
      apiName, endpoint
    });
  }
}

// Check if an API endpoint is rate limited
export async function isApiRateLimited(
  apiName: string,
  endpoint: string
): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from('api_rate_limits')
      .select('*')
      .eq('api_name', apiName)
      .eq('endpoint', endpoint)
      .single();
    
    if (error || !data) {
      return false;
    }
    
    // If we're out of requests and reset time is in the future
    if (
      data.requests_remaining !== null && 
      data.requests_remaining <= 0 && 
      data.reset_at && 
      new Date(data.reset_at) > new Date()
    ) {
      return true;
    }
    
    return false;
  } catch (error) {
    console.error(`Error checking rate limits: ${error.message}`);
    return false;
  }
}

// Calculate backoff time with jitter
export function calculateBackoffMs(retryCount: number, baseDelay = 1000): number {
  // Exponential backoff: 1s, 2s, 4s, 8s, 16s...
  const exponentialDelay = baseDelay * Math.pow(2, retryCount);
  
  // Add random jitter (±25%)
  const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
  
  return Math.min(exponentialDelay + jitter, 30000); // Cap at 30 seconds
}

// Sleep for a specified period
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Format Spotify release dates
export function formatSpotifyReleaseDate(releaseDate: string | null): string | null {
  if (!releaseDate) return null;
  
  const dateParts = releaseDate.split('-');
  
  if (dateParts.length === 1 && dateParts[0].length === 4) {
    // Year only format (e.g., "2012") - append "-01-01" to make it January 1st
    return `${releaseDate}-01-01`;
  } else if (dateParts.length === 2) {
    // Year-month format (e.g., "2012-03") - append "-01" to make it the 1st day of the month
    return `${releaseDate}-01`;
  } else if (dateParts.length === 3) {
    // Already full date format, return as is
    return releaseDate;
  } else {
    // Unknown format, log it and return null
    console.error(`Unknown release date format: ${releaseDate}`);
    return null;
  }
}

// Increment a metric for observability
export async function incrementMetric(
  metricName: string,
  value: number = 1,
  dimensions: MetricDimensions = {}
): Promise<void> {
  try {
    await logSystemEvent('info', 'metrics', 
      `${metricName}:${value}`,
      {
        metric_name: metricName,
        value,
        dimensions,
        timestamp: new Date().toISOString()
      }
    );
  } catch (error) {
    console.error(`Error incrementing metric ${metricName}: ${error.message}`);
  }
}

// Update data quality score
export async function updateDataQualityScore(
  entityType: string,
  entityId: string,
  source: string,
  qualityScore: number,
  completenessScore?: number,
  accuracyScore?: number,
  metadata: any = {}
): Promise<void> {
  try {
    // Check if a score already exists
    const { data: existingScore, error: queryError } = await supabase
      .from('data_quality_scores')
      .select('id')
      .eq('entity_type', entityType)
      .eq('entity_id', entityId)
      .eq('source', source)
      .maybeSingle();
    
    if (queryError) {
      console.error(`Error checking for existing quality score: ${queryError.message}`);
      return;
    }
    
    // Upsert the score
    await supabase
      .from('data_quality_scores')
      .upsert({
        entity_type: entityType,
        entity_id: entityId,
        source: source,
        quality_score: qualityScore,
        completeness_score: completenessScore,
        accuracy_score: accuracyScore,
        last_verified_at: new Date().toISOString(),
        metadata: {
          ...metadata,
          updated_at: new Date().toISOString()
        }
      }, {
        onConflict: 'entity_type,entity_id,source'
      });
  } catch (error) {
    console.error(`Error updating data quality score: ${error.message}`);
  }
}

// Register a worker heartbeat
export async function registerWorkerHeartbeat(
  workerId: string,
  workerType: string,
  status: string = 'active',
  currentBatchId?: string,
  metadata: any = {}
): Promise<void> {
  try {
    await supabase
      .from('worker_heartbeats')
      .upsert({
        worker_id: workerId,
        worker_type: workerType,
        status,
        current_batch_id: currentBatchId,
        last_heartbeat: new Date().toISOString(),
        metadata: {
          ...metadata,
          updated_at: new Date().toISOString()
        }
      }, {
        onConflict: 'worker_id'
      });
  } catch (error) {
    console.error(`Error registering worker heartbeat: ${error.message}`);
  }
}

// Generate a correlation ID for tracing
export function generateCorrelationId(): string {
  return crypto.randomUUID();
}

// Check if a batch is completed
export async function checkBatchCompletion(batchId: string): Promise<{
  complete: boolean;
  total: number;
  completed: number;
  failed: number;
}> {
  try {
    // Get batch status
    const { data: batch, error: batchError } = await supabase
      .from('processing_batches')
      .select('*')
      .eq('id', batchId)
      .single();
    
    if (batchError || !batch) {
      console.error(`Error getting batch ${batchId}:`, batchError);
      return { complete: false, total: 0, completed: 0, failed: 0 };
    }
    
    // If already completed or errored, no need to check further
    if (batch.status === 'completed' || batch.status === 'error' || batch.status === 'completed_with_next') {
      return { 
        complete: true, 
        total: batch.items_total || 0, 
        completed: batch.items_processed || 0,
        failed: batch.items_failed || 0
      };
    }
    
    // Get counts for this batch
    const { count: totalItems, error: countError } = await supabase
      .from('processing_items')
      .select('*', { count: 'exact', head: true })
      .eq('batch_id', batchId);
    
    if (countError) {
      console.error(`Error counting items for batch ${batchId}:`, countError);
      return { complete: false, total: 0, completed: 0, failed: 0 };
    }
    
    // Get completed/failed counts
    const { data: completedItems, error: completedError } = await supabase
      .from('processing_items')
      .select('status')
      .eq('batch_id', batchId)
      .in('status', ['completed', 'error']);
    
    if (completedError) {
      console.error(`Error counting completed items for batch ${batchId}:`, completedError);
      return { complete: false, total: 0, completed: 0, failed: 0 };
    }
    
    // Count by status
    const total = totalItems || 0;
    const completed = completedItems?.filter(item => item.status === 'completed').length || 0;
    const failed = completedItems?.filter(item => item.status === 'error').length || 0;
    
    // Check if all items are processed
    const complete = total > 0 && (completed + failed) === total;
    
    return { complete, total, completed, failed };
  } catch (error) {
    console.error(`Error checking batch completion for ${batchId}:`, error);
    return { complete: false, total: 0, completed: 0, failed: 0 };
  }
}

// Validate the integrity of a batch
export async function validateBatchIntegrity(batchId: string): Promise<boolean> {
  try {
    // Get batch info
    const { data: batch, error: batchError } = await supabase
      .from('processing_batches')
      .select('*')
      .eq('id', batchId)
      .single();
    
    if (batchError || !batch) {
      console.error(`Error getting batch ${batchId}:`, batchError);
      return false;
    }
    
    // Count items
    const { count: actualItemCount, error: countError } = await supabase
      .from('processing_items')
      .select('*', { count: 'exact', head: true })
      .eq('batch_id', batchId);
    
    if (countError) {
      console.error(`Error counting items for batch ${batchId}:`, countError);
      return false;
    }
    
    // Count completed items
    const { count: completedCount, error: completedError } = await supabase
      .from('processing_items')
      .select('*', { count: 'exact', head: true })
      .eq('batch_id', batchId)
      .eq('status', 'completed');
    
    if (completedError) {
      console.error(`Error counting completed items for batch ${batchId}:`, completedError);
      return false;
    }
    
    // Count failed items
    const { count: failedCount, error: failedError } = await supabase
      .from('processing_items')
      .select('*', { count: 'exact', head: true })
      .eq('batch_id', batchId)
      .eq('status', 'error');
    
    if (failedError) {
      console.error(`Error counting failed items for batch ${batchId}:`, failedError);
      return false;
    }
    
    // Verify the counts match what's recorded in the batch
    const itemsTotal = batch.items_total || 0;
    const itemsProcessed = batch.items_processed || 0;
    const itemsFailed = batch.items_failed || 0;
    
    if (itemsTotal !== actualItemCount) {
      await logSystemEvent('warning', 'batch_integrity', 
        `Batch ${batchId} has inconsistent items_total: recorded=${itemsTotal}, actual=${actualItemCount}`,
        { batchId, batchType: batch.batch_type }
      );
      return false;
    }
    
    if (itemsProcessed !== completedCount) {
      await logSystemEvent('warning', 'batch_integrity', 
        `Batch ${batchId} has inconsistent items_processed: recorded=${itemsProcessed}, actual=${completedCount}`,
        { batchId, batchType: batch.batch_type }
      );
      return false;
    }
    
    if (itemsFailed !== failedCount) {
      await logSystemEvent('warning', 'batch_integrity', 
        `Batch ${batchId} has inconsistent items_failed: recorded=${itemsFailed}, actual=${failedCount}`,
        { batchId, batchType: batch.batch_type }
      );
      return false;
    }
    
    return true;
  } catch (error) {
    console.error(`Error validating batch integrity for ${batchId}:`, error);
    return false;
  }
}

// Send an item to the dead letter queue
export async function sendToDeadLetterQueue(
  originalItemId: string,
  originalBatchId: string,
  itemType: string,
  itemId: string,
  errorMessage: string,
  metadata: any = {},
  retryCount: number = 0
): Promise<string | null> {
  try {
    const { data, error } = await supabase
      .from('dead_letter_items')
      .insert({
        original_item_id: originalItemId,
        original_batch_id: originalBatchId,
        item_type: itemType,
        item_id: itemId,
        error_message: errorMessage,
        retry_count: retryCount,
        metadata: {
          ...metadata,
          sent_at: new Date().toISOString()
        }
      })
      .select('id')
      .single();
    
    if (error) {
      console.error(`Error sending to dead letter queue: ${error.message}`);
      return null;
    }
    
    return data.id;
  } catch (error) {
    console.error(`Error sending to dead letter queue: ${error.message}`);
    return null;
  }
}

// Update item status in a processing batch
export async function updateItemStatus(
  itemId: string, 
  status: 'pending' | 'processing' | 'completed' | 'error',
  errorMessage?: string,
  metadata?: any
): Promise<boolean> {
  try {
    // Get the current item to update its metadata correctly
    const { data: currentItem, error: getError } = await supabase
      .from('processing_items')
      .select('metadata, retry_count')
      .eq('id', itemId)
      .single();
    
    if (getError) {
      console.error(`Error getting item ${itemId} for status update:`, getError);
      return false;
    }
    
    // Prepare the update
    const updateData: any = {
      status,
      updated_at: new Date().toISOString()
    };
    
    // Increment retry count for errors
    if (status === 'error') {
      updateData.retry_count = (currentItem.retry_count || 0) + 1;
      updateData.last_error = errorMessage;
    }
    
    // Update metadata if provided
    if (metadata) {
      updateData.metadata = {
        ...(currentItem.metadata || {}),
        ...metadata,
        last_updated: new Date().toISOString()
      };
    }
    
    // Update the item
    const { error: updateError } = await supabase
      .from('processing_items')
      .update(updateData)
      .eq('id', itemId);
    
    if (updateError) {
      console.error(`Error updating item ${itemId} status:`, updateError);
      return false;
    }
    
    return true;
  } catch (error) {
    console.error(`Error updating item status for ${itemId}:`, error);
    return false;
  }
}

// Log processing error
export async function logProcessingError(
  errorType: string,
  component: string,
  message: string,
  error: any,
  context: any = {}
): Promise<void> {
  try {
    // Extract error details
    const errorMessage = error instanceof Error ? error.message : String(error);
    const stackTrace = error instanceof Error ? error.stack : undefined;
    
    // Log to system logs
    await logSystemEvent('error', component, message, {
      error_type: errorType,
      error_message: errorMessage,
      ...context
    });
    
    // Log to error_logs table
    await supabase
      .from('error_logs')
      .insert({
        error_type: errorType,
        source: component,
        message: message,
        stack_trace: stackTrace,
        context: {
          error_message: errorMessage,
          ...context
        },
        item_type: context.item_type,
        item_id: context.item_id
      });
  } catch (logError) {
    console.error(`Error logging processing error: ${logError.message}`, {
      original_error: error,
      message
    });
  }
}

// Check if a circuit breaker is open for an API
export async function checkCircuitBreaker(apiName: string): Promise<boolean> {
  try {
    // Check if we have a recent circuit breaker event
    const { data: circuitEvents, error: eventError } = await supabase
      .from('system_logs')
      .select('*')
      .eq('component', 'circuit_breaker')
      .eq('context->api_name', apiName)
      .order('created_at', { ascending: false })
      .limit(1);
    
    if (eventError || !circuitEvents || circuitEvents.length === 0) {
      return false;
    }
    
    // Check if the circuit breaker is still active
    const event = circuitEvents[0];
    if (event.context && event.context.reset_after) {
      const resetTime = new Date(event.context.reset_after);
      const isOpen = resetTime > new Date();
      
      return isOpen;
    }
    
    return false;
  } catch (error) {
    console.error(`Error checking circuit breaker for ${apiName}:`, error);
    return false;
  }
}

// Check if backpressure is needed
export async function checkBackpressure(
  component: string, 
  maxConcurrent: number = 10
): Promise<boolean> {
  try {
    // Count active workers for this component
    const { count, error } = await supabase
      .from('worker_heartbeats')
      .select('*', { count: 'exact', head: true })
      .eq('worker_type', component)
      .eq('status', 'active')
      .gt('last_heartbeat', new Date(Date.now() - 5 * 60 * 1000).toISOString());
    
    if (error) {
      console.error(`Error checking backpressure for ${component}:`, error);
      return false;
    }
    
    return (count || 0) >= maxConcurrent;
  } catch (error) {
    console.error(`Error checking backpressure for ${component}:`, error);
    return false;
  }
}

// Track error rates for monitoring
export async function trackErrorRates(
  batchType: string, 
  window: number = 24 * 60 * 60 * 1000, // Default 24h window
  alertThreshold: number = 0.25 // 25% error rate triggers alert
): Promise<{
  errorRate: number;
  retryRate: number;
  alertThresholdExceeded: boolean;
}> {
  try {
    const startTime = new Date(Date.now() - window).toISOString();
    
    // Count total items processed
    const { count: totalCount, error: totalError } = await supabase
      .from('processing_items')
      .select('*', { count: 'exact', head: true })
      .eq('metadata->batch_type', batchType)
      .gt('updated_at', startTime);
    
    if (totalError) {
      console.error(`Error counting total items for ${batchType}:`, totalError);
      return { errorRate: 0, retryRate: 0, alertThresholdExceeded: false };
    }
    
    // Count error items
    const { count: errorCount, error: errorCountError } = await supabase
      .from('processing_items')
      .select('*', { count: 'exact', head: true })
      .eq('metadata->batch_type', batchType)
      .eq('status', 'error')
      .gt('updated_at', startTime);
    
    if (errorCountError) {
      console.error(`Error counting failed items for ${batchType}:`, errorCountError);
      return { errorRate: 0, retryRate: 0, alertThresholdExceeded: false };
    }
    
    // Count retry attempts 
    const { data: retryItems, error: retryError } = await supabase
      .from('processing_items')
      .select('retry_count')
      .eq('metadata->batch_type', batchType)
      .gt('retry_count', 0)
      .gt('updated_at', startTime);
    
    if (retryError) {
      console.error(`Error counting retry items for ${batchType}:`, retryError);
      return { errorRate: 0, retryRate: 0, alertThresholdExceeded: false };
    }
    
    // Calculate rates
    const total = totalCount || 1; // Avoid division by zero
    const errors = errorCount || 0;
    const retries = retryItems?.length || 0;
    
    const errorRate = errors / total;
    const retryRate = retries / total;
    const alertThresholdExceeded = errorRate >= alertThreshold;
    
    // Log alert if threshold exceeded
    if (alertThresholdExceeded) {
      await logSystemEvent('warning', 'error_rate_alert',
        `High error rate detected for ${batchType}: ${(errorRate * 100).toFixed(1)}%`,
        {
          batch_type: batchType,
          error_rate: errorRate,
          retry_rate: retryRate,
          alert_threshold: alertThreshold,
          time_window_hours: window / (60 * 60 * 1000)
        }
      );
    }
    
    return { errorRate, retryRate, alertThresholdExceeded };
  } catch (error) {
    console.error(`Error tracking error rates for ${batchType}:`, error);
    return { errorRate: 0, retryRate: 0, alertThresholdExceeded: false };
  }
}
