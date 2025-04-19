
// Pipeline utilities for the Spotify data pipeline

import { supabase } from "./api-clients.ts";
import type { DataQualityScore, SystemLogEntry, BatchStatus, MetricDimensions } from "./types.ts";

// Format Spotify release date to work with PostgreSQL date type
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

// Generate a unique correlation ID for tracing
export function generateCorrelationId(): string {
  return crypto.randomUUID();
}

// Log system events with consistent formatting
export async function logSystemEvent(
  level: 'info' | 'warning' | 'error' | 'debug',
  component: string,
  message: string,
  context?: any,
  traceId?: string
): Promise<void> {
  try {
    const logEntry: SystemLogEntry = {
      level,
      component,
      message,
      context,
      trace_id: traceId
    };
    
    // For local testing and debugging
    console.log(`${level.toUpperCase()} [${component}]: ${message}`);
    if (context) {
      console.log(`Context: ${JSON.stringify(context)}`);
    }
    
    // Log to system_logs table
    await supabase
      .from('system_logs')
      .insert(logEntry);
  } catch (error) {
    // If logging fails, at least output to console
    console.error(`Failed to log event: ${error.message}`);
    console.error(`Original message: ${message}`);
  }
}

// Check if an API is currently rate limited
export async function isApiRateLimited(
  apiName: string,
  endpoint: string
): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from("api_rate_limits")
      .select("*")
      .eq("api_name", apiName)
      .eq("endpoint", endpoint)
      .single();
    
    if (error || !data) return false;
    
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
  } catch (err) {
    console.error("Error checking rate limits:", err);
    return false;
  }
}

// Update API rate limits based on response headers
export async function updateRateLimit(
  apiName: string,
  endpoint: string,
  headers: Headers,
  context: { status: number }
): Promise<void> {
  try {
    // Extract rate limit info from headers
    const remaining = headers.get('x-ratelimit-remaining') || headers.get('ratelimit-remaining');
    const limit = headers.get('x-ratelimit-limit') || headers.get('ratelimit-limit');
    const reset = headers.get('x-ratelimit-reset') || headers.get('ratelimit-reset');
    
    if (!remaining && !limit && !reset) {
      // Some APIs don't include rate limit headers in every response
      return;
    }
    
    let resetDate: Date | null = null;
    
    if (reset) {
      // Reset could be a timestamp or seconds from now
      if (reset.length > 10) {
        // Timestamp in milliseconds
        resetDate = new Date(parseInt(reset));
      } else {
        // Seconds from now
        resetDate = new Date(Date.now() + parseInt(reset) * 1000);
      }
    }
    
    // Upsert into api_rate_limits table
    await supabase
      .from('api_rate_limits')
      .upsert({
        api_name: apiName,
        endpoint: endpoint,
        requests_remaining: remaining ? parseInt(remaining) : null,
        requests_limit: limit ? parseInt(limit) : null,
        reset_at: resetDate ? resetDate.toISOString() : null,
        last_response: {
          status: context.status,
          timestamp: new Date().toISOString()
        },
        updated_at: new Date().toISOString()
      }, {
        onConflict: 'api_name,endpoint'
      });
  } catch (error) {
    console.error('Error updating rate limits:', error);
  }
}

// Update data quality score for an entity
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
    const dataQualityScore: DataQualityScore = {
      entity_type: entityType,
      entity_id: entityId,
      source: source,
      quality_score: qualityScore,
      completeness_score: completenessScore,
      accuracy_score: accuracyScore,
      metadata: metadata || {}
    };
    
    await supabase
      .from('data_quality_scores')
      .upsert(dataQualityScore, {
        onConflict: 'entity_type,entity_id,source'
      });
  } catch (error) {
    console.error('Error updating data quality score:', error);
  }
}

// Update the status of a processing item
export async function updateItemStatus(
  itemId: string,
  status: 'pending' | 'processing' | 'completed' | 'error',
  errorMessage?: string,
  metadata?: any
): Promise<void> {
  try {
    const updateData: any = {
      status,
      updated_at: new Date().toISOString()
    };
    
    if (errorMessage) {
      updateData.last_error = errorMessage;
    }
    
    if (metadata) {
      updateData.metadata = metadata;
    }
    
    await supabase
      .from('processing_items')
      .update(updateData)
      .eq('id', itemId);
  } catch (error) {
    console.error('Error updating item status:', error);
  }
}

// Calculate backoff time based on retry count with jitter
export function calculateBackoffMs(retryCount: number, baseDelay = 1000): number {
  // Exponential backoff: 1s, 2s, 4s, 8s, 16s...
  const exponentialDelay = baseDelay * Math.pow(2, retryCount);
  
  // Add random jitter (±25%)
  const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
  
  return Math.min(exponentialDelay + jitter, 30000); // Cap at 30 seconds
}

// Sleep for a specified number of milliseconds
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Increment a metric with dimensions
export async function incrementMetric(
  metricName: string,
  value: number = 1,
  dimensions: MetricDimensions = {}
): Promise<void> {
  try {
    await supabase.rpc('increment_metric', {
      p_metric_name: metricName,
      p_value: value,
      p_dimensions: dimensions
    });
  } catch (error) {
    console.error(`Error incrementing metric ${metricName}:`, error);
  }
}

// Check if a batch is complete
export async function checkBatchCompletion(batchId: string): Promise<{
  complete: boolean;
  total: number;
  completed: number;
  failed: number;
}> {
  try {
    // Get item counts for this batch
    const { data, error } = await supabase
      .from('processing_items')
      .select('status', { count: 'exact' })
      .eq('batch_id', batchId);
    
    if (error) {
      console.error(`Error checking batch completion: ${error.message}`);
      return { complete: false, total: 0, completed: 0, failed: 0 };
    }
    
    const total = data.length;
    const completed = data.filter(item => item.status === 'completed').length;
    const failed = data.filter(item => item.status === 'error').length;
    const pending = data.filter(item => item.status === 'pending' || item.status === 'processing').length;
    
    return {
      complete: pending === 0 && total > 0,
      total,
      completed,
      failed
    };
  } catch (error) {
    console.error(`Error in checkBatchCompletion: ${error.message}`);
    return { complete: false, total: 0, completed: 0, failed: 0 };
  }
}

// Validate batch integrity before processing
export async function validateBatchIntegrity(batchId: string): Promise<{valid: boolean, reason?: string}> {
  try {
    // Check if batch exists
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", batchId)
      .single();
    
    if (batchError || !batch) {
      return { valid: false, reason: `Batch ${batchId} not found` };
    }
    
    // Check if batch is in a valid state
    if (batch.status !== 'pending' && batch.status !== 'processing') {
      return { valid: false, reason: `Batch ${batchId} is in ${batch.status} state, not processable` };
    }
    
    // Check if there are pending items
    const { count, error: countError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "pending");
    
    if (countError) {
      return { valid: false, reason: `Error checking pending items: ${countError.message}` };
    }
    
    if (count === 0) {
      return { valid: false, reason: `No pending items in batch ${batchId}` };
    }
    
    return { valid: true };
  } catch (err) {
    console.error("Error validating batch integrity:", err);
    return { valid: false, reason: `Exception during validation: ${err.message}` };
  }
}

// Check if a circuit breaker is open for an API
export async function checkCircuitBreaker(apiName: string): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from('api_rate_limits')
      .select('*')
      .eq('api_name', apiName)
      .is('circuit_breaker_until', 'not.null')
      .order('updated_at', { ascending: false })
      .limit(1)
      .single();
    
    if (error || !data) {
      return false;
    }
    
    if (data.circuit_breaker_until && new Date(data.circuit_breaker_until) > new Date()) {
      return true; // Circuit breaker is active
    }
    
    return false;
  } catch (error) {
    console.error(`Error checking circuit breaker: ${error.message}`);
    return false;
  }
}

// Check for backpressure in the processing queues
export async function checkBackpressure(): Promise<{
  backpressure: boolean;
  pendingBatches: number;
  pendingItems: number;
}> {
  try {
    // Count pending batches
    const { count: pendingBatchCount, error: batchError } = await supabase
      .from('processing_batches')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'pending');
    
    // Count pending items
    const { count: pendingItemCount, error: itemError } = await supabase
      .from('processing_items')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'pending');
    
    const pendingBatches = pendingBatchCount || 0;
    const pendingItems = pendingItemCount || 0;
    
    // Define thresholds for backpressure
    const BATCH_THRESHOLD = 1000; // Too many pending batches
    const ITEM_THRESHOLD = 10000; // Too many pending items
    
    const backpressure = pendingBatches > BATCH_THRESHOLD || pendingItems > ITEM_THRESHOLD;
    
    return {
      backpressure,
      pendingBatches,
      pendingItems
    };
  } catch (error) {
    console.error(`Error checking backpressure: ${error.message}`);
    return {
      backpressure: false,
      pendingBatches: 0,
      pendingItems: 0
    };
  }
}

// Track error rates across different batch types
export async function trackErrorRates(batchType: string): Promise<{
  errorRate: number;
  retryRate: number;
  alertThresholdExceeded: boolean;
}> {
  try {
    // Look at batches completed in the last 24 hours
    const timeWindow = new Date();
    timeWindow.setHours(timeWindow.getHours() - 24);
    
    // Get completed batches
    const { data: batches, error: batchError } = await supabase
      .from('processing_batches')
      .select('items_total, items_processed, items_failed')
      .eq('batch_type', batchType)
      .in('status', ['completed', 'partial', 'error'])
      .gte('updated_at', timeWindow.toISOString());
    
    if (batchError || !batches || batches.length === 0) {
      return {
        errorRate: 0,
        retryRate: 0,
        alertThresholdExceeded: false
      };
    }
    
    // Calculate totals
    let totalItems = 0;
    let totalProcessed = 0;
    let totalFailed = 0;
    
    batches.forEach(batch => {
      totalItems += batch.items_total || 0;
      totalProcessed += batch.items_processed || 0;
      totalFailed += batch.items_failed || 0;
    });
    
    // Calculate error rate
    const errorRate = totalItems > 0 ? totalFailed / totalItems : 0;
    
    // Get retry information
    const { data: retryData, error: retryError } = await supabase
      .from('processing_items')
      .select('retry_count')
      .eq('status', 'error')
      .gt('retry_count', 0)
      .gte('updated_at', timeWindow.toISOString());
    
    const totalRetries = retryData ? retryData.length : 0;
    const retryRate = totalFailed > 0 ? totalRetries / totalFailed : 0;
    
    // Check if error rate exceeds threshold for alerting
    const alertThresholdExceeded = errorRate > 0.25 && totalItems > 10;
    
    return {
      errorRate,
      retryRate,
      alertThresholdExceeded
    };
  } catch (error) {
    console.error(`Error tracking error rates: ${error.message}`);
    return {
      errorRate: 0,
      retryRate: 0,
      alertThresholdExceeded: false
    };
  }
}
