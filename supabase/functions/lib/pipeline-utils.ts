
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
