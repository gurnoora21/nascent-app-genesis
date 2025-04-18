
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { supabase } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Calculate backoff time based on retry count with jitter
function calculateBackoff(retryCount: number, baseDelay = 1000): number {
  // Exponential backoff: 1s, 2s, 4s, 8s, 16s...
  const exponentialDelay = baseDelay * Math.pow(2, retryCount);
  
  // Add random jitter (±25%)
  const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
  
  return Math.min(exponentialDelay + jitter, 30000); // Cap at 30 seconds
}

// Check if API is rate limited
async function isApiRateLimited(apiName: string, endpoint: string): Promise<boolean> {
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

// Validate batch integrity before processing
async function validateBatchIntegrity(batchId: string): Promise<{valid: boolean, reason?: string}> {
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

// Process a pending producers batch
async function processProducersBatch(): Promise<{
  success: boolean;
  message: string;
  batchId?: string;
  processed?: number;
  failed?: number;
  status?: string;
}> {
  try {
    // Generate a unique worker ID
    const workerId = crypto.randomUUID();
    
    console.log(`Worker ${workerId} claiming a producers batch`);
    
    // Pre-flight check: Check if APIs are rate limited
    const geniusLimited = await isApiRateLimited("genius", "/songs");
    const discogsLimited = await isApiRateLimited("discogs", "/releases");
    
    if (geniusLimited && discogsLimited) {
      return {
        success: true,
        message: "Both Genius and Discogs APIs are currently rate limited, skipping batch claim",
        status: "delayed"
      };
    }
    
    // Claim a batch to process with soft locking mechanism
    const { data: batchId, error: claimError } = await supabase.rpc(
      "claim_processing_batch",
      {
        p_batch_type: "process_producers",
        p_worker_id: workerId,
        p_claim_ttl_seconds: 3600 // 1 hour claim time
      }
    );
    
    if (claimError) {
      console.error("Error claiming batch:", claimError);
      throw claimError;
    }
    
    if (!batchId) {
      return {
        success: true,
        message: "No pending producers batches found to process",
        status: "idle"
      };
    }
    
    console.log(`Worker ${workerId} claimed producers batch ${batchId}`);
    
    // Validate batch integrity
    const batchValidation = await validateBatchIntegrity(batchId);
    if (!batchValidation.valid) {
      console.warn(`Batch integrity check failed: ${batchValidation.reason}`);
      
      // Release the batch as it's not valid
      await supabase.rpc(
        "release_processing_batch",
        {
          p_batch_id: batchId,
          p_worker_id: workerId,
          p_status: "error"
        }
      );
      
      // Update batch with error message
      await supabase
        .from("processing_batches")
        .update({
          error_message: batchValidation.reason
        })
        .eq("id", batchId);
      
      return {
        success: false,
        message: `Batch integrity validation failed: ${batchValidation.reason}`,
        batchId,
        status: "error"
      };
    }
    
    // Define batch-specific parameters
    const batchSize = 10; // Default conservative batch size
    
    // Get items to process - these are tracks for which we need to identify producers
    const { data: items, error: itemsError } = await supabase
      .from("processing_items")
      .select("*")
      .eq("batch_id", batchId)
      .eq("status", "pending")
      .order("priority", { ascending: false })
      .order("created_at", { ascending: true })
      .limit(batchSize);
    
    if (itemsError) {
      console.error("Error getting batch items:", itemsError);
      throw itemsError;
    }
    
    if (!items || items.length === 0) {
      // No items to process, release the batch as completed
      await supabase.rpc(
        "release_processing_batch",
        {
          p_batch_id: batchId,
          p_worker_id: workerId,
          p_status: "completed"
        }
      );
      
      return {
        success: true,
        message: "Producers batch claimed but no items to process",
        batchId,
        processed: 0,
        failed: 0,
        status: "completed"
      };
    }
    
    console.log(`Processing ${items.length} tracks for producer identification in batch ${batchId}`);
    
    const processedItems: string[] = [];
    const failedItems: string[] = [];
    
    // Process producer identification
    await identifyProducers(items, processedItems, failedItems);
    
    console.log(`Producers batch ${batchId} processing complete. Processed: ${processedItems.length}, Failed: ${failedItems.length}`);
    
    // Get accurate counts from the database for this batch
    const { count: totalItems, error: countError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId);
    
    const { count: pendingItems, error: pendingError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "pending");
    
    const { count: completedItems, error: completedError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "completed");
    
    const { count: errorItems, error: errorCountError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "error");
    
    // Calculate if all items are processed
    const allDone = !pendingError && pendingItems === 0;
    
    // Release the batch with appropriate status
    await supabase.rpc(
      "release_processing_batch",
      {
        p_batch_id: batchId,
        p_worker_id: workerId,
        p_status: allDone ? "completed" : "processing"
      }
    );
    
    // Update batch with accurate progress
    if (!countError && !completedError && !errorCountError) {
      await supabase
        .from("processing_batches")
        .update({
          items_total: totalItems || 0,
          items_processed: completedItems || 0,
          items_failed: errorItems || 0,
        })
        .eq("id", batchId);
    }
    
    return {
      success: true,
      message: `Processed ${processedItems.length} tracks for producer identification, failed ${failedItems.length} tracks, ${pendingItems || 'unknown'} items remaining`,
      batchId,
      processed: processedItems.length,
      failed: failedItems.length,
      status: allDone ? "completed" : "in_progress"
    };
  } catch (error) {
    console.error("Error processing producers batch:", error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_producers_batch",
      p_message: `Error processing producers batch`,
      p_stack_trace: error.stack || "",
      p_context: { batchType: "process_producers" }
    });
    
    return {
      success: false,
      message: `Error processing producers batch: ${error.message}`,
      status: "error"
    };
  }
}

// Identify producers for tracks
async function identifyProducers(
  items: any[], 
  processedItems: string[], 
  failedItems: string[]
): Promise<void> {
  // Process each track - identify producers for each track
  for (const item of items) {
    try {
      if (item.item_type !== "track") {
        throw new Error(`Expected item_type "track", got "${item.item_type}"`);
      }
      
      const trackId = item.item_id;
      console.log(`Identifying producers for track ${trackId}`);
      
      // Call the identify-producers function
      const response = await fetch(
        `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/identify-producers`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
          },
          body: JSON.stringify({ trackId })
        }
      );
      
      if (!response.ok) {
        throw new Error(`HTTP error ${response.status}: ${await response.text()}`);
      }
      
      const result = await response.json();
      
      if (!result.success) {
        throw new Error(result.message || "Unknown error identifying producers");
      }
      
      // Mark item as processed
      await supabase
        .from("processing_items")
        .update({
          status: "completed",
          metadata: {
            ...item.metadata,
            producers_identified: result.producers?.length || 0,
            sources: result.sources || [],
            processed_at: new Date().toISOString()
          }
        })
        .eq("id", item.id);
      
      processedItems.push(item.id);
      
    } catch (error) {
      console.error(`Error identifying producers for track ${item.item_id}:`, error);
      
      // Determine if error is transient or permanent
      const isTransient = error.message.includes("rate limit") || 
                          error.message.includes("timeout") ||
                          error.message.includes("network");
      
      const shouldRetry = (item.retry_count || 0) < 5 && isTransient;
      
      await supabase
        .from("processing_items")
        .update({
          status: shouldRetry ? "pending" : "error",
          retry_count: (item.retry_count || 0) + 1,
          last_error: error.message,
          metadata: {
            ...item.metadata,
            last_error: error.message,
            retry_after: shouldRetry ? 
              new Date(Date.now() + calculateBackoff(item.retry_count || 0)).toISOString() : null
          }
        })
        .eq("id", item.id);
      
      // Only count as failed if we've given up retrying
      if (!shouldRetry) {
        failedItems.push(item.id);
      }
      
      // Log specific error
      await supabase.rpc("log_error", {
        p_error_type: "processing",
        p_source: "process_producers_batch",
        p_message: `Error identifying producers for track ${item.item_id}`,
        p_stack_trace: error.stack || "",
        p_context: { item },
        p_item_id: item.item_id,
        p_item_type: item.item_type
      });
    }
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Allow for manual processing through payload
    if (req.method === "POST") {
      const payload = await req.json();
      
      if (payload.action === "process" || payload.action === "identify") {
        const result = await processProducersBatch();
        
        return new Response(
          JSON.stringify(result),
          {
            status: result.success ? 200 : 500,
            headers: {
              "Content-Type": "application/json",
              ...corsHeaders,
            },
          }
        );
      }
      
      return new Response(
        JSON.stringify({
          success: false,
          message: "Invalid action specified",
        }),
        {
          status: 400,
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
    }
    
    // Default behavior: process a batch
    const result = await processProducersBatch();
    
    return new Response(
      JSON.stringify(result),
      {
        status: result.success ? 200 : 500,
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  } catch (error) {
    console.error("Error handling process-producers-batch request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_producers_batch",
        p_message: "Error handling process-producers-batch request",
        p_stack_trace: error.stack || "",
        p_context: { error: error.message },
      });
    } catch (logError) {
      console.error("Error logging to database:", logError);
    }

    return new Response(
      JSON.stringify({
        success: false,
        error: error.message,
      }),
      {
        status: 500,
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  }
});
