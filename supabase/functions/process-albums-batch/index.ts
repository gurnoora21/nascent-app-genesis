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

// Process a pending albums batch
async function processAlbumsBatch(): Promise<{
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
    
    console.log(`Worker ${workerId} claiming an albums batch`);
    
    // Pre-flight check: Check if API is rate limited
    const apiLimited = await isApiRateLimited("spotify", "/albums");
    if (apiLimited) {
      return {
        success: true,
        message: "API is currently rate limited, skipping batch claim",
        status: "delayed"
      };
    }
    
    // Claim a batch to process with soft locking mechanism
    const { data: batchId, error: claimError } = await supabase.rpc(
      "claim_processing_batch",
      {
        p_batch_type: "process_albums",
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
        message: "No pending albums batches found to process",
        status: "idle"
      };
    }
    
    console.log(`Worker ${workerId} claimed albums batch ${batchId}`);
    
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
    
    // Get items to process
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
      
      // For completed batches, create the next batch in the pipeline
      await createTracksBatch(batchId);
      
      return {
        success: true,
        message: "Albums batch claimed but no items to process",
        batchId,
        processed: 0,
        failed: 0,
        status: "completed"
      };
    }
    
    console.log(`Processing ${items.length} albums in batch ${batchId}`);
    
    const processedItems: string[] = [];
    const failedItems: string[] = [];
    
    // Process album items
    await processAlbumItems(batchId, items, processedItems, failedItems);
    
    console.log(`Albums batch ${batchId} processing complete. Processed: ${processedItems.length}, Failed: ${failedItems.length}`);
    
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
    
    // If batch is completed, create next batch in pipeline
    if (allDone) {
      await createTracksBatch(batchId);
    }
    
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
      message: `Processed ${processedItems.length} albums, failed ${failedItems.length} albums, ${pendingItems || 'unknown'} items remaining`,
      batchId,
      processed: processedItems.length,
      failed: failedItems.length,
      status: allDone ? "completed" : "in_progress"
    };
  } catch (error) {
    console.error("Error processing albums batch:", error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_albums_batch",
      p_message: `Error processing albums batch`,
      p_stack_trace: error.stack || "",
      p_context: { batchType: "process_albums" }
    });
    
    return {
      success: false,
      message: `Error processing albums batch: ${error.message}`,
      status: "error"
    };
  }
}

// Process album items with error handling
async function processAlbumItems(
  batchId: string,
  items: any[], 
  processedItems: string[], 
  failedItems: string[]
): Promise<void> {
  // Process each album - this only updates the album metadata, not track data
  for (const item of items) {
    try {
      console.log(`Processing album ${item.item_id}`);
      
      if (item.item_type !== "album") {
        throw new Error(`Expected item_type "album", got "${item.item_type}"`);
      }
      
      // Get the album details
      const { data: album, error: albumError } = await supabase
        .from("albums")
        .select("*")
        .eq("id", item.item_id)
        .single();
      
      if (albumError || !album) {
        throw new Error(`Album not found: ${albumError?.message || "No data returned"}`);
      }
      
      // Get the artist details for this album
      const { data: artist, error: artistError } = await supabase
        .from("artists")
        .select("*")
        .eq("id", album.artist_id)
        .single();
      
      if (artistError || !artist) {
        throw new Error(`Artist not found: ${artistError?.message || "No data returned"}`);
      }
      
      // Update album with additional metadata if needed
      // This could involve fetching more data from the Spotify API
      // but for now, we're just marking it as processed
      
      // Mark item as processed
      await supabase
        .from("processing_items")
        .update({
          status: "completed",
          metadata: {
            ...item.metadata,
            processed_at: new Date().toISOString()
          }
        })
        .eq("id", item.id);
      
      processedItems.push(item.id);
      
    } catch (error) {
      console.error(`Error processing album ${item.item_id}:`, error);
      
      // If retry count < 5, increment and keep as pending
      const shouldRetry = (item.retry_count || 0) < 5;
      const isTransient = error.message.includes("rate limit") || 
                           error.message.includes("timeout") ||
                           error.message.includes("network");
      
      await supabase
        .from("processing_items")
        .update({
          status: (shouldRetry && isTransient) ? "pending" : "error",
          retry_count: (item.retry_count || 0) + 1,
          last_error: error.message,
          metadata: {
            ...item.metadata,
            last_error: error.message,
            retry_after: (shouldRetry && isTransient) ? 
              new Date(Date.now() + calculateBackoff(item.retry_count || 0)).toISOString() : null
          }
        })
        .eq("id", item.id);
      
      // Only count as failed if we've given up retrying
      if (!shouldRetry || !isTransient) {
        failedItems.push(item.id);
      }
      
      // Log specific error
      await supabase.rpc("log_error", {
        p_error_type: "processing",
        p_source: "process_albums_batch",
        p_message: `Error processing album ${item.item_id}`,
        p_stack_trace: error.stack || "",
        p_context: { item },
        p_item_id: item.item_id,
        p_item_type: item.item_type
      });
    }
  }
}

// Create the next batch in the pipeline (tracks batch)
async function createTracksBatch(albumsBatchId: string): Promise<string | null> {
  try {
    // Create a new batch for processing tracks
    const { data: newBatch, error: createError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "process_tracks",
        status: "pending",
        metadata: {
          parent_batch_id: albumsBatchId,
          parent_batch_type: "process_albums",
          processing_pipeline: true
        }
      })
      .select("id")
      .single();
    
    if (createError) {
      console.error(`Error creating tracks batch:`, createError);
      return null;
    }
    
    console.log(`Created tracks batch with ID ${newBatch.id}`);
    
    // Get completed album items
    const { data: completedItems, error: itemsError } = await supabase
      .from("processing_items")
      .select("item_id, priority, metadata")
      .eq("batch_id", albumsBatchId)
      .eq("item_type", "album")
      .eq("status", "completed");
    
    if (itemsError) {
      console.error(`Error getting completed album items:`, itemsError);
      return newBatch.id;
    }
    
    if (!completedItems || completedItems.length === 0) {
      console.log(`No completed albums found in batch ${albumsBatchId}`);
      return newBatch.id;
    }
    
    // Get the album IDs
    const albumIds = completedItems.map(item => item.item_id);
    
    // Create processing items for each album to process their tracks
    const trackBatchItems = albumIds.map((albumId, index) => {
      const item = completedItems[index];
      return {
        batch_id: newBatch.id,
        item_type: "album_for_tracks",
        item_id: albumId,
        status: "pending",
        priority: item.priority || 5,
        metadata: {
          source_batch_id: albumsBatchId,
          artist_id: item.metadata?.artist_id,
          pipeline_stage: "process_tracks"
        }
      };
    });
    
    if (trackBatchItems.length > 0) {
      const { error: insertError } = await supabase
        .from("processing_items")
        .insert(trackBatchItems);
      
      if (insertError) {
        console.error(`Error creating track batch items:`, insertError);
      } else {
        console.log(`Added ${trackBatchItems.length} albums to process for tracks in batch ${newBatch.id}`);
        
        // Update the batch with the accurate total number of items
        await supabase
          .from("processing_batches")
          .update({
            items_total: trackBatchItems.length
          })
          .eq("id", newBatch.id);
      }
    } else {
      console.log(`No albums to add to tracks batch ${newBatch.id}`);
    }
    
    return newBatch.id;
  } catch (error) {
    console.error(`Error creating tracks batch:`, error);
    return null;
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const result = await processAlbumsBatch();
    
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
    console.error("Error handling process-albums-batch request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_albums_batch",
        p_message: "Error handling process-albums-batch request",
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
