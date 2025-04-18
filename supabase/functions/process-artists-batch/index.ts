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

// Process a pending artists batch
async function processArtistsBatch(): Promise<{
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
    
    console.log(`Worker ${workerId} claiming an artists batch`);
    
    // Pre-flight check: Check if API is rate limited
    const apiLimited = await isApiRateLimited("spotify", "/artists");
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
        p_batch_type: "process_artists",
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
        message: "No pending artists batches found to process",
        status: "idle"
      };
    }
    
    console.log(`Worker ${workerId} claimed artists batch ${batchId}`);
    
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
      await createAlbumsBatch(batchId);
      
      return {
        success: true,
        message: "Artists batch claimed but no items to process",
        batchId,
        processed: 0,
        failed: 0,
        status: "completed"
      };
    }
    
    console.log(`Processing ${items.length} artists in batch ${batchId}`);
    
    const processedItems: string[] = [];
    const failedItems: string[] = [];
    
    // Process artist batch
    await processArtistItems(batchId, workerId, items, processedItems, failedItems);
    
    console.log(`Artists batch ${batchId} processing complete. Processed: ${processedItems.length}, Failed: ${failedItems.length}`);
    
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
      await createAlbumsBatch(batchId);
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
      message: `Processed ${processedItems.length} artists, failed ${failedItems.length} artists, ${pendingItems || 'unknown'} items remaining`,
      batchId,
      processed: processedItems.length,
      failed: failedItems.length,
      status: allDone ? "completed" : "in_progress"
    };
  } catch (error) {
    console.error("Error processing artists batch:", error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_artists_batch",
      p_message: `Error processing artists batch`,
      p_stack_trace: error.stack || "",
      p_context: { batchType: "process_artists" }
    });
    
    return {
      success: false,
      message: `Error processing artists batch: ${error.message}`,
      status: "error"
    };
  }
}

// Process artist items with retries and error handling
async function processArtistItems(
  batchId: string, 
  workerId: string,
  items: any[], 
  processedItems: string[], 
  failedItems: string[]
): Promise<void> {
  try {
    const response = await fetch(
      `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/process-artist`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
        },
        body: JSON.stringify({ 
          batchId,
          workerId,
          maxItems: items.length
        })
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error ${response.status}: ${await response.text()}`);
    }
    
    const result = await response.json();
    
    if (!result.success) {
      throw new Error(result.message || "Unknown error processing artist batch");
    }
    
    // Update our tracking arrays
    processedItems.push(...(result.data?.processedItems || []));
    failedItems.push(...(result.data?.failedItems || []));
  } catch (error) {
    console.error("Error calling process-artist function:", error);
    
    // Mark all items as error
    for (const item of items) {
      // If retry count < 5, increment and keep as pending
      const shouldRetry = (item.retry_count || 0) < 5;
      
      await supabase
        .from("processing_items")
        .update({
          status: shouldRetry ? "pending" : "error",
          retry_count: (item.retry_count || 0) + 1,
          last_error: error.message,
          // Add backoff metadata for retries
          metadata: {
            ...item.metadata,
            last_error: error.message,
            retry_after: shouldRetry ? new Date(Date.now() + calculateBackoff(item.retry_count || 0)).toISOString() : null
          }
        })
        .eq("id", item.id);
      
      // Only count as failed if we've given up retrying
      if (!shouldRetry) {
        failedItems.push(item.id);
      }
    }
    
    // Log batch-level error
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_artist_batch",
      p_message: `Error processing artist batch ${batchId}`,
      p_stack_trace: error.stack || "",
      p_context: { batchId, items: items.map(i => i.id) }
    });
  }
}

// Create the next batch in the pipeline (albums batch)
async function createAlbumsBatch(artistsBatchId: string): Promise<string | null> {
  try {
    // Create a new batch for processing albums
    const { data: newBatch, error: createError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "process_albums",
        status: "pending",
        metadata: {
          parent_batch_id: artistsBatchId,
          parent_batch_type: "process_artists",
          processing_pipeline: true
        }
      })
      .select("id")
      .single();
    
    if (createError) {
      console.error(`Error creating albums batch:`, createError);
      return null;
    }
    
    console.log(`Created albums batch with ID ${newBatch.id}`);
    
    // Get completed artist items from the artists batch
    const { data: completedItems, error: itemsError } = await supabase
      .from("processing_items")
      .select("item_id, priority")
      .eq("batch_id", artistsBatchId)
      .eq("item_type", "artist")
      .eq("status", "completed");
    
    if (itemsError) {
      console.error(`Error getting completed artist items:`, itemsError);
      return newBatch.id;
    }
    
    if (!completedItems || completedItems.length === 0) {
      console.log(`No completed artists found in batch ${artistsBatchId}`);
      return newBatch.id;
    }
    
    // Get albums for these artists
    const artistIds = completedItems.map(item => item.item_id);
    
    // Fetch albums to process
    const { data: albums, error: albumsError } = await supabase
      .from("albums")
      .select("id, artist_id")
      .in("artist_id", artistIds);
    
    if (albumsError) {
      console.error(`Error fetching albums for artists:`, albumsError);
      return newBatch.id;
    }
    
    if (!albums || albums.length === 0) {
      console.log(`No albums found for artists in batch ${artistsBatchId}`);
      return newBatch.id;
    }
    
    // Create processing items for each album
    const albumItems = albums.map(album => {
      // Find the priority of the parent artist
      const artistItem = completedItems.find(item => item.item_id === album.artist_id);
      const priority = artistItem ? artistItem.priority : 5; // Default priority
      
      return {
        batch_id: newBatch.id,
        item_type: "album",
        item_id: album.id,
        status: "pending",
        priority,
        metadata: {
          source_batch_id: artistsBatchId,
          artist_id: album.artist_id,
          pipeline_stage: "process_albums"
        }
      };
    });
    
    if (albumItems.length > 0) {
      const { error: insertError } = await supabase
        .from("processing_items")
        .insert(albumItems);
      
      if (insertError) {
        console.error(`Error creating album items:`, insertError);
      } else {
        console.log(`Added ${albumItems.length} albums to process_albums batch ${newBatch.id}`);
        
        // Update the batch with the accurate total number of items
        await supabase
          .from("processing_batches")
          .update({
            items_total: albumItems.length
          })
          .eq("id", newBatch.id);
      }
    } else {
      console.log(`No albums to add to batch ${newBatch.id}`);
    }
    
    return newBatch.id;
  } catch (error) {
    console.error(`Error creating albums batch:`, error);
    return null;
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const result = await processArtistsBatch();
    
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
    console.error("Error handling process-artists-batch request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_artists_batch",
        p_message: "Error handling process-artists-batch request",
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
