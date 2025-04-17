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

// Process a pending batch with improved handling
async function processBatch(batchType: string): Promise<{
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
    
    console.log(`Worker ${workerId} claiming a batch of type: ${batchType}`);
    
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
        p_batch_type: batchType,
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
        message: "No pending batches found to process",
        status: "idle"
      };
    }
    
    console.log(`Worker ${workerId} claimed batch ${batchId}`);
    
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
    
    // Get batch details
    const { data: batchDetails, error: batchDetailsError } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", batchId)
      .single();
    
    if (batchDetailsError) {
      console.error("Error getting batch details:", batchDetailsError);
      throw batchDetailsError;
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
      await createNextBatchInPipeline(batchId);
      
      return {
        success: true,
        message: "Batch claimed but no items to process",
        batchId,
        processed: 0,
        failed: 0,
        status: "completed"
      };
    }
    
    console.log(`Processing ${items.length} items in batch ${batchId}`);
    
    const processedItems: string[] = [];
    const failedItems: string[] = [];
    
    // Process based on batch type
    if (batchType === "process_artists") {
      // Call process-artist function with batch
      await processArtistBatch(batchId, workerId, items, processedItems, failedItems);
    } else if (batchType === "collect_tracks") {
      // Call collect-tracks function with batch
      await processTrackCollection(batchId, workerId, items, processedItems, failedItems);
    } else if (batchType === "identify_producers") {
      // Call identify-producers function with batch
      await processProducerIdentification(batchId, workerId, items, processedItems, failedItems);
    } else {
      console.warn(`Unknown batch type: ${batchType}`);
      
      // Mark all items as completed anyway to avoid getting stuck
      for (const item of items) {
        await supabase
          .from("processing_items")
          .update({
            status: "completed",
            metadata: {
              ...item.metadata,
              warning: `Unknown batch type: ${batchType}`
            }
          })
          .eq("id", item.id);
        
        processedItems.push(item.id);
      }
    }
    
    console.log(`Batch ${batchId} processing complete. Processed: ${processedItems.length}, Failed: ${failedItems.length}`);
    
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
      await createNextBatchInPipeline(batchId);
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
      message: `Processed ${processedItems.length} items, failed ${failedItems.length} items, ${pendingItems || 'unknown'} items remaining`,
      batchId,
      processed: processedItems.length,
      failed: failedItems.length,
      status: allDone ? "completed" : "in_progress"
    };
  } catch (error) {
    console.error("Error processing batch:", error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_batch",
      p_message: `Error processing batch`,
      p_stack_trace: error.stack || "",
      p_context: { batchType }
    });
    
    return {
      success: false,
      message: `Error processing batch: ${error.message}`,
      status: "error"
    };
  }
}

// Process artist batch with retries and error handling
async function processArtistBatch(
  batchId: string, 
  workerId: string,
  items: any[], 
  processedItems: string[], 
  failedItems: string[]
): Promise<void> {
  // Instead of processing artists one by one, call the updated process-artist function with batchId
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

// Process track collection with retries and error handling
async function processTrackCollection(
  batchId: string, 
  workerId: string,
  items: any[], 
  processedItems: string[], 
  failedItems: string[]
): Promise<void> {
  // Process each artist to collect their tracks
  for (const item of items) {
    try {
      if (item.item_type !== "artist") {
        throw new Error(`Expected item_type "artist", got "${item.item_type}"`);
      }
      
      console.log(`Collecting tracks for artist ${item.item_id}`);
      
      // Get the artist details
      const { data: artist, error: artistError } = await supabase
        .from("artists")
        .select("*")
        .eq("id", item.item_id)
        .single();
      
      if (artistError || !artist) {
        throw new Error(`Artist not found: ${artistError?.message || "No data returned"}`);
      }
      
      // Fetch albums for this artist and create track collection tasks
      const response = await fetch(
        `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/collect-tracks`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
          },
          body: JSON.stringify({ 
            artistId: item.item_id,
            priority: item.priority
          })
        }
      );
      
      if (!response.ok) {
        throw new Error(`HTTP error ${response.status}: ${await response.text()}`);
      }
      
      const result = await response.json();
      
      if (!result.success) {
        throw new Error(result.message || "Unknown error collecting tracks");
      }
      
      // Mark item as processed
      await supabase
        .from("processing_items")
        .update({
          status: "completed",
          metadata: {
            ...item.metadata,
            albums_processed: result.data?.albumsProcessed || 0,
            tracks_collected: result.data?.tracksCollected || 0
          }
        })
        .eq("id", item.id);
      
      processedItems.push(item.id);
      
    } catch (error) {
      console.error(`Error processing track collection for item ${item.id}:`, error);
      
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
        p_source: "collect_tracks",
        p_message: `Error collecting tracks for artist ${item.item_id}`,
        p_stack_trace: error.stack || "",
        p_context: { item },
        p_item_id: item.item_id,
        p_item_type: item.item_type
      });
    }
  }
}

// Process producer identification with retries and error handling
async function processProducerIdentification(
  batchId: string, 
  workerId: string,
  items: any[], 
  processedItems: string[], 
  failedItems: string[]
): Promise<void> {
  // Call identify-producers function for each track
  for (const item of items) {
    try {
      if (item.item_type !== "track") {
        throw new Error(`Expected item_type "track", got "${item.item_type}"`);
      }
      
      console.log(`Identifying producers for track ${item.item_id}`);
      
      // Call the identify-producers function
      const response = await fetch(
        `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/identify-producers`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
          },
          body: JSON.stringify({ trackId: item.item_id })
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
            producers_identified: result.data?.producers?.length || 0,
            sources: result.data?.sources || []
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
        p_source: "identify_producers",
        p_message: `Error identifying producers for track ${item.item_id}`,
        p_stack_trace: error.stack || "",
        p_context: { item },
        p_item_id: item.item_id,
        p_item_type: item.item_type
      });
    }
  }
}

// Create the next batch in the pipeline based on the current batch type
async function createNextBatchInPipeline(batchId: string): Promise<string | null> {
  try {
    // Get the current batch to determine its type
    const { data: currentBatch, error: batchError } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", batchId)
      .single();
    
    if (batchError) {
      console.error(`Error getting batch ${batchId}:`, batchError);
      return null;
    }
    
    let nextBatchType: string | null = null;
    
    // Determine the next batch type in the pipeline
    switch (currentBatch.batch_type) {
      case "discover_artists":
        nextBatchType = "process_artists";
        break;
      case "process_artists":
        nextBatchType = "collect_tracks";
        break;
      case "collect_tracks":
        nextBatchType = "identify_producers";
        break;
      // Once producer identification is complete, the pipeline ends
      default:
        return null;
    }
    
    if (!nextBatchType) {
      return null;
    }
    
    console.log(`Creating next batch in pipeline: ${nextBatchType} after ${currentBatch.batch_type}`);
    
    // Create the next batch
    const { data: newBatch, error: createError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: nextBatchType,
        status: "pending",
        metadata: {
          parent_batch_id: batchId,
          parent_batch_type: currentBatch.batch_type,
          processing_pipeline: true
        }
      })
      .select("id")
      .single();
    
    if (createError) {
      console.error(`Error creating next batch in pipeline:`, createError);
      return null;
    }
    
    console.log(`Created next batch in pipeline: ${nextBatchType} with ID ${newBatch.id}`);
    
    // Add appropriate items based on the batch type
    if (nextBatchType === "collect_tracks") {
      // For collect_tracks, we add only COMPLETED processed artists 
      const { data: completedItems, error: itemsError } = await supabase
        .from("processing_items")
        .select("item_id, priority")
        .eq("batch_id", batchId)
        .eq("item_type", "artist")
        .eq("status", "completed");  // Only get completed items
      
      if (itemsError) {
        console.error(`Error getting completed items from batch:`, itemsError);
      } else if (completedItems && completedItems.length > 0) {
        // Create new processing items for each artist
        const newItems = completedItems.map(item => ({
          batch_id: newBatch.id,
          item_type: "artist",
          item_id: item.item_id,
          status: "pending",
          priority: item.priority,
          metadata: {
            source_batch_id: batchId,
            pipeline_stage: "collect_tracks"
          }
        }));
        
        const { error: insertError } = await supabase
          .from("processing_items")
          .insert(newItems);
        
        if (insertError) {
          console.error(`Error creating items for collect_tracks batch:`, insertError);
        } else {
          console.log(`Added ${newItems.length} artists to collect_tracks batch ${newBatch.id}`);
          
          // Update the batch with the accurate total number of items
          await supabase
            .from("processing_batches")
            .update({
              items_total: newItems.length
            })
            .eq("id", newBatch.id);
        }
      }
    } else if (nextBatchType === "identify_producers") {
      // For identify_producers, collect all tracks that have been processed
      // Find tracks from the collect_tracks phase that are now ready for producer identification
      const { data: processedArtists, error: artistsError } = await supabase
        .from("processing_items")
        .select("item_id")
        .eq("batch_id", batchId)
        .eq("status", "completed");
      
      if (artistsError || !processedArtists) {
        console.error(`Error getting processed artists:`, artistsError);
        return newBatch.id;
      }
      
      const artistIds = processedArtists.map(item => item.item_id);
      
      if (artistIds.length === 0) {
        console.log(`No processed artists found, skipping track collection`);
        return newBatch.id;
      }
      
      // Get tracks for these artists that don't have producers yet
      const { data: tracks, error: tracksError } = await supabase
        .from("tracks")
        .select("id")
        .in("artist_id", artistIds)
        .not("id", "in", supabase.from("track_producers").select("track_id"))
        .limit(100); // Limit to a reasonable batch size
      
      if (tracksError) {
        console.error(`Error getting tracks for producer identification:`, tracksError);
        return newBatch.id;
      }
      
      if (!tracks || tracks.length === 0) {
        console.log(`No tracks found for producer identification`);
        return newBatch.id;
      }
      
      // Create processing items for each track
      const trackItems = tracks.map(track => ({
        batch_id: newBatch.id,
        item_type: "track",
        item_id: track.id,
        status: "pending",
        priority: 10, // High priority
        metadata: {
          source_batch_id: batchId,
          pipeline_stage: "identify_producers"
        }
      }));
      
      const { error: insertError } = await supabase
        .from("processing_items")
        .insert(trackItems);
      
      if (insertError) {
        console.error(`Error creating track items for producer identification:`, insertError);
      } else {
        console.log(`Added ${trackItems.length} tracks to identify_producers batch ${newBatch.id}`);
        
        // Update batch with item count
        await supabase
          .from("processing_batches")
          .update({
            items_total: trackItems.length
          })
          .eq("id", newBatch.id);
      }
    } else if (nextBatchType === "process_artists") {
      // For process_artists, add only COMPLETED items from discover_artists batch
      const { data: completedItems, error: itemsError } = await supabase
        .from("processing_items")
        .select("item_id, priority")
        .eq("batch_id", batchId)
        .eq("item_type", "artist")
        .eq("status", "completed");  // Only get completed items
      
      if (itemsError) {
        console.error(`Error getting completed items from parent batch:`, itemsError);
      } else if (completedItems && completedItems.length > 0) {
        // Create new processing items for each artist
        const newItems = completedItems.map(item => ({
          batch_id: newBatch.id,
          item_type: "artist",
          item_id: item.item_id,
          status: "pending",
          priority: item.priority,
          metadata: {
            source_batch_id: batchId,
            pipeline_stage: "process_artists"
          }
        }));
        
        const { error: insertError } = await supabase
          .from("processing_items")
          .insert(newItems);
        
        if (insertError) {
          console.error(`Error creating items for process_artists batch:`, insertError);
        } else {
          console.log(`Added ${newItems.length} artists to process_artists batch ${newBatch.id}`);
          
          // Update the batch with the accurate total number of items
          await supabase
            .from("processing_batches")
            .update({
              items_total: newItems.length
            })
            .eq("id", newBatch.id);
        }
      }
    }
    
    return newBatch.id;
  } catch (error) {
    console.error(`Error creating next batch in pipeline:`, error);
    return null;
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Parse request body
    const { batchType = "process_artists", maxRetries = 5 } = 
      req.method === "POST" ? await req.json() : {};
    
    const result = await processBatch(batchType);
    
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
    console.error("Error handling process-batch request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_batch",
        p_message: "Error handling process-batch request",
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
