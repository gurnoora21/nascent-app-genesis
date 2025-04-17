
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

// Create a new batch for processing tracks with validation
async function createTracksBatch(limit: number = 50): Promise<string> {
  try {
    // Pre-flight check: Check if APIs are rate limited
    const spotifyLimited = await isApiRateLimited("spotify", "/tracks");
    const geniusLimited = await isApiRateLimited("genius", "/search");
    
    if (spotifyLimited || geniusLimited) {
      throw new Error(`APIs currently rate limited, deferring batch creation`);
    }
    
    // First, check if there's an existing batch of type identify_producers
    const { data: existingBatches, error: batchCheckError } = await supabase
      .from("processing_batches")
      .select("id")
      .eq("batch_type", "identify_producers")
      .eq("status", "pending")
      .order("created_at", { ascending: true })
      .limit(1);
    
    if (batchCheckError) {
      console.error("Error checking for existing batches:", batchCheckError);
      throw batchCheckError;
    }
    
    let batchId;
    
    if (existingBatches && existingBatches.length > 0) {
      // Use existing batch
      batchId = existingBatches[0].id;
      console.log(`Using existing identify_producers batch ${batchId}`);
    } else {
      // Create a new batch
      const { data: newBatch, error: batchError } = await supabase
        .from("processing_batches")
        .insert({
          batch_type: "identify_producers",
          status: "pending",
          metadata: { 
            limit,
            created_by: "process-tracks-batch",
            created_at: new Date().toISOString()
          }
        })
        .select("id")
        .single();
      
      if (batchError) {
        console.error("Error creating tracks batch:", batchError);
        throw batchError;
      }
      
      batchId = newBatch.id;
      console.log(`Created new identify_producers batch ${batchId}`);
    }
    
    // Exclude tracks that:
    // 1. Already have producers
    // 2. Are currently in a pending processing item
    // 3. Have failed processing 5+ times
    const subQuery = supabase
      .from("processing_items")
      .select("item_id")
      .eq("item_type", "track")
      .in("status", ["pending", "processing"])
      .or(`retry_count.gte.5,status.eq.error`);
      
    // Get tracks that don't have producers yet
    const { data: tracks, error: tracksError } = await supabase
      .from("tracks")
      .select("id, name, artist_id, popularity")
      .not("id", "in", supabase.from("track_producers").select("track_id"))
      .not("id", "in", subQuery)
      .order("popularity", { ascending: false }) // Process popular tracks first
      .limit(limit);
    
    if (tracksError) {
      console.error("Error getting tracks without producers:", tracksError);
      throw tracksError;
    }
    
    if (!tracks || tracks.length === 0) {
      throw new Error("No tracks found without producers");
    }
    
    // Add tracks to the batch with priority based on popularity
    const batchItems = tracks.map((track) => ({
      batch_id: batchId,
      item_type: "track",
      item_id: track.id,
      status: "pending",
      priority: track.popularity ? Math.min(Math.floor(track.popularity / 10), 10) : 5, // 0-10 priority scale
      metadata: {
        track_name: track.name,
        artist_id: track.artist_id
      }
    }));
    
    const { error: itemsError } = await supabase
      .from("processing_items")
      .insert(batchItems);
    
    if (itemsError) {
      console.error("Error adding tracks to batch:", itemsError);
      throw itemsError;
    }
    
    // Update batch with item count
    await supabase
      .from("processing_batches")
      .update({
        items_total: batchItems.length
      })
      .eq("id", batchId);
    
    console.log(`Added ${batchItems.length} tracks to batch ${batchId}`);
    
    return batchId;
  } catch (error) {
    console.error("Error creating tracks batch:", error);
    throw error;
  }
}

// Process a pending batch
async function processTracksBatch(): Promise<{
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
    
    console.log(`Worker ${workerId} claiming a batch of type: identify_producers`);
    
    // Pre-flight check: Check if APIs are rate limited
    const spotifyLimited = await isApiRateLimited("spotify", "/tracks");
    const geniusLimited = await isApiRateLimited("genius", "/search");
    const discogsLimited = await isApiRateLimited("discogs", "/database/search");
    
    if (spotifyLimited || geniusLimited || discogsLimited) {
      return {
        success: true,
        message: "APIs currently rate limited, skipping batch processing",
        status: "delayed"
      };
    }
    
    // Claim a batch to process
    const { data: batchId, error: claimError } = await supabase.rpc(
      "claim_processing_batch",
      {
        p_batch_type: "identify_producers",
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
    
    // Calculate optimal batch size based on API limits and batch history
    // Start with conservative default 
    let batchSize = 10;
    
    // Adjust based on rate limit data if available
    if (!spotifyLimited && !geniusLimited && !discogsLimited) {
      // If no rate limiting detected, we can be more aggressive
      batchSize = 15;
    }
    
    // Additional adjustments based on batch history could be added here
    
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
    
    // Process each item in the batch with proper handling
    for (const item of items) {
      try {
        // Skip items scheduled for future retry based on metadata
        if (
          item.metadata?.retry_after && 
          new Date(item.metadata.retry_after) > new Date()
        ) {
          console.log(`Skipping item ${item.id} - scheduled for retry after ${item.metadata.retry_after}`);
          continue;
        }
        
        console.log(`Processing item ${item.id}: ${item.item_type}/${item.item_id}`);
        
        if (item.item_type === "track") {
          // Call the identify-producers function
          const response = await fetch(
            `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/identify-producers`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
              },
              body: JSON.stringify({ 
                trackId: item.item_id,
                batchItemId: item.id,
                workerId
              })
            }
          );
          
          if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`HTTP error ${response.status}: ${errorText}`);
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
                result: {
                  producers: result.data?.producers?.length || 0,
                  sources: result.data?.sources || [],
                  confidence: result.data?.confidence || "medium"
                },
                completed_at: new Date().toISOString()
              }
            })
            .eq("id", item.id);
          
          processedItems.push(item.id);
        } else {
          console.warn(`Unknown item type: ${item.item_type}`);
          
          // Mark as completed anyway to avoid getting stuck
          await supabase
            .from("processing_items")
            .update({
              status: "completed",
              metadata: {
                ...item.metadata,
                warning: `Unknown item type: ${item.item_type}`,
                completed_at: new Date().toISOString()
              }
            })
            .eq("id", item.id);
          
          processedItems.push(item.id);
        }
      } catch (itemError) {
        console.error(`Error processing item ${item.id}:`, itemError);
        
        // Determine if error is transient or permanent
        const isTransient = itemError.message.includes("rate limit") || 
                             itemError.message.includes("timeout") ||
                             itemError.message.includes("network") ||
                             itemError.message.includes("429") ||
                             itemError.message.includes("503");
        
        // Increment retry count and mark appropriately
        const shouldRetry = (item.retry_count || 0) < 5 && isTransient;
        const backoffTime = calculateBackoff(item.retry_count || 0);
        const retryAfter = new Date(Date.now() + backoffTime).toISOString();
        
        await supabase
          .from("processing_items")
          .update({
            status: shouldRetry ? "pending" : "error",
            retry_count: (item.retry_count || 0) + 1,
            last_error: itemError.message,
            metadata: {
              ...item.metadata,
              last_error: itemError.message,
              last_error_at: new Date().toISOString(),
              is_transient: isTransient,
              retry_after: shouldRetry ? retryAfter : null,
              backoff_ms: backoffTime
            }
          })
          .eq("id", item.id);
        
        // Log error
        await supabase.rpc("log_error", {
          p_error_type: "processing",
          p_source: "process_tracks_batch",
          p_message: `Error processing item ${item.item_type}/${item.item_id}`,
          p_stack_trace: itemError.stack || "",
          p_context: item,
          p_item_id: item.item_id,
          p_item_type: item.item_type
        });
        
        // Only count as failed if we've given up retrying
        if (!shouldRetry) {
          failedItems.push(item.id);
        }
      }
    }
    
    console.log(`Batch ${batchId} processing complete. Processed: ${processedItems.length}, Failed: ${failedItems.length}`);
    
    // Get remaining items count
    const { count, error: countError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "pending");
    
    if (countError) {
      console.error("Error getting remaining items count:", countError);
    }
    
    // Release the batch
    const allDone = !countError && count === 0;
    
    await supabase.rpc(
      "release_processing_batch",
      {
        p_batch_id: batchId,
        p_worker_id: workerId,
        p_status: allDone ? "completed" : "processing"
      }
    );
    
    // Update batch with progress
    const { data: currentBatch, error: batchError } = await supabase
      .from("processing_batches")
      .select("items_total, items_processed, items_failed")
      .eq("id", batchId)
      .single();
    
    if (!batchError) {
      await supabase
        .from("processing_batches")
        .update({
          items_processed: (currentBatch.items_processed || 0) + processedItems.length,
          items_failed: (currentBatch.items_failed || 0) + failedItems.length
        })
        .eq("id", batchId);
    }
    
    return {
      success: true,
      message: `Processed ${processedItems.length} items, failed ${failedItems.length} items, ${count || 'unknown'} items remaining`,
      batchId,
      processed: processedItems.length,
      failed: failedItems.length,
      status: allDone ? "completed" : "in_progress"
    };
  } catch (error) {
    console.error("Error processing tracks batch:", error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_tracks_batch",
      p_message: `Error processing tracks batch`,
      p_stack_trace: error.stack || "",
      p_context: {}
    });
    
    return {
      success: false,
      message: `Error processing tracks batch: ${error.message}`,
      status: "error"
    };
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    let result;
    
    if (req.method === "POST") {
      // Parse request body
      const { action, limit = 50 } = await req.json();
      
      if (action === "create") {
        // Create a new batch
        const batchId = await createTracksBatch(limit);
        
        result = {
          success: true,
          message: `Created a new tracks batch with ${limit} tracks`,
          batchId,
          status: "created"
        };
      } else {
        // Process a batch
        result = await processTracksBatch();
      }
    } else {
      // Default to processing a batch
      result = await processTracksBatch();
    }
    
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
    console.error("Error handling process-tracks-batch request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_tracks_batch",
        p_message: "Error handling process-tracks-batch request",
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
