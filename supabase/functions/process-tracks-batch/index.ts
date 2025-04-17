import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { supabase } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Create a new batch for processing tracks
async function createTracksBatch(limit: number = 50): Promise<string> {
  try {
    // Get tracks that don't have producers yet
    const { data: tracks, error: tracksError } = await supabase
      .from("tracks")
      .select("id")
      .not("id", "in", supabase.from("track_producers").select("track_id"))
      .limit(limit);
    
    if (tracksError) {
      console.error("Error getting tracks without producers:", tracksError);
      throw tracksError;
    }
    
    if (!tracks || tracks.length === 0) {
      throw new Error("No tracks found without producers");
    }
    
    // Create a new batch
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "identify_producers",
        status: "pending",
        items_total: tracks.length,
        metadata: { limit }
      })
      .select("id")
      .single();
    
    if (batchError) {
      console.error("Error creating tracks batch:", batchError);
      throw batchError;
    }
    
    // Add tracks to the batch
    const batchItems = tracks.map((track) => ({
      batch_id: batch.id,
      item_type: "track",
      item_id: track.id,
      status: "pending"
    }));
    
    const { error: itemsError } = await supabase
      .from("processing_items")
      .insert(batchItems);
    
    if (itemsError) {
      console.error("Error adding tracks to batch:", itemsError);
      throw itemsError;
    }
    
    return batch.id;
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
}> {
  try {
    // Generate a unique worker ID - Using crypto.randomUUID() for consistency
    const workerId = crypto.randomUUID();
    
    console.log(`Worker ${workerId} claiming a batch of type: identify_producers`);
    
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
        message: "No pending batches found to process"
      };
    }
    
    console.log(`Worker ${workerId} claimed batch ${batchId}`);
    
    // Get items to process
    const { data: items, error: itemsError } = await supabase
      .from("processing_items")
      .select("*")
      .eq("batch_id", batchId)
      .eq("status", "pending")
      .order("priority", { ascending: false })
      .order("created_at", { ascending: true })
      .limit(10); // Process 10 items at a time to avoid timeouts
    
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
        failed: 0
      };
    }
    
    console.log(`Processing ${items.length} items in batch ${batchId}`);
    
    const processedItems: string[] = [];
    const failedItems: string[] = [];
    
    // Process each item in the batch
    for (const item of items) {
      try {
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
              body: JSON.stringify({ trackId: item.item_id })
            }
          );
          
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
                result
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
                warning: `Unknown item type: ${item.item_type}`
              }
            })
            .eq("id", item.id);
          
          processedItems.push(item.id);
        }
      } catch (itemError) {
        console.error(`Error processing item ${item.id}:`, itemError);
        
        // Increment retry count and mark as error
        await supabase
          .from("processing_items")
          .update({
            status: "error",
            retry_count: (item.retry_count || 0) + 1,
            last_error: itemError.message
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
        
        failedItems.push(item.id);
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
    
    return {
      success: true,
      message: `Processed ${processedItems.length} items, failed ${failedItems.length} items, ${count || 'unknown'} items remaining`,
      batchId,
      processed: processedItems.length,
      failed: failedItems.length
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
      message: `Error processing tracks batch: ${error.message}`
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
          batchId
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
