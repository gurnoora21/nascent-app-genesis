
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { supabase } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Process a pending batch
async function processBatch(batchType: string): Promise<{
  success: boolean;
  message: string;
  batchId?: string;
  processed?: number;
  failed?: number;
}> {
  try {
    // Generate a unique worker ID
    const workerId = crypto.randomUUID();
    
    console.log(`Worker ${workerId} claiming a batch of type: ${batchType}`);
    
    // Claim a batch to process
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
      .order("created_at", { ascending: true });
    
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
        failed: 0
      };
    }
    
    console.log(`Processing ${items.length} items in batch ${batchId}`);
    
    const processedItems: string[] = [];
    const failedItems: string[] = [];
    
    // Process each item in the batch based on batch type
    for (const item of items) {
      try {
        console.log(`Processing item ${item.id}: ${item.item_type}/${item.item_id}`);
        
        // Process based on batch type and item type
        if (batchType === "process_artists" && item.item_type === "artist") {
          // Call the process-artist function
          const response = await fetch(
            `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/process-artist`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
              },
              body: JSON.stringify({ spotifyId: item.item_id })
            }
          );
          
          const result = await response.json();
          
          if (!result.success) {
            throw new Error(result.message || "Unknown error processing artist");
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
          console.warn(`Unknown batch type or item type combination: ${batchType}/${item.item_type}`);
          
          // Mark as completed anyway to avoid getting stuck
          await supabase
            .from("processing_items")
            .update({
              status: "completed",
              metadata: {
                ...item.metadata,
                warning: `Unknown batch type or item type combination: ${batchType}/${item.item_type}`
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
          p_source: "process_batch",
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
    
    // Get all items for this batch to check overall progress
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
    
    // Update batch with progress
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
      failed: failedItems.length
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
      message: `Error processing batch: ${error.message}`
    };
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
      // For collect_tracks, we add all processed artists from the process_artists batch
      const { data: completedItems, error: itemsError } = await supabase
        .from("processing_items")
        .select("item_id, priority")
        .eq("batch_id", batchId)
        .eq("item_type", "artist")
        .eq("status", "completed");
      
      if (itemsError) {
        console.error(`Error getting completed items from batch:`, itemsError);
      } else if (completedItems && completedItems.length > 0) {
        // Create new processing items for each artist
        const newItems = completedItems.map(item => ({
          batch_id: newBatch.id,
          item_type: "artist",
          item_id: item.item_id,
          status: "pending",
          priority: item.priority
        }));
        
        const { error: insertError } = await supabase
          .from("processing_items")
          .insert(newItems);
        
        if (insertError) {
          console.error(`Error creating items for collect_tracks batch:`, insertError);
        } else {
          console.log(`Added ${newItems.length} artists to collect_tracks batch ${newBatch.id}`);
          
          // Update the batch with the total number of items
          await supabase
            .from("processing_batches")
            .update({
              items_total: newItems.length
            })
            .eq("id", newBatch.id);
        }
      }
    } else if (nextBatchType === "identify_producers") {
      // For identify_producers, we create a batch but leave item creation to process-tracks-batch
      console.log(`identify_producers batch created, but items will be added by process-tracks-batch`);
    } else if (nextBatchType === "process_artists") {
      // For process_artists, add completed items from discover_artists batch
      const { data: completedItems, error: itemsError } = await supabase
        .from("processing_items")
        .select("item_id, priority")
        .eq("batch_id", batchId)
        .eq("item_type", "artist")
        .eq("status", "completed");
      
      if (itemsError) {
        console.error(`Error getting items from parent batch:`, itemsError);
      } else if (completedItems && completedItems.length > 0) {
        // Create new processing items for each artist
        const newItems = completedItems.map(item => ({
          batch_id: newBatch.id,
          item_type: "artist",
          item_id: item.item_id,
          status: "pending",
          priority: item.priority
        }));
        
        const { error: insertError } = await supabase
          .from("processing_items")
          .insert(newItems);
        
        if (insertError) {
          console.error(`Error creating items for process_artists batch:`, insertError);
        } else {
          console.log(`Added ${newItems.length} artists to process_artists batch ${newBatch.id}`);
          
          // Update the batch with the total number of items
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
    const { batchType = "process_artists" } = 
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
