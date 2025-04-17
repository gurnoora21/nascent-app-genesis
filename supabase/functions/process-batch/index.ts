
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { supabase } from "../lib/api-clients.ts";
import { v4 } from "https://deno.land/std@0.177.0/uuid/mod.ts";

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
    // Generate a unique worker ID - Fix: proper usage of v4
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
        
        // Process based on item type
        if (item.item_type === "artist") {
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
    
    // Release the batch
    const allDone = processedItems.length + failedItems.length === items.length;
    
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
      message: `Processed ${processedItems.length} items, failed ${failedItems.length} items`,
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

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Parse request body
    const { batchType = "discover_artists" } = 
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
