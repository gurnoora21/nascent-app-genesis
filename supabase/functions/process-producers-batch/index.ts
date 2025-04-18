import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { supabase } from "../lib/api-clients.ts";
import { 
  claimProcessingBatch, 
  releaseProcessingBatch, 
  isApiRateLimited, 
  validateBatchIntegrity,
  updateItemStatus,
  calculateBackoff,
  logSystemEvent
} from "../lib/pipeline-utils.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

async function processProducersBatch(): Promise<{
  success: boolean;
  message: string;
  batchId?: string;
  processed?: number;
  failed?: number;
  status?: string;
}> {
  try {
    const workerId = crypto.randomUUID();
    
    console.log(`Worker ${workerId} claiming a producers batch`);
    
    const geniusLimited = await isApiRateLimited("genius", "/songs");
    const discogsLimited = await isApiRateLimited("discogs", "/releases");
    
    if (geniusLimited && discogsLimited) {
      return {
        success: true,
        message: "Both Genius and Discogs APIs are currently rate limited, skipping batch claim",
        status: "delayed"
      };
    }
    
    const batchId = await claimProcessingBatch("process_producers", workerId, 3600);
    
    if (!batchId) {
      return {
        success: true,
        message: "No pending producers batches found to process",
        status: "idle"
      };
    }
    
    console.log(`Worker ${workerId} claimed producers batch ${batchId}`);
    
    const batchValidation = await validateBatchIntegrity(batchId);
    if (!batchValidation.valid) {
      console.warn(`Batch integrity check failed: ${batchValidation.reason}`);
      
      await releaseProcessingBatch(batchId, workerId, "error");
      
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
    
    const batchSize = 10;
    
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
      await releaseProcessingBatch(batchId, workerId, "completed");
      
      return {
        success: true,
        message: "Producers batch claimed but no items to process",
        batchId,
        processed: 0,
        failed: 0,
        status: "completed"
      };
    }
    
    await logSystemEvent(
      "info",
      "process_producers_batch",
      `Processing ${items.length} tracks for producer identification in batch ${batchId}`,
      { numItems: items.length, batchId }
    );
    
    const processedItems: string[] = [];
    const failedItems: string[] = [];
    
    await identifyProducers(items, processedItems, failedItems);
    
    console.log(`Producers batch ${batchId} processing complete. Processed: ${processedItems.length}, Failed: ${failedItems.length}`);
    
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
    
    const allDone = !pendingError && pendingItems === 0;
    
    await releaseProcessingBatch(
      batchId,
      workerId,
      allDone ? "completed" : "processing"
    );
    
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
    
    await logSystemEvent(
      "error",
      "process_producers_batch",
      `Error processing producers batch: ${error.message}`,
      { error: error.stack || error.message },
      crypto.randomUUID()
    );
    
    return {
      success: false,
      message: `Error processing producers batch: ${error.message}`,
      status: "error"
    };
  }
}

async function identifyProducers(
  items: any[], 
  processedItems: string[], 
  failedItems: string[]
): Promise<void> {
  for (const item of items) {
    try {
      if (item.item_type !== "track") {
        throw new Error(`Expected item_type "track", got "${item.item_type}"`);
      }
      
      const trackId = item.item_id;
      console.log(`Identifying producers for track ${trackId}`);
      
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
      
      await updateItemStatus(
        item.id,
        "completed",
        {
          ...item.metadata,
          producers_identified: result.producers?.length || 0,
          sources: result.sources || [],
          processed_at: new Date().toISOString()
        }
      );
      
      processedItems.push(item.id);
      
    } catch (error) {
      console.error(`Error identifying producers for track ${item.item_id}:`, error);
      
      const isTransient = error.message.includes("rate limit") || 
                          error.message.includes("timeout") ||
                          error.message.includes("network");
      
      const shouldRetry = (item.retry_count || 0) < 5 && isTransient;
      
      await updateItemStatus(
        item.id,
        shouldRetry ? "pending" : "error",
        {
          ...item.metadata,
          last_error: error.message,
          retry_after: shouldRetry ? 
            new Date(Date.now() + calculateBackoff(item.retry_count || 0)).toISOString() : null
        },
        error.message
      );
      
      if (!shouldRetry) {
        failedItems.push(item.id);
      }
      
      await logSystemEvent(
        "error",
        "process_producers_batch",
        `Error identifying producers for track ${item.item_id}: ${error.message}`,
        { item, error: error.stack || error.message }
      );
    }
  }
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
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
    
    await logSystemEvent(
      "error",
      "process_producers_batch",
      `Error handling process-producers-batch request: ${error.message}`,
      { error: error.stack || error.message },
      crypto.randomUUID()
    );

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
