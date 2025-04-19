import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { 
  logSystemEvent,
  isApiRateLimited,
  calculateBackoffMs,
  sleep,
  incrementMetric
} from "../lib/pipeline-utils.ts";
import { supabase, spotify } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Process an artists batch by creating paginated album page jobs
async function processAlbumsDispatcher(): Promise<{
  success: boolean;
  message: string;
  batchId?: string;
  pages?: number;
  artistId?: string;
  status?: string;
}> {
  try {
    // Generate a unique worker ID
    const workerId = crypto.randomUUID();
    const correlationId = crypto.randomUUID();
    
    console.log(`Worker ${workerId} claiming an artists batch`);
    
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
    
    // Get batch details
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", batchId)
      .single();
    
    if (batchError || !batch) {
      throw new Error(`Batch ${batchId} not found: ${batchError?.message || "No data returned"}`);
    }
    
    // Get items to process (artists)
    const { data: items, error: itemsError } = await supabase
      .from("processing_items")
      .select("*")
      .eq("batch_id", batchId)
      .eq("status", "pending")
      .eq("item_type", "artist")
      .order("priority", { ascending: false })
      .order("created_at", { ascending: true })
      .limit(10); // Process up to 10 artists per run
    
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
        message: "Artists batch claimed but no items to process",
        batchId,
        status: "completed"
      };
    }
    
    console.log(`Processing ${items.length} artists in batch ${batchId}`);
    
    // Define constants for pagination
    const PAGE_SIZE = 20; // Albums per page
    let totalPages = 0;
    let totalArtists = 0;
    
    // Process each artist by creating album page batches
    for (const item of items) {
      try {
        console.log(`Processing artist ${item.item_id}`);
        
        // Get the artist details
        const { data: artist, error: artistError } = await supabase
          .from("artists")
          .select("*")
          .eq("id", item.item_id)
          .single();
        
        if (artistError || !artist) {
          throw new Error(`Artist not found: ${artistError?.message || "No data returned"}`);
        }
        
        if (!artist.spotify_id) {
          throw new Error(`Artist ${artist.name} does not have a Spotify ID`);
        }
        
        // Get total albums count for artist (limit=1 to minimize API load)
        const { data: countResponse, error: countError } = await spotify.getArtistAlbums(
          artist.spotify_id,
          { limit: 1, offset: 0 }
        );
        
        if (countError) {
          throw new Error(`Error getting album count: ${countError.message}`);
        }
        
        const totalAlbums = countResponse.total || 0;
        const pages = Math.ceil(totalAlbums / PAGE_SIZE);
        
        await logSystemEvent('info', 'process_album_dispatcher', 
          `Artist ${artist.name} has ${totalAlbums} albums, creating ${pages} album page batches`, 
          { artistId: artist.id, artistName: artist.name, totalAlbums, pages, correlationId },
          correlationId
        );
        
        // Create album page batches
        for (let i = 0; i < pages; i++) {
          const offset = i * PAGE_SIZE;
          
          // Create a new album page batch
          const { data: albumPageBatch, error: albumBatchError } = await supabase
            .from("processing_batches")
            .insert({
              batch_type: "process_album_page",
              status: "pending",
              metadata: {
                artist_id: artist.id,
                artist_name: artist.name,
                spotify_id: artist.spotify_id,
                parent_batch_id: batchId,
                offset,
                limit: PAGE_SIZE,
                page_index: i,
                total_pages: pages,
                correlation_id: correlationId
              }
            })
            .select("id")
            .single();
          
          if (albumBatchError || !albumPageBatch) {
            throw new Error(`Error creating album page batch: ${albumBatchError?.message || "No data returned"}`);
          }
          
          totalPages++;
          
          // Call the album page processing function
          // Do this asynchronously to not block this loop
          if (i === 0) {  // Process the first page immediately
            edgeRuntime.waitUntil(
              supabase.functions.invoke("process-album-page", {
                body: {
                  batchId: albumPageBatch.id,
                  artistId: artist.id,
                  offset,
                  limit: PAGE_SIZE,
                  correlationId
                }
              })
            );
          }
        }
        
        // Mark artist as processed
        await supabase
          .from("processing_items")
          .update({
            status: "completed",
            metadata: {
              ...item.metadata,
              processed_at: new Date().toISOString(),
              album_pages: pages,
              total_albums: totalAlbums
            }
          })
          .eq("id", item.id);
        
        totalArtists++;
        
      } catch (error) {
        console.error(`Error processing artist ${item.item_id}:`, error);
        
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
                new Date(Date.now() + calculateBackoffMs(item.retry_count || 0)).toISOString() : null
            }
          })
          .eq("id", item.id);
        
        // Log specific error
        await supabase.rpc("log_error", {
          p_error_type: "processing",
          p_source: "process_album_dispatcher",
          p_message: `Error processing artist ${item.item_id}`,
          p_stack_trace: error.stack || "",
          p_context: { item },
          p_item_id: item.item_id,
          p_item_type: item.item_type
        });
      }
    }
    
    // Update batch with progress
    await supabase
      .from("processing_batches")
      .update({
        updated_at: new Date().toISOString(),
        metadata: {
          ...batch.metadata,
          album_page_batches_created: totalPages,
          artists_processed: totalArtists,
          updated_at: new Date().toISOString(),
          correlation_id: correlationId
        }
      })
      .eq("id", batchId);
    
    // Release the batch to allow other artists to be processed
    await supabase.rpc(
      "release_processing_batch",
      {
        p_batch_id: batchId,
        p_worker_id: workerId,
        p_status: "processing" // Keep as processing if there are more artists to process
      }
    );
    
    await incrementMetric('album_page_batches_created', totalPages, {
      artist_count: totalArtists,
      batch_id: batchId
    });
    
    return {
      success: true,
      message: `Created ${totalPages} album page batches for ${totalArtists} artists`,
      batchId,
      pages: totalPages,
      artistId: items[0]?.item_id,
      status: "processing"
    };
    
  } catch (error) {
    console.error("Error in processAlbumsDispatcher:", error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_album_dispatcher",
      p_message: `Error in album dispatcher`,
      p_stack_trace: error.stack || "",
      p_context: { error: error.message }
    });
    
    return {
      success: false,
      message: `Error in album dispatcher: ${error.message}`,
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
    const result = await processAlbumsDispatcher();
    
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
    console.error("Error handling process-album-dispatcher request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_album_dispatcher",
        p_message: "Error handling process-album-dispatcher request",
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
