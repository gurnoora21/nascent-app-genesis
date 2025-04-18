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

// Manually format Spotify release dates since we can't use the utility function
function formatReleaseDate(releaseDate: string | null): string | null {
  if (!releaseDate) return null;
  
  const dateParts = releaseDate.split('-');
  
  if (dateParts.length === 1 && dateParts[0].length === 4) {
    // Year only format (e.g., "2012") - append "-01-01" to make it January 1st
    return `${releaseDate}-01-01`;
  } else if (dateParts.length === 2) {
    // Year-month format (e.g., "2012-03") - append "-01" to make it the 1st day of the month
    return `${releaseDate}-01`;
  } else if (dateParts.length === 3) {
    // Already full date format, return as is
    return releaseDate;
  } else {
    // Unknown format, log it and return null
    console.error(`Unknown release date format: ${releaseDate}`);
    return null;
  }
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

// Process a pending tracks batch
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
    
    console.log(`Worker ${workerId} claiming a tracks batch`);
    
    // Pre-flight check: Check if API is rate limited
    const apiLimited = await isApiRateLimited("spotify", "/tracks");
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
        p_batch_type: "process_tracks",
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
        message: "No pending tracks batches found to process",
        status: "idle"
      };
    }
    
    console.log(`Worker ${workerId} claimed tracks batch ${batchId}`);
    
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
    const batchSize = 5; // Smaller batch size for album track processing
    
    // Get items to process - these are albums we need to extract tracks from
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
      await createProducersBatch(batchId);
      
      return {
        success: true,
        message: "Tracks batch claimed but no items to process",
        batchId,
        processed: 0,
        failed: 0,
        status: "completed"
      };
    }
    
    console.log(`Processing ${items.length} album-track items in batch ${batchId}`);
    
    const processedItems: string[] = [];
    const failedItems: string[] = [];
    
    // Process tracks from albums
    const trackIds = await processAlbumTracks(items, processedItems, failedItems);
    
    console.log(`Tracks batch ${batchId} processing complete. Processed: ${processedItems.length}, Failed: ${failedItems.length}, Tracks found: ${trackIds.length}`);
    
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
      await createProducersBatch(batchId, trackIds);
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
      message: `Processed ${processedItems.length} album-track items, found ${trackIds.length} tracks, failed ${failedItems.length} items, ${pendingItems || 'unknown'} items remaining`,
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
      p_context: { batchType: "process_tracks" }
    });
    
    return {
      success: false,
      message: `Error processing tracks batch: ${error.message}`,
      status: "error"
    };
  }
}

// Process tracks from albums and store them
async function processAlbumTracks(
  items: any[], 
  processedItems: string[], 
  failedItems: string[]
): Promise<string[]> {
  const allTrackIds: string[] = [];
  
  // Process each album to extract and store its tracks
  for (const item of items) {
    try {
      if (item.item_type !== "album_for_tracks") {
        throw new Error(`Expected item_type "album_for_tracks", got "${item.item_type}"`);
      }
      
      const albumId = item.item_id;
      console.log(`Processing tracks for album ${albumId}`);
      
      // Get the album details
      const { data: album, error: albumError } = await supabase
        .from("albums")
        .select("*")
        .eq("id", albumId)
        .single();
      
      if (albumError || !album) {
        throw new Error(`Album not found: ${albumError?.message || "No data returned"}`);
      }
      
      // Fetch the album tracks
      const response = await fetch(
        `https://api.spotify.com/v1/albums/${album.spotify_id}/tracks?limit=50`,
        {
          headers: {
            "Authorization": `Bearer ${await getSpotifyAccessToken()}`
          }
        }
      );
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP error ${response.status}: ${errorText}`);
      }
      
      const trackData = await response.json();
      
      if (!trackData || !trackData.items || !Array.isArray(trackData.items)) {
        throw new Error(`Invalid track data returned from Spotify API`);
      }
      
      console.log(`Found ${trackData.items.length} tracks for album ${albumId}`);
      
      // Store each track and link to album
      for (const track of trackData.items) {
        try {
          // Check if track already exists
          const { data: existingTrack, error: existsError } = await supabase
            .from("tracks")
            .select("id")
            .eq("spotify_id", track.id)
            .maybeSingle();
          
          let trackId;
          
          if (existingTrack) {
            trackId = existingTrack.id;
            
            // Update track data
            await supabase
              .from("tracks")
              .update({
                name: track.name,
                popularity: track.popularity || 0,
                spotify_url: track.external_urls?.spotify,
                preview_url: track.preview_url,
                updated_at: new Date().toISOString(),
                artist_id: album.artist_id,
                release_date: formatReleaseDate(album.release_date),
                metadata: {
                  spotify: track,
                  source_album_id: albumId
                }
              })
              .eq("id", trackId);
          } else {
            // Create new track
            const { data: newTrack, error: createError } = await supabase
              .from("tracks")
              .insert({
                name: track.name,
                spotify_id: track.id,
                popularity: track.popularity || 0,
                spotify_url: track.external_urls?.spotify,
                preview_url: track.preview_url,
                artist_id: album.artist_id,
                release_date: formatReleaseDate(album.release_date),
                metadata: {
                  spotify: track,
                  source_album_id: albumId
                }
              })
              .select("id")
              .single();
            
            if (createError) {
              console.error(`Error creating track ${track.name}:`, createError);
              continue;
            }
            
            trackId = newTrack.id;
          }
          
          // Link track to album
          await supabase
            .from("track_albums")
            .upsert({
              track_id: trackId,
              album_id: albumId,
              track_number: track.track_number,
              disc_number: track.disc_number
            }, {
              onConflict: "track_id,album_id"
            });
          
          // Link track to artist
          await supabase
            .from("track_artists")
            .upsert({
              track_id: trackId,
              artist_id: album.artist_id,
              is_primary: true
            }, {
              onConflict: "track_id,artist_id"
            });
          
          // Add track ID to our list for next batch
          allTrackIds.push(trackId);
          
        } catch (trackError) {
          console.error(`Error processing track ${track.name} from album ${albumId}:`, trackError);
          // Continue processing other tracks even if one fails
        }
      }
      
      // Mark item as processed
      await supabase
        .from("processing_items")
        .update({
          status: "completed",
          metadata: {
            ...item.metadata,
            tracks_processed: trackData.items.length,
            processed_at: new Date().toISOString()
          }
        })
        .eq("id", item.id);
      
      processedItems.push(item.id);
      
    } catch (error) {
      console.error(`Error processing album tracks for ${item.item_id}:`, error);
      
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
        p_source: "process_tracks_batch",
        p_message: `Error processing album tracks for ${item.item_id}`,
        p_stack_trace: error.stack || "",
        p_context: { item },
        p_item_id: item.item_id,
        p_item_type: item.item_type
      });
    }
  }
  
  return allTrackIds;
}

// Get Spotify access token
async function getSpotifyAccessToken(): Promise<string> {
  const clientId = Deno.env.get("SPOTIFY_CLIENT_ID");
  const clientSecret = Deno.env.get("SPOTIFY_CLIENT_SECRET");
  
  if (!clientId || !clientSecret) {
    throw new Error("Spotify credentials not configured");
  }
  
  const authString = `${clientId}:${clientSecret}`;
  const base64Auth = btoa(authString);
  
  const response = await fetch("https://accounts.spotify.com/api/token", {
    method: "POST",
    headers: {
      "Authorization": `Basic ${base64Auth}`,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: "grant_type=client_credentials"
  });
  
  if (!response.ok) {
    throw new Error(`Failed to get Spotify access token: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  return data.access_token;
}

// Create the next batch in the pipeline (producers batch)
async function createProducersBatch(tracksBatchId: string, tracksFromCurrentBatch: string[] = []): Promise<string | null> {
  try {
    // Create a new batch for processing producers
    const { data: newBatch, error: createError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "process_producers",
        status: "pending",
        metadata: {
          parent_batch_id: tracksBatchId,
          parent_batch_type: "process_tracks",
          processing_pipeline: true
        }
      })
      .select("id")
      .single();
    
    if (createError) {
      console.error(`Error creating producers batch:`, createError);
      return null;
    }
    
    console.log(`Created producers batch with ID ${newBatch.id}`);
    
    // Get tracks to process - first from current batch if provided
    let tracksToProcess: string[] = [...tracksFromCurrentBatch];
    
    // If we don't have enough tracks from current batch, look for more tracks that need producer identification
    if (tracksToProcess.length < 50) {
      const { data: tracks, error: tracksError } = await supabase
        .from("tracks")
        .select("id")
        .not("id", "in", supabase.from("track_producers").select("track_id"))
        .limit(50 - tracksToProcess.length);
      
      if (!tracksError && tracks && tracks.length > 0) {
        tracksToProcess = [...tracksToProcess, ...tracks.map(t => t.id)];
      }
    }
    
    if (tracksToProcess.length === 0) {
      console.log(`No tracks found for producer identification`);
      return newBatch.id;
    }
    
    // Create processing items for each track
    const producerItems = tracksToProcess.map(trackId => ({
      batch_id: newBatch.id,
      item_type: "track",
      item_id: trackId,
      status: "pending",
      priority: 10, // High priority
      metadata: {
        source_batch_id: tracksBatchId,
        pipeline_stage: "process_producers"
      }
    }));
    
    if (producerItems.length > 0) {
      const { error: insertError } = await supabase
        .from("processing_items")
        .insert(producerItems);
      
      if (insertError) {
        console.error(`Error creating producer identification items:`, insertError);
      } else {
        console.log(`Added ${producerItems.length} tracks to producer identification batch ${newBatch.id}`);
        
        // Update the batch with the accurate total number of items
        await supabase
          .from("processing_batches")
          .update({
            items_total: producerItems.length
          })
          .eq("id", newBatch.id);
      }
    }
    
    return newBatch.id;
  } catch (error) {
    console.error(`Error creating producers batch:`, error);
    return null;
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
      
      if (payload.action === "process") {
        const result = await processTracksBatch();
        
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
    const result = await processTracksBatch();
    
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
