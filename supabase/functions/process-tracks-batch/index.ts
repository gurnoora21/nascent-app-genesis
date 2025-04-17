import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { SpotifyClient, supabase } from "../lib/api-clients.ts";

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
async function createTracksBatch(limit: number = 50, metadata?: Record<string, any>): Promise<string> {
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
      const batchMetadata = { 
        limit,
        created_by: "process-tracks-batch",
        created_at: new Date().toISOString(),
        ...metadata
      };
      
      const { data: newBatch, error: batchError } = await supabase
        .from("processing_batches")
        .insert({
          batch_type: "identify_producers",
          status: "pending",
          metadata: batchMetadata
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
    
    // First, get the IDs of tracks that are already in processing items
    const { data: existingItemsData, error: existingItemsError } = await supabase
      .from("processing_items")
      .select("item_id")
      .eq("item_type", "track")
      .in("status", ["pending", "processing"])
      .or(`retry_count.gte.5,status.eq.error`);
    
    if (existingItemsError) {
      console.error("Error checking existing processing items:", existingItemsError);
      throw existingItemsError;
    }
    
    const existingItemIds = existingItemsData?.map(item => item.item_id) || [];
    
    // Get the IDs of tracks that already have producers
    const { data: trackProducersData, error: trackProducersError } = await supabase
      .from("track_producers")
      .select("track_id");
    
    if (trackProducersError) {
      console.error("Error checking tracks with producers:", trackProducersError);
      throw trackProducersError;
    }
    
    const tracksWithProducers = trackProducersData?.map(item => item.track_id) || [];
    
    // Build the query with explicit filters
    let query = supabase
      .from("tracks")
      .select("id, name, artist_id, popularity")
      .order("popularity", { ascending: false });
    
    // Apply filtering for existing tracks and producers
    if (tracksWithProducers.length > 0) {
      query = query.not('id', 'in', tracksWithProducers);
    }
    
    if (existingItemIds.length > 0) {
      query = query.not('id', 'in', existingItemIds);
    }
    
    // Apply artist filter from metadata
    if (metadata?.artist_spotify_id) {
      // Priority 1: Filter by Spotify ID if provided
      const { data: artistData, error: artistError } = await supabase
        .from("artists")
        .select("id")
        .eq("spotify_id", metadata.artist_spotify_id)
        .single();
      
      if (!artistError && artistData) {
        query = query.eq('artist_id', artistData.id);
      } else {
        console.error("Artist not found with Spotify ID:", metadata.artist_spotify_id);
        throw new Error("Artist not found with provided Spotify ID");
      }
    } else if (metadata?.artist_name) {
      // Fallback: Filter by exact artist name match
      const { data: artistData, error: artistError } = await supabase
        .from("artists")
        .select("id")
        .eq("name", metadata.artist_name)  // Using exact match instead of ilike
        .limit(1);
      
      if (!artistError && artistData && artistData.length > 0) {
        query = query.eq('artist_id', artistData[0].id);
      } else {
        console.error("Artist not found with name:", metadata.artist_name);
        throw new Error("Artist not found with provided name");
      }
    }
    
    // Apply the limit
    query = query.limit(limit);
    
    // Execute the query
    const { data: tracks, error: tracksError } = await query;
    
    if (tracksError) {
      console.error("Error getting tracks:", tracksError);
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
  status?: string;
}> {
  try {
    const workerId = crypto.randomUUID();
    console.log(`Worker ${workerId} claiming a batch`);
    
    // Claim a process_tracks batch first
    const { data: batchId, error: claimError } = await supabase.rpc(
      "claim_processing_batch",
      {
        p_batch_type: "process_tracks",
        p_worker_id: workerId,
        p_claim_ttl_seconds: 3600
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

    console.log(`Processing batch ${batchId}`);
    
    // Get all albums from this batch
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", batchId)
      .single();
    
    if (batchError) {
      throw batchError;
    }

    const spotify = new SpotifyClient();
    let processedTracks = 0;
    
    // Get all albums for this artist
    const { data: albums, error: albumsError } = await supabase
      .from("albums")
      .select("*")
      .eq("artist_id", batch.metadata.artist_id);
    
    if (albumsError) {
      throw albumsError;
    }

    console.log(`Found ${albums?.length || 0} albums to process`);
    
    // Create a new batch for producer identification
    const { data: producerBatch, error: producerBatchError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "identify_producers",
        status: "pending",
        metadata: {
          artist_name: batch.metadata.artist_name,
          source_batch_id: batchId
        }
      })
      .select("id")
      .single();
    
    if (producerBatchError) {
      throw producerBatchError;
    }

    // Process each album
    for (const album of albums || []) {
      try {
        console.log(`Processing album: ${album.name}`);
        
        // Get tracks for this album from Spotify
        const tracks = await spotify.getAlbumTracks(album.spotify_id);
        
        if (!tracks?.items) {
          console.log(`No tracks found for album ${album.name}`);
          continue;
        }

        // Store each track
        for (const track of tracks.items) {
          try {
            // Store track in database
            const { data: newTrack, error: trackError } = await supabase
              .from("tracks")
              .upsert({
                spotify_id: track.id,
                name: track.name,
                artist_id: album.artist_id,
                spotify_url: track.external_urls?.spotify,
                preview_url: track.preview_url,
                popularity: track.popularity,
                metadata: {
                  spotify: track
                }
              })
              .select("id")
              .single();
            
            if (trackError) {
              console.error(`Error storing track ${track.name}:`, trackError);
              continue;
            }

            // Create track-album relationship
            await supabase
              .from("track_albums")
              .upsert({
                track_id: newTrack.id,
                album_id: album.id,
                track_number: track.track_number,
                disc_number: track.disc_number
              });

            // Add track to producer identification batch
            await supabase
              .from("processing_items")
              .insert({
                batch_id: producerBatch.id,
                item_type: "track",
                item_id: newTrack.id,
                priority: album.popularity || 5,
                status: "pending"
              });

            processedTracks++;
          } catch (trackError) {
            console.error(`Error processing track ${track.name}:`, trackError);
            continue;
          }
        }
      } catch (albumError) {
        console.error(`Error processing album ${album.name}:`, albumError);
        continue;
      }
    }

    // Mark the original batch as completed
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
      message: `Processed ${processedTracks} tracks across ${albums?.length || 0} albums`,
      batchId: producerBatch.id,
      processed: processedTracks,
      status: "completed"
    };
  } catch (error) {
    console.error("Error processing tracks batch:", error);
    
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
      const { action, limit = 50, metadata } = await req.json();
      
      if (action === "create") {
        // Create a new batch
        const batchId = await createTracksBatch(limit, metadata);
        
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
