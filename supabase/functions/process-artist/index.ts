
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { SpotifyClient, supabase } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Process a specific artist by Spotify ID
async function processArtist(spotifyId: string): Promise<{success: boolean, message: string, data?: any}> {
  try {
    const spotify = new SpotifyClient();

    console.log(`Processing artist with Spotify ID: ${spotifyId}`);
    
    // First check if artist exists in our database
    const { data: existingArtist, error: checkError } = await supabase
      .from("artists")
      .select("*")
      .eq("spotify_id", spotifyId)
      .single();

    if (checkError) {
      console.error(`Error checking for existing artist ${spotifyId}:`, checkError);
      if (checkError.code === "PGRST116") { // Not found
        // Get artist details from Spotify
        const artistData = await spotify.getArtist(spotifyId);
        
        // Insert the artist into our database
        const { data: artistRecord, error: insertError } = await supabase
          .from("artists")
          .insert({
            name: artistData.name,
            spotify_id: artistData.id,
            genres: artistData.genres || [],
            popularity: artistData.popularity,
            spotify_url: artistData.external_urls?.spotify,
            image_url: artistData.images?.[0]?.url,
            metadata: {
              followers: artistData.followers,
              images: artistData.images,
              external_urls: artistData.external_urls,
            },
            last_processed_at: new Date().toISOString(),
          })
          .select("*")
          .single();

        if (insertError) {
          throw new Error(`Failed to insert artist: ${insertError.message}`);
        }
        
        console.log(`Inserted new artist: ${artistData.name}`);
      } else {
        throw new Error(`Failed to check for existing artist: ${checkError.message}`);
      }
    } else {
      console.log(`Artist ${existingArtist.name} already exists in the database`);
    }

    // Get all artist albums from Spotify
    const albums = [];
    let offset = 0;
    const limit = 50;
    let hasMore = true;

    while (hasMore) {
      const albumsResponse = await spotify.getArtistAlbums(spotifyId, limit, offset);
      
      if (!albumsResponse?.items || albumsResponse.items.length === 0) {
        hasMore = false;
        break;
      }
      
      albums.push(...albumsResponse.items);
      
      offset += limit;
      hasMore = albumsResponse.items.length === limit && albumsResponse.next;
    }

    console.log(`Found ${albums.length} albums for artist`);

    // For each album, get the tracks
    const processedTracks = [];
    
    for (const album of albums) {
      // Get album tracks
      let trackOffset = 0;
      const trackLimit = 50;
      let hasMoreTracks = true;
      
      while (hasMoreTracks) {
        const tracksResponse = await spotify.getAlbumTracks(album.id, trackLimit, trackOffset);
        
        if (!tracksResponse?.items || tracksResponse.items.length === 0) {
          hasMoreTracks = false;
          break;
        }
        
        // Process each track
        for (const track of tracksResponse.items) {
          try {
            // Get artist ID from the database
            const { data: artistRecord, error: artistError } = await supabase
              .from("artists")
              .select("id")
              .eq("spotify_id", spotifyId)
              .single();

            if (artistError) {
              console.error(`Error getting artist ID for ${spotifyId}:`, artistError);
              continue;
            }

            // Check if track already exists
            const { data: existingTrack, error: trackCheckError } = await supabase
              .from("tracks")
              .select("id")
              .eq("spotify_id", track.id)
              .single();

            if (trackCheckError && trackCheckError.code !== "PGRST116") {
              console.error(`Error checking for existing track ${track.id}:`, trackCheckError);
              continue;
            }

            if (!existingTrack) {
              // Insert track
              const { data: trackRecord, error: insertTrackError } = await supabase
                .from("tracks")
                .insert({
                  name: track.name,
                  spotify_id: track.id,
                  artist_id: artistRecord.id,
                  album_name: album.name,
                  release_date: album.release_date,
                  spotify_url: track.external_urls?.spotify,
                  preview_url: track.preview_url,
                  metadata: {
                    disc_number: track.disc_number,
                    duration_ms: track.duration_ms,
                    explicit: track.explicit,
                    external_urls: track.external_urls,
                    album: {
                      id: album.id,
                      name: album.name,
                      type: album.album_type,
                      total_tracks: album.total_tracks
                    }
                  }
                })
                .select("id")
                .single();

              if (insertTrackError) {
                console.error(`Error inserting track ${track.id}:`, insertTrackError);
                continue;
              }
              
              processedTracks.push({
                id: trackRecord.id,
                spotify_id: track.id,
                name: track.name
              });
            } else {
              processedTracks.push({
                id: existingTrack.id,
                spotify_id: track.id,
                name: track.name
              });
            }
          } catch (trackError) {
            console.error(`Error processing track ${track.id}:`, trackError);
            await supabase.rpc("log_error", {
              p_error_type: "processing",
              p_source: "process_artist",
              p_message: `Error processing track`,
              p_stack_trace: trackError.stack || "",
              p_context: { trackId: track.id, albumId: album.id },
              p_item_id: spotifyId,
              p_item_type: "artist"
            });
          }
        }
        
        trackOffset += trackLimit;
        hasMoreTracks = tracksResponse.items.length === trackLimit && tracksResponse.next;
      }
    }

    // Update the artist's last_processed_at timestamp
    await supabase
      .from("artists")
      .update({
        last_processed_at: new Date().toISOString()
      })
      .eq("spotify_id", spotifyId);

    console.log(`Processed ${processedTracks.length} tracks for artist`);

    return {
      success: true,
      message: `Successfully processed artist with ${processedTracks.length} tracks`,
      data: {
        tracksProcessed: processedTracks.length
      }
    };
  } catch (error) {
    console.error(`Error processing artist ${spotifyId}:`, error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_artist",
      p_message: `Error processing artist`,
      p_stack_trace: error.stack || "",
      p_context: { spotifyId },
      p_item_id: spotifyId,
      p_item_type: "artist"
    });
    
    return {
      success: false,
      message: `Error processing artist: ${error.message}`
    };
  }
}

// New function to process all artists in a batch
async function processBatch(batchId: string): Promise<{
  success: boolean, 
  message: string, 
  processed: number,
  failed: number,
  data?: any
}> {
  try {
    console.log(`Processing all artists in batch: ${batchId}`);
    
    // Get all pending artist items from the batch
    const { data: items, error: itemsError } = await supabase
      .from("processing_items")
      .select("*")
      .eq("batch_id", batchId)
      .eq("status", "pending")
      .eq("item_type", "artist");
    
    if (itemsError) {
      throw new Error(`Failed to get items from batch: ${itemsError.message}`);
    }
    
    if (!items || items.length === 0) {
      return {
        success: true,
        message: "No pending artist items found in the batch",
        processed: 0,
        failed: 0
      };
    }
    
    console.log(`Found ${items.length} pending artists to process in batch ${batchId}`);
    
    const processed = [];
    const failed = [];
    
    // Process each artist
    for (const item of items) {
      try {
        console.log(`Processing item ${item.id}: artist with Spotify ID ${item.item_id}`);
        
        // Process the artist
        const result = await processArtist(item.item_id);
        
        if (result.success) {
          // Mark item as processed with completed status
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
          
          processed.push(item.id);
        } else {
          // Mark as error
          await supabase
            .from("processing_items")
            .update({
              status: "error",
              retry_count: (item.retry_count || 0) + 1,
              last_error: result.message
            })
            .eq("id", item.id);
          
          failed.push(item.id);
        }
      } catch (itemError) {
        console.error(`Error processing item ${item.id}:`, itemError);
        
        // Mark as error
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
          p_source: "process_artist",
          p_message: `Error processing artist`,
          p_stack_trace: itemError.stack || "",
          p_context: item,
          p_item_id: item.item_id,
          p_item_type: item.item_type
        });
        
        failed.push(item.id);
      }
    }
    
    // Update the batch with accurate progress
    const { count: totalItems } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId);
    
    const { count: completedItems } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "completed");
    
    const { count: errorItems } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "error");
    
    await supabase
      .from("processing_batches")
      .update({
        items_total: totalItems || 0,
        items_processed: completedItems || 0,
        items_failed: errorItems || 0,
      })
      .eq("id", batchId);
    
    // Check if all items are processed
    const { count: pendingItems } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "pending");
    
    // If no pending items, mark batch as completed
    if (pendingItems === 0) {
      await supabase.rpc(
        "release_processing_batch",
        {
          p_batch_id: batchId,
          p_worker_id: crypto.randomUUID(), // Using a random UUID as worker ID
          p_status: "completed"
        }
      );
    }
    
    return {
      success: true,
      message: `Processed ${processed.length} artists, failed ${failed.length} artists`,
      processed: processed.length,
      failed: failed.length,
      data: {
        processedItems: processed,
        failedItems: failed
      }
    };
  } catch (error) {
    console.error(`Error processing batch ${batchId}:`, error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_artist",
      p_message: `Error processing batch`,
      p_stack_trace: error.stack || "",
      p_context: { batchId },
      p_item_id: batchId,
      p_item_type: "batch"
    });
    
    return {
      success: false,
      message: `Error processing batch: ${error.message}`,
      processed: 0,
      failed: 0
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
    const requestBody = await req.json();
    const { spotifyId, batchId } = requestBody;
    
    // Check if we're processing a batch or a single artist
    if (batchId) {
      // Process all artists in a batch
      const result = await processBatch(batchId);
      
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
    } else if (spotifyId) {
      // Process a single artist (maintaining backward compatibility)
      const result = await processArtist(spotifyId);
      
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
    } else {
      return new Response(
        JSON.stringify({
          success: false,
          error: "Either spotifyId or batchId is required",
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
  } catch (error) {
    console.error("Error handling process-artist request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_artist",
        p_message: "Error handling process-artist request",
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
