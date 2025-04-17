
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

    let artistRecord;
    
    if (checkError) {
      console.error(`Error checking for existing artist ${spotifyId}:`, checkError);
      if (checkError.code === "PGRST116") { // Not found
        // Get artist details from Spotify
        const artistData = await spotify.getArtist(spotifyId);
        
        // Insert the artist into our database
        const { data: insertedArtist, error: insertError } = await supabase
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
        
        artistRecord = insertedArtist;
        console.log(`Inserted new artist: ${artistData.name}`);
      } else {
        throw new Error(`Failed to check for existing artist: ${checkError.message}`);
      }
    } else {
      artistRecord = existingArtist;
      console.log(`Artist ${existingArtist.name} already exists in the database`);
    }

    // Cache for artists we encounter to avoid duplicate lookups
    const artistCache = new Map<string, string>();
    artistCache.set(spotifyId, artistRecord.id);

    // Get all artist albums from Spotify using chunking approach and ALL include_groups
    let allAlbums = [];
    let offset = 0;
    const limit = 20; // Smaller chunks for better stability
    let hasMore = true;
    let totalAlbums = 0;

    console.log(`Fetching albums for artist ${spotifyId} in chunks of ${limit} (including appears_on and compilations)`);
    
    while (hasMore) {
      try {
        const albumsResponse = await spotify.getArtistAlbums(
          spotifyId, 
          limit, 
          offset, 
          "album,single,compilation,appears_on" // Include ALL album groups
        );
        
        if (!albumsResponse?.items || albumsResponse.items.length === 0) {
          hasMore = false;
          break;
        }
        
        // Update total count if it's the first batch
        if (offset === 0 && albumsResponse.total) {
          totalAlbums = albumsResponse.total;
          console.log(`Artist has ${totalAlbums} total albums to process`);
        }
        
        allAlbums.push(...albumsResponse.items);
        
        offset += limit;
        console.log(`Fetched ${allAlbums.length}/${totalAlbums} albums so far`);
        
        // Check if there are more albums to fetch
        hasMore = albumsResponse.items.length === limit && albumsResponse.next;
      } catch (albumFetchError) {
        console.error(`Error fetching albums at offset ${offset}:`, albumFetchError);
        // Log the error but continue with the albums we already have
        await supabase.rpc("log_error", {
          p_error_type: "processing",
          p_source: "process_artist",
          p_message: `Error fetching albums batch`,
          p_stack_trace: albumFetchError.stack || "",
          p_context: { spotifyId, offset, limit },
          p_item_id: spotifyId,
          p_item_type: "artist"
        });
        
        // Break the loop but continue processing with what we have
        break;
      }
    }

    console.log(`Found ${allAlbums.length} albums for artist`);
    
    // Track deduplication map using Spotify IDs
    const processedTrackIds = new Set<string>();
    const processedTracks = [];
    
    // Process albums in smaller chunks to avoid timeouts
    const albumChunkSize = 5;
    for (let i = 0; i < allAlbums.length; i += albumChunkSize) {
      const albumChunk = allAlbums.slice(i, i + albumChunkSize);
      console.log(`Processing album chunk ${i/albumChunkSize + 1}/${Math.ceil(allAlbums.length/albumChunkSize)}`);
      
      // Process each album in the chunk
      const albumPromises = albumChunk.map(async (album) => {
        try {
          // Get album tracks
          let trackOffset = 0;
          const trackLimit = 20; // Smaller chunks
          let hasMoreTracks = true;
          let albumTracks = [];
          
          while (hasMoreTracks) {
            try {
              const tracksResponse = await spotify.getAlbumTracks(album.id, trackLimit, trackOffset);
              
              if (!tracksResponse?.items || tracksResponse.items.length === 0) {
                hasMoreTracks = false;
                break;
              }
              
              albumTracks.push(...tracksResponse.items);
              
              trackOffset += trackLimit;
              hasMoreTracks = tracksResponse.items.length === trackLimit && tracksResponse.next;
            } catch (tracksError) {
              console.error(`Error fetching tracks for album ${album.id}:`, tracksError);
              await supabase.rpc("log_error", {
                p_error_type: "processing",
                p_source: "process_artist",
                p_message: `Error fetching album tracks`,
                p_stack_trace: tracksError.stack || "",
                p_context: { albumId: album.id, offset: trackOffset },
                p_item_id: spotifyId,
                p_item_type: "artist"
              });
              
              // Break but continue with next album
              break;
            }
          }
          
          console.log(`Album "${album.name}" has ${albumTracks.length} tracks`);
          
          // Filter out tracks we've already processed
          const newTracks = albumTracks.filter(track => !processedTrackIds.has(track.id));
          
          if (newTracks.length === 0) {
            console.log(`No new tracks to process in album "${album.name}"`);
            return;
          }
          
          // Mark these tracks as processed
          newTracks.forEach(track => processedTrackIds.add(track.id));
          
          // Process tracks in smaller batches
          const trackChunkSize = 50;
          for (let j = 0; j < newTracks.length; j += trackChunkSize) {
            const trackChunk = newTracks.slice(j, j + trackChunkSize);
            
            // Create the track records for insertion
            const trackRecords = trackChunk.map(track => ({
              name: track.name,
              spotify_id: track.id,
              artist_id: artistRecord.id, // Primary artist is the one we're processing
              album_name: album.name,
              release_date: album.release_date,
              spotify_url: track.external_urls?.spotify,
              preview_url: track.preview_url,
              metadata: {
                disc_number: track.disc_number,
                track_number: track.track_number,
                duration_ms: track.duration_ms,
                explicit: track.explicit,
                external_urls: track.external_urls,
                album: {
                  id: album.id,
                  name: album.name,
                  type: album.album_type,
                  total_tracks: album.total_tracks,
                  release_date: album.release_date,
                  release_date_precision: album.release_date_precision
                }
              }
            }));
            
            // Insert tracks with ON CONFLICT DO UPDATE
            const { data: insertedTracks, error: insertTracksError } = await supabase
              .from("tracks")
              .upsert(trackRecords, {
                onConflict: 'spotify_id',
                returning: 'id,spotify_id,name'
              });

            if (insertTracksError) {
              console.error(`Error upserting tracks:`, insertTracksError);
              continue;
            }
            
            console.log(`Upserted ${insertedTracks.length} tracks from album "${album.name}"`);
            processedTracks.push(...insertedTracks);
            
            // Now process all artist relationships for these tracks
            for (let k = 0; k < trackChunk.length; k++) {
              const track = trackChunk[k];
              const insertedTrack = insertedTracks[k];
              
              if (!insertedTrack) continue;
              
              // Process each artist on the track
              for (const [index, trackArtist] of track.artists.entries()) {
                let dbArtistId;
                
                // Check if we've already seen this artist
                if (artistCache.has(trackArtist.id)) {
                  dbArtistId = artistCache.get(trackArtist.id);
                } else {
                  // Try to get the artist from the database
                  const { data: existingTrackArtist, error: artistError } = await supabase
                    .from("artists")
                    .select("id")
                    .eq("spotify_id", trackArtist.id)
                    .single();
                  
                  if (artistError && artistError.code === "PGRST116") {
                    // Artist doesn't exist, so insert it with basic information
                    const { data: newArtist, error: insertArtistError } = await supabase
                      .from("artists")
                      .insert({
                        name: trackArtist.name,
                        spotify_id: trackArtist.id,
                        spotify_url: trackArtist.external_urls?.spotify,
                        metadata: {
                          external_urls: trackArtist.external_urls,
                          needs_full_processing: true // Flag to fully process this artist later
                        }
                      })
                      .select("id")
                      .single();
                    
                    if (insertArtistError) {
                      console.error(`Error inserting artist ${trackArtist.id}:`, insertArtistError);
                      continue;
                    }
                    
                    dbArtistId = newArtist.id;
                  } else if (artistError) {
                    console.error(`Error checking for artist ${trackArtist.id}:`, artistError);
                    continue;
                  } else {
                    dbArtistId = existingTrackArtist.id;
                  }
                  
                  // Cache the artist ID
                  artistCache.set(trackArtist.id, dbArtistId);
                }
                
                // Create the track-artist relationship
                // Is primary if it's our main artist or it's the first artist listed
                const isPrimary = trackArtist.id === spotifyId || index === 0;
                const { error: relationshipError } = await supabase
                  .from("track_artists")
                  .upsert({
                    track_id: insertedTrack.id,
                    artist_id: dbArtistId,
                    is_primary: isPrimary
                  }, {
                    onConflict: 'track_id,artist_id'
                  });
                
                if (relationshipError) {
                  console.error(`Error creating track-artist relationship:`, relationshipError);
                } else {
                  console.log(`Created track-artist relationship for "${track.name}" and "${trackArtist.name}"`);
                }
              }
            }
          }
        } catch (albumError) {
          console.error(`Error processing album ${album.id}:`, albumError);
          await supabase.rpc("log_error", {
            p_error_type: "processing",
            p_source: "process_artist",
            p_message: `Error processing album`,
            p_stack_trace: albumError.stack || "",
            p_context: { albumId: album.id },
            p_item_id: spotifyId,
            p_item_type: "artist"
          });
          // Continue with next album
        }
      });
      
      // Wait for all albums in this chunk to be processed
      await Promise.all(albumPromises);
    }

    // Update the artist's last_processed_at timestamp
    await supabase
      .from("artists")
      .update({
        last_processed_at: new Date().toISOString()
      })
      .eq("spotify_id", spotifyId);

    console.log(`Processed ${processedTrackIds.size} unique tracks for artist (based on Spotify ID)`);

    return {
      success: true,
      message: `Successfully processed artist with ${processedTrackIds.size} unique tracks`,
      data: {
        tracksProcessed: processedTrackIds.size,
        tracksDetails: processedTracks.slice(0, 10) // Return a sample to avoid overloading the response
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

// Process all artists in a batch
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
    
    // Process artists in smaller chunks to avoid timeouts
    const artistChunkSize = 5;
    for (let i = 0; i < items.length; i += artistChunkSize) {
      const itemChunk = items.slice(i, i + artistChunkSize);
      console.log(`Processing artist chunk ${i/artistChunkSize + 1}/${Math.ceil(items.length/artistChunkSize)}`);
      
      // Process each artist in the chunk
      for (const item of itemChunk) {
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
      
      // Update batch progress after each chunk to reflect current status
      await updateBatchProgress(batchId);
    }
    
    // Final update of the batch with accurate progress
    await updateBatchProgress(batchId);
    
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

// Helper function to update batch progress
async function updateBatchProgress(batchId: string): Promise<void> {
  try {
    // Get accurate counts from the database for this batch
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
    
    // Update the batch with accurate progress
    await supabase
      .from("processing_batches")
      .update({
        items_total: totalItems || 0,
        items_processed: completedItems || 0,
        items_failed: errorItems || 0,
      })
      .eq("id", batchId);
  } catch (error) {
    console.error(`Error updating batch progress for ${batchId}:`, error);
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
