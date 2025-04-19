
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { supabase, SpotifyClient } from "../lib/api-clients.ts";
import { 
  logSystemEvent, 
  logProcessingError, 
  formatSpotifyReleaseDate,
  updateRateLimit,
  isApiRateLimited,
  sleep,
  checkCircuitBreaker,
  incrementMetric,
  generateCorrelationId
} from "../lib/pipeline-utils.ts";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface ProcessTrackPageRequest {
  batchId: string;
  albumId: string;
  albumName?: string;
  spotifyId: string;
  offset: number;
  limit: number;
  correlationId?: string;
  parentAlbumBatchId?: string;
}

async function processTrackPage(request: ProcessTrackPageRequest): Promise<{
  success: boolean;
  message: string;
  nextOffset?: number;
  totalTracks?: number;
  processedTracks?: number;
  isFinalPage?: boolean;
}> {
  const { 
    batchId, 
    albumId, 
    albumName = "Unknown Album", 
    spotifyId, 
    offset, 
    limit, 
    correlationId = generateCorrelationId(),
    parentAlbumBatchId
  } = request;
  
  // Create a worker ID for tracing
  const workerId = crypto.randomUUID();
  
  try {
    await logSystemEvent('info', 'process_track_page', 
      `Starting track page processing for album ${albumName} (offset: ${offset}, limit: ${limit})`,
      { 
        workerId,
        batchId, 
        albumId, 
        albumName, 
        spotifyId, 
        offset, 
        limit,
        correlationId,
        parentAlbumBatchId
      },
      correlationId
    );
    
    // Check if the Spotify API is rate limited
    const isRateLimited = await checkCircuitBreaker('spotify');
    if (isRateLimited) {
      await logSystemEvent('warning', 'process_track_page', 
        `Spotify API is rate limited, cannot process track page for ${albumName}`,
        { albumId, spotifyId, correlationId },
        correlationId
      );
      
      return {
        success: false,
        message: 'Spotify API is rate limited, try again later',
      };
    }
    
    // Mark batch as processing
    await supabase
      .from('processing_batches')
      .update({
        status: 'processing',
        claimed_by: workerId,
        started_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', batchId);
    
    // Get the album to find the artist
    const { data: album, error: albumError } = await supabase
      .from('albums')
      .select('artist_id, release_date')
      .eq('id', albumId)
      .single();
    
    if (albumError || !album) {
      await logSystemEvent('error', 'process_track_page', 
        `Error getting album ${albumId}: ${albumError?.message || 'Album not found'}`,
        { albumId, spotifyId, offset, limit, correlationId },
        correlationId
      );
      
      // Mark batch as errored
      await supabase
        .from('processing_batches')
        .update({
          status: 'error',
          error_message: `Error getting album: ${albumError?.message || 'Album not found'}`,
          updated_at: new Date().toISOString()
        })
        .eq('id', batchId);
      
      return {
        success: false,
        message: `Error getting album: ${albumError?.message || 'Album not found'}`
      };
    }
    
    const artistId = album.artist_id;
    
    // Create Spotify client
    const spotify = new SpotifyClient();
    
    // Fetch tracks for this page
    const response = await spotify.getAlbumTracks(
      spotifyId,
      {
        limit,
        offset,
        market: 'US'
      }
    );
    
    if (response.error) {
      await logSystemEvent('error', 'process_track_page', 
        `Error fetching tracks for album ${albumName}: ${response.error.message}`,
        { albumId, spotifyId, offset, limit, correlationId, response: response.error },
        correlationId
      );
      
      // Mark batch as errored
      await supabase
        .from('processing_batches')
        .update({
          status: 'error',
          error_message: `Error fetching tracks: ${response.error.message}`,
          updated_at: new Date().toISOString()
        })
        .eq('id', batchId);
      
      return {
        success: false,
        message: `Error fetching tracks: ${response.error.message}`
      };
    }
    
    const { data: tracksData } = response;
    
    if (!tracksData || !tracksData.items || tracksData.items.length === 0) {
      await logSystemEvent('info', 'process_track_page', 
        `No tracks found for album ${albumName} at offset ${offset}`,
        { albumId, spotifyId, offset, limit, correlationId },
        correlationId
      );
      
      // Mark batch as completed and final
      await supabase
        .from('processing_batches')
        .update({
          status: 'completed',
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          items_total: 0,
          items_processed: 0,
          metadata: {
            album_id: albumId,
            album_name: albumName,
            spotify_id: spotifyId,
            artist_id: artistId,
            offset,
            limit,
            is_final_page: true,
            total_tracks: tracksData.total || 0,
            correlation_id: correlationId,
            parent_album_batch_id: parentAlbumBatchId
          }
        })
        .eq('id', batchId);
      
      return {
        success: true,
        message: 'No tracks found for this album',
        totalTracks: tracksData.total || 0,
        processedTracks: 0,
        isFinalPage: true
      };
    }
    
    const tracks = tracksData.items;
    const total = tracksData.total || tracks.length;
    
    await logSystemEvent('info', 'process_track_page', 
      `Found ${tracks.length} tracks for album ${albumName} (offset: ${offset}, total: ${total})`,
      { albumId, spotifyId, trackCount: tracks.length, offset, limit, total, correlationId },
      correlationId
    );
    
    // Process each track in this page
    let processedCount = 0;
    for (const track of tracks) {
      try {
        // Check if track already exists
        const { data: existingTrack, error: checkError } = await supabase
          .from('tracks')
          .select('id')
          .eq('spotify_id', track.id)
          .maybeSingle();
        
        if (checkError && checkError.code !== 'PGRST116') {
          console.error(`Error checking for existing track ${track.id}:`, checkError);
          continue;
        }
        
        let trackId: string;
        if (existingTrack) {
          // Update the existing track
          const { error: updateError } = await supabase
            .from('tracks')
            .update({
              name: track.name,
              popularity: track.popularity,
              spotify_url: track.external_urls?.spotify,
              preview_url: track.preview_url,
              updated_at: new Date().toISOString(),
              metadata: {
                ...track,
                updated_at: new Date().toISOString(),
                correlation_id: correlationId
              }
            })
            .eq('id', existingTrack.id);
          
          if (updateError) {
            console.error(`Error updating track ${track.id}:`, updateError);
            continue;
          }
          
          trackId = existingTrack.id;
        } else {
          // Insert a new track
          const { data: newTrack, error: insertError } = await supabase
            .from('tracks')
            .insert({
              name: track.name,
              artist_id: artistId,
              spotify_id: track.id,
              spotify_url: track.external_urls?.spotify,
              preview_url: track.preview_url,
              popularity: track.popularity,
              release_date: album.release_date,
              metadata: {
                ...track,
                created_at: new Date().toISOString(),
                correlation_id: correlationId
              }
            })
            .select('id')
            .single();
          
          if (insertError) {
            console.error(`Error inserting track ${track.id}:`, insertError);
            continue;
          }
          
          trackId = newTrack.id;
        }
        
        // Now link the track to the album via track_albums
        const { data: existingLink, error: linkCheckError } = await supabase
          .from('track_albums')
          .select('id')
          .eq('track_id', trackId)
          .eq('album_id', albumId)
          .maybeSingle();
        
        if (linkCheckError && linkCheckError.code !== 'PGRST116') {
          console.error(`Error checking for existing album-track link:`, linkCheckError);
        } else if (!existingLink) {
          // Create the link if it doesn't exist
          const { error: linkError } = await supabase
            .from('track_albums')
            .insert({
              track_id: trackId,
              album_id: albumId,
              track_number: track.track_number,
              disc_number: track.disc_number
            });
          
          if (linkError) {
            console.error(`Error linking track to album:`, linkError);
          }
        }
        
        // Link the track to its artists via track_artists
        if (track.artists && track.artists.length > 0) {
          for (const artist of track.artists) {
            try {
              // First check if this artist exists in our database
              const { data: dbArtist, error: artistCheckError } = await supabase
                .from('artists')
                .select('id')
                .eq('spotify_id', artist.id)
                .maybeSingle();
              
              let artistDbId: string;
              if (artistCheckError && artistCheckError.code !== 'PGRST116') {
                console.error(`Error checking for artist ${artist.id}:`, artistCheckError);
                continue;
              }
              
              if (!dbArtist) {
                // Insert a minimal artist entry if not found
                const { data: newArtist, error: artistInsertError } = await supabase
                  .from('artists')
                  .insert({
                    name: artist.name,
                    spotify_id: artist.id,
                    spotify_url: artist.external_urls?.spotify,
                    metadata: {
                      ...artist,
                      created_at: new Date().toISOString(),
                      correlation_id: correlationId,
                      source: 'track_processing'
                    }
                  })
                  .select('id')
                  .single();
                
                if (artistInsertError) {
                  console.error(`Error inserting artist ${artist.id}:`, artistInsertError);
                  continue;
                }
                
                artistDbId = newArtist.id;
              } else {
                artistDbId = dbArtist.id;
              }
              
              // Check if the track-artist link already exists
              const { data: existingArtistLink, error: artistLinkCheckError } = await supabase
                .from('track_artists')
                .select('id')
                .eq('track_id', trackId)
                .eq('artist_id', artistDbId)
                .maybeSingle();
              
              if (artistLinkCheckError && artistLinkCheckError.code !== 'PGRST116') {
                console.error(`Error checking for track-artist link:`, artistLinkCheckError);
              } else if (!existingArtistLink) {
                // Create the link if it doesn't exist
                const { error: artistLinkError } = await supabase
                  .from('track_artists')
                  .insert({
                    track_id: trackId,
                    artist_id: artistDbId,
                    is_primary: artist.id === track.artists[0].id // First artist is primary
                  });
                
                if (artistLinkError) {
                  console.error(`Error linking track to artist:`, artistLinkError);
                }
              }
            } catch (artistError) {
              console.error(`Error processing artist ${artist.name} for track:`, artistError);
            }
          }
        }
        
        // Add track to processing batch items for tracking
        await supabase
          .from('processing_items')
          .insert({
            batch_id: batchId,
            item_type: 'track',
            item_id: trackId,
            status: 'completed',
            metadata: {
              track_name: track.name,
              spotify_id: track.id,
              album_id: albumId,
              correlation_id: correlationId
            }
          });
        
        processedCount++;
      } catch (error) {
        console.error(`Error processing track ${track.id}:`, error);
        await logProcessingError(
          'track_processing',
          'process_track_page',
          `Error processing track ${track.name}`,
          error,
          {
            albumId,
            spotifyId: track.id,
            trackName: track.name,
            correlationId
          }
        );
      }
    }
    
    // Determine if this is the final page
    const isFinalPage = offset + tracks.length >= total;
    const nextOffset = offset + limit;
    
    // Update batch status based on whether there are more pages
    if (isFinalPage) {
      // This is the last page
      await supabase
        .from('processing_batches')
        .update({
          status: 'completed',
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          items_total: tracks.length,
          items_processed: processedCount,
          metadata: {
            album_id: albumId,
            album_name: albumName,
            spotify_id: spotifyId,
            artist_id: artistId,
            offset,
            limit,
            is_final_page: true,
            total_tracks: total,
            processed_tracks: processedCount,
            correlation_id: correlationId,
            parent_album_batch_id: parentAlbumBatchId
          }
        })
        .eq('id', batchId);
      
      await logSystemEvent('info', 'process_track_page', 
        `Completed final track page for album ${albumName} (processed ${processedCount}/${tracks.length})`,
        { 
          albumId, 
          spotifyId,
          batchId,
          offset, 
          total,
          processedCount,
          correlationId,
          parentAlbumBatchId
        },
        correlationId
      );
      
      // Update the album's track count if needed
      await supabase
        .from('albums')
        .update({
          total_tracks: total,
          updated_at: new Date().toISOString()
        })
        .eq('id', albumId);
      
      // Notify monitor to check for completion of all track pages
      EdgeRuntime.waitUntil(
        supabase.functions.invoke('monitor-pipeline', {
          body: {
            specificBatchId: batchId,
            correlationId
          }
        })
      );
    } else {
      // This is not the last page, mark as completed_with_next and schedule the next page
      await supabase
        .from('processing_batches')
        .update({
          status: 'completed_with_next',
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          items_total: tracks.length,
          items_processed: processedCount,
          metadata: {
            album_id: albumId,
            album_name: albumName,
            spotify_id: spotifyId,
            artist_id: artistId,
            offset,
            limit,
            next_offset: nextOffset,
            is_final_page: false,
            total_tracks: total,
            processed_tracks: processedCount,
            correlation_id: correlationId,
            parent_album_batch_id: parentAlbumBatchId
          }
        })
        .eq('id', batchId);
      
      await logSystemEvent('info', 'process_track_page', 
        `Completed track page for album ${albumName}, scheduling next page at offset ${nextOffset}`,
        { 
          albumId, 
          spotifyId, 
          batchId,
          offset, 
          nextOffset, 
          total,
          processedCount,
          correlationId,
          parentAlbumBatchId
        },
        correlationId
      );
      
      // Schedule next page
      EdgeRuntime.waitUntil(async () => {
        try {
          // Create a new batch for the next page
          const { data: nextBatch, error: batchError } = await supabase
            .from('processing_batches')
            .insert({
              batch_type: 'process_track_page',
              status: 'pending',
              metadata: {
                album_id: albumId,
                album_name: albumName,
                spotify_id: spotifyId,
                artist_id: artistId,
                offset: nextOffset,
                limit,
                page_index: Math.floor(nextOffset / limit),
                parent_album_batch_id: parentAlbumBatchId,
                correlation_id: correlationId,
                created_at: new Date().toISOString()
              }
            })
            .select('id')
            .single();
          
          if (batchError || !nextBatch) {
            await logSystemEvent('error', 'process_track_page', 
              `Error creating next page batch: ${batchError?.message || 'Unknown error'}`,
              { 
                albumId, 
                spotifyId, 
                offset: nextOffset, 
                limit,
                correlationId,
                parentAlbumBatchId
              },
              correlationId
            );
            return;
          }
          
          // Give a small delay to avoid overwhelming the API or database
          await sleep(500);
          
          // Call process-track-page for the next batch
          await supabase.functions.invoke('process-track-page', {
            body: {
              batchId: nextBatch.id,
              albumId,
              albumName,
              spotifyId,
              offset: nextOffset,
              limit,
              correlationId,
              parentAlbumBatchId
            }
          });
        } catch (error) {
          await logSystemEvent('error', 'process_track_page', 
            `Error scheduling next page: ${error.message}`,
            { 
              albumId, 
              spotifyId, 
              offset: nextOffset, 
              limit,
              correlationId,
              parentAlbumBatchId,
              error: error.stack
            },
            correlationId
          );
        }
      });
    }
    
    // Track metrics
    await incrementMetric('tracks_processed', processedCount, {
      album_id: albumId,
      batch_id: batchId,
      is_final_page: isFinalPage
    });
    
    return {
      success: true,
      message: `Processed ${processedCount} tracks for album ${albumName}`,
      nextOffset: isFinalPage ? undefined : nextOffset,
      totalTracks: total,
      processedTracks: processedCount,
      isFinalPage
    };
  } catch (error) {
    await logSystemEvent('error', 'process_track_page', 
      `Error processing track page: ${error.message}`,
      { 
        batchId, 
        albumId, 
        spotifyId, 
        offset, 
        limit,
        correlationId,
        parentAlbumBatchId,
        error: error.stack
      },
      correlationId
    );
    
    // Mark batch as error
    await supabase
      .from('processing_batches')
      .update({
        status: 'error',
        error_message: error.message,
        updated_at: new Date().toISOString()
      })
      .eq('id', batchId);
    
    return {
      success: false,
      message: `Error processing track page: ${error.message}`
    };
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Parse request body
    const request: ProcessTrackPageRequest = await req.json();
    
    // Validate required parameters
    if (!request.batchId || !request.albumId || !request.spotifyId) {
      return new Response(
        JSON.stringify({
          success: false,
          message: 'Missing required parameters: batchId, albumId, and spotifyId are required'
        }),
        {
          status: 400,
          headers: {
            'Content-Type': 'application/json',
            ...corsHeaders
          }
        }
      );
    }
    
    // Default values for optional parameters
    request.offset = request.offset || 0;
    request.limit = request.limit || 50;
    request.correlationId = request.correlationId || generateCorrelationId();
    
    // Process the track page
    const result = await processTrackPage(request);
    
    return new Response(
      JSON.stringify(result),
      {
        status: result.success ? 200 : 500,
        headers: {
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      }
    );
  } catch (error) {
    console.error('Error processing request:', error);
    
    return new Response(
      JSON.stringify({
        success: false,
        message: `Error processing request: ${error.message}`
      }),
      {
        status: 500,
        headers: {
          'Content-Type': 'application/json',
          ...corsHeaders
        }
      }
    );
  }
});
