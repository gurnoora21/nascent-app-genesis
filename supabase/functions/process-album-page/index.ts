
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

interface ProcessAlbumPageRequest {
  batchId: string;
  artistId: string;
  artistName?: string;
  spotifyId: string;
  offset: number;
  limit: number;
  correlationId?: string;
}

async function processAlbumPage(request: ProcessAlbumPageRequest): Promise<{
  success: boolean;
  message: string;
  nextOffset?: number;
  totalAlbums?: number;
  processedAlbums?: number;
  isFinalPage?: boolean;
}> {
  const { 
    batchId, 
    artistId, 
    artistName = "Unknown Artist", 
    spotifyId, 
    offset, 
    limit, 
    correlationId = generateCorrelationId() 
  } = request;
  
  // Create a worker ID for tracing
  const workerId = crypto.randomUUID();
  
  try {
    await logSystemEvent('info', 'process_album_page', 
      `Starting album page processing for artist ${artistName} (offset: ${offset}, limit: ${limit})`,
      { 
        workerId,
        batchId, 
        artistId, 
        artistName, 
        spotifyId, 
        offset, 
        limit,
        correlationId
      },
      correlationId
    );
    
    // Check if the Spotify API is rate limited
    const isRateLimited = await checkCircuitBreaker('spotify');
    if (isRateLimited) {
      await logSystemEvent('warning', 'process_album_page', 
        `Spotify API is rate limited, cannot process album page for ${artistName}`,
        { artistId, spotifyId, correlationId },
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
    
    // Create Spotify client
    const spotify = new SpotifyClient();
    
    // Fetch albums for this page
    const response = await spotify.getArtistAlbums(
      spotifyId,
      {
        limit,
        offset,
        include_groups: 'album,single,compilation',
        market: 'US'
      }
    );
    
    if (response.error) {
      await logSystemEvent('error', 'process_album_page', 
        `Error fetching albums for artist ${artistName}: ${response.error.message}`,
        { artistId, spotifyId, offset, limit, correlationId, response: response.error },
        correlationId
      );
      
      // Mark batch as errored
      await supabase
        .from('processing_batches')
        .update({
          status: 'error',
          error_message: `Error fetching albums: ${response.error.message}`,
          updated_at: new Date().toISOString()
        })
        .eq('id', batchId);
      
      return {
        success: false,
        message: `Error fetching albums: ${response.error.message}`
      };
    }
    
    const { data: albumsData } = response;
    
    if (!albumsData || !albumsData.items || albumsData.items.length === 0) {
      await logSystemEvent('info', 'process_album_page', 
        `No albums found for artist ${artistName} at offset ${offset}`,
        { artistId, spotifyId, offset, limit, correlationId },
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
            artist_id: artistId,
            artist_name: artistName,
            spotify_id: spotifyId,
            offset,
            limit,
            is_final_page: true,
            total_albums: albumsData.total || 0,
            correlation_id: correlationId
          }
        })
        .eq('id', batchId);
      
      return {
        success: true,
        message: 'No albums found for this artist',
        totalAlbums: albumsData.total || 0,
        processedAlbums: 0,
        isFinalPage: true
      };
    }
    
    const albums = albumsData.items;
    const total = albumsData.total || albums.length;
    
    await logSystemEvent('info', 'process_album_page', 
      `Found ${albums.length} albums for artist ${artistName} (offset: ${offset}, total: ${total})`,
      { artistId, spotifyId, albumCount: albums.length, offset, limit, total, correlationId },
      correlationId
    );
    
    // Process each album in this page
    let processedCount = 0;
    for (const album of albums) {
      try {
        // Check if album already exists
        const { data: existingAlbum, error: checkError } = await supabase
          .from('albums')
          .select('id')
          .eq('spotify_id', album.id)
          .maybeSingle();
        
        if (checkError && checkError.code !== 'PGRST116') {
          console.error(`Error checking for existing album ${album.id}:`, checkError);
          continue;
        }
        
        // Format the release date
        const releaseDate = formatSpotifyReleaseDate(album.release_date);
        
        if (existingAlbum) {
          // Update the existing album
          const { error: updateError } = await supabase
            .from('albums')
            .update({
              name: album.name,
              album_type: album.album_type,
              release_date: releaseDate,
              total_tracks: album.total_tracks,
              popularity: album.popularity,
              image_url: album.images?.[0]?.url,
              spotify_url: album.external_urls?.spotify,
              updated_at: new Date().toISOString(),
              metadata: {
                ...album,
                updated_at: new Date().toISOString(),
                correlation_id: correlationId
              }
            })
            .eq('id', existingAlbum.id);
          
          if (updateError) {
            console.error(`Error updating album ${album.id}:`, updateError);
          } else {
            processedCount++;
          }
        } else {
          // Insert a new album
          const { data: newAlbum, error: insertError } = await supabase
            .from('albums')
            .insert({
              name: album.name,
              artist_id: artistId,
              album_type: album.album_type,
              release_date: releaseDate,
              total_tracks: album.total_tracks,
              popularity: album.popularity,
              spotify_id: album.id,
              image_url: album.images?.[0]?.url,
              spotify_url: album.external_urls?.spotify,
              is_primary_artist_album: album.artists?.[0]?.id === spotifyId,
              metadata: {
                ...album,
                created_at: new Date().toISOString(),
                correlation_id: correlationId
              }
            })
            .select('id')
            .single();
          
          if (insertError) {
            console.error(`Error inserting album ${album.id}:`, insertError);
          } else {
            processedCount++;
            
            // Add album to processing items for tracking
            await supabase
              .from('processing_items')
              .insert({
                batch_id: batchId,
                item_type: 'album',
                item_id: newAlbum.id,
                status: 'completed',
                metadata: {
                  album_name: album.name,
                  spotify_id: album.id,
                  correlation_id: correlationId
                }
              });
          }
        }
      } catch (error) {
        console.error(`Error processing album ${album.id}:`, error);
        await logProcessingError(
          'album_processing',
          'process_album_page',
          `Error processing album ${album.name}`,
          error,
          {
            artistId,
            spotifyId: album.id,
            albumName: album.name,
            correlationId
          }
        );
      }
      
      // Brief pause to avoid database contention
      await sleep(50);
    }
    
    // Determine if this is the final page
    const isFinalPage = offset + albums.length >= total;
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
          items_total: albums.length,
          items_processed: processedCount,
          metadata: {
            artist_id: artistId, 
            artist_name: artistName,
            spotify_id: spotifyId,
            offset,
            limit,
            is_final_page: true,
            total_albums: total,
            processed_albums: processedCount,
            correlation_id: correlationId
          }
        })
        .eq('id', batchId);
      
      await logSystemEvent('info', 'process_album_page', 
        `Completed final album page for artist ${artistName} (processed ${processedCount}/${albums.length})`,
        { 
          artistId, 
          spotifyId,
          batchId,
          offset, 
          total,
          processedCount,
          correlationId
        },
        correlationId
      );
      
      // Notify monitor to check for completion of all album pages
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
          items_total: albums.length,
          items_processed: processedCount,
          metadata: {
            artist_id: artistId,
            artist_name: artistName,
            spotify_id: spotifyId,
            offset,
            limit,
            next_offset: nextOffset,
            is_final_page: false,
            total_albums: total,
            processed_albums: processedCount,
            correlation_id: correlationId
          }
        })
        .eq('id', batchId);
      
      await logSystemEvent('info', 'process_album_page', 
        `Completed album page for artist ${artistName}, scheduling next page at offset ${nextOffset}`,
        { 
          artistId, 
          spotifyId, 
          batchId,
          offset, 
          nextOffset, 
          total,
          processedCount,
          correlationId
        },
        correlationId
      );
      
      // Schedule next page
      EdgeRuntime
      .waitUntil(async () => {
        try {
          // Create a new batch for the next page
          const { data: nextBatch, error: batchError } = await supabase
            .from('processing_batches')
            .insert({
              batch_type: 'process_album_page',
              status: 'pending',
              metadata: {
                artist_id: artistId,
                artist_name: artistName,
                spotify_id: spotifyId,
                offset: nextOffset,
                limit,
                page_index: Math.floor(nextOffset / limit),
                parent_batch_id: request.batchId,
                correlation_id: correlationId,
                created_at: new Date().toISOString()
              }
            })
            .select('id')
            .single();
          
          if (batchError || !nextBatch) {
            await logSystemEvent('error', 'process_album_page', 
              `Error creating next page batch: ${batchError?.message || 'Unknown error'}`,
              { 
                artistId, 
                spotifyId, 
                offset: nextOffset, 
                limit,
                correlationId
              },
              correlationId
            );
            return;
          }
          
          // Give a small delay to avoid overwhelming the API or database
          await sleep(500);
          
          // Call process-album-page for the next batch
          await supabase.functions.invoke('process-album-page', {
            body: {
              batchId: nextBatch.id,
              artistId,
              artistName,
              spotifyId,
              offset: nextOffset,
              limit,
              correlationId
            }
          });
        } catch (error) {
          await logSystemEvent('error', 'process_album_page', 
            `Error scheduling next page: ${error.message}`,
            { 
              artistId, 
              spotifyId, 
              offset: nextOffset, 
              limit,
              correlationId,
              error: error.stack
            },
            correlationId
          );
        }
      });
    }
    
    // Track metrics
    await incrementMetric('albums_processed', processedCount, {
      artist_id: artistId,
      batch_id: batchId,
      is_final_page: isFinalPage
    });
    
    return {
      success: true,
      message: `Processed ${processedCount} albums for artist ${artistName}`,
      nextOffset: isFinalPage ? undefined : nextOffset,
      totalAlbums: total,
      processedAlbums: processedCount,
      isFinalPage
    };
  } catch (error) {
    await logSystemEvent('error', 'process_album_page', 
      `Error processing album page: ${error.message}`,
      { 
        batchId, 
        artistId, 
        spotifyId, 
        offset, 
        limit,
        correlationId,
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
      message: `Error processing album page: ${error.message}`
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
    const request: ProcessAlbumPageRequest = await req.json();
    
    // Validate required parameters
    if (!request.batchId || !request.artistId || !request.spotifyId) {
      return new Response(
        JSON.stringify({
          success: false,
          message: 'Missing required parameters: batchId, artistId, and spotifyId are required'
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
    request.limit = request.limit || 20;
    request.correlationId = request.correlationId || generateCorrelationId();
    
    // Process the album page
    const result = await processAlbumPage(request);
    
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
