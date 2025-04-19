
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { 
  logSystemEvent, 
  updateRateLimit, 
  isApiRateLimited, 
  calculateBackoffMs,
  sleep,
  incrementMetric,
  updateDataQualityScore,
  formatSpotifyReleaseDate
} from "../lib/pipeline-utils.ts";
import { supabase, spotify } from "../lib/api-clients.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

interface AlbumPageRequest {
  batchId: string;
  artistId?: string;
  offset?: number;
  limit?: number;
  correlationId?: string;
  parentBatchId?: string;
  pageIndex?: number;
  totalPages?: number;
}

async function processAlbumPage(request: AlbumPageRequest): Promise<{
  success: boolean;
  message: string;
  batchId: string;
  processed?: number;
  failed?: number;
  status?: string;
  nextPageScheduled?: boolean;
}> {
  const { batchId, correlationId } = request;
  const processedAlbums: string[] = [];
  const failedAlbums: string[] = [];
  const startTime = Date.now();
  const traceId = correlationId || crypto.randomUUID();
  
  try {
    // Log start of processing
    await logSystemEvent('info', 'process_album_page', 
      `Starting album page processing for batch ${batchId}`, 
      { batchId, request, traceId },
      traceId
    );
    
    // Check API rate limits before proceeding
    const apiLimited = await isApiRateLimited('spotify', '/albums');
    if (apiLimited) {
      await logSystemEvent('warning', 'process_album_page', 
        `Spotify API is rate limited, delaying album page processing`, 
        { batchId, traceId },
        traceId
      );
      
      return {
        success: true,
        message: "API is currently rate limited, will retry later",
        batchId,
        status: "delayed"
      };
    }
    
    // Get batch details
    const { data: batch, error: batchError } = await supabase
      .from('processing_batches')
      .select('*')
      .eq('id', batchId)
      .single();
    
    if (batchError || !batch) {
      throw new Error(`Batch ${batchId} not found: ${batchError?.message || 'No data returned'}`);
    }
    
    // Mark batch as processing
    await supabase
      .from('processing_batches')
      .update({
        status: 'processing',
        started_at: batch.started_at || new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', batchId);
    
    // Extract pagination parameters
    const offset = request.offset || batch.metadata?.offset || 0;
    const limit = request.limit || batch.metadata?.limit || 20;
    const artistId = request.artistId || batch.metadata?.artist_id;
    const parentBatchId = request.parentBatchId || batch.metadata?.parent_batch_id;
    const pageIndex = request.pageIndex || batch.metadata?.page_index || 0;
    const totalPages = request.totalPages || batch.metadata?.total_pages || 1;
    
    // Get artist details
    const { data: artist, error: artistError } = await supabase
      .from('artists')
      .select('id, name, spotify_id')
      .eq('id', artistId)
      .single();
    
    if (artistError || !artist) {
      throw new Error(`Artist ${artistId} not found: ${artistError?.message || 'No data returned'}`);
    }
    
    if (!artist.spotify_id) {
      throw new Error(`Artist ${artist.name} does not have a Spotify ID`);
    }

    // Fetch albums from Spotify
    await logSystemEvent('info', 'process_album_page', 
      `Fetching albums for artist ${artist.name} (offset: ${offset}, limit: ${limit}, page ${pageIndex+1}/${totalPages})`, 
      { artistId, artistName: artist.name, offset, limit, pageIndex, totalPages, traceId },
      traceId
    );
    
    const { albums, error: spotifyError } = await getArtistAlbums(
      artist.spotify_id, 
      offset, 
      limit,
      traceId
    );
    
    if (spotifyError) {
      throw new Error(`Error fetching albums from Spotify: ${spotifyError.message}`);
    }
    
    let isFinal = false;
    
    if (!albums || albums.length === 0) {
      isFinal = true;
      
      const finalStatus = {
        status: 'completed',
        items_total: 0,
        items_processed: 0,
        items_failed: 0,
        completed_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        metadata: {
          ...batch.metadata,
          is_final_page: true,
          processing_time_ms: Date.now() - startTime,
          success_rate: 1,
          offset,
          limit,
          page_index: pageIndex,
          total_pages: totalPages
        }
      };

      await supabase
        .from('processing_batches')
        .update(finalStatus)
        .eq('id', batchId);

      return {
        success: true,
        message: `No albums found for artist ${artist.name} at offset ${offset}`,
        batchId,
        processed: 0,
        failed: 0,
        status: 'completed'
      };
    }
    
    // Process albums in smaller batches
    const batchSize = 5;
    const albumBatches = [];
    
    for (let i = 0; i < albums.length; i += batchSize) {
      albumBatches.push(albums.slice(i, i + batchSize));
    }
    
    // Track list creation
    let trackPageBatchIds: string[] = [];
    
    for (const albumBatch of albumBatches) {
      const processingPromises = albumBatch.map(album => 
        processAlbum(album, artist.id, traceId)
          .then(result => {
            if (result.success) {
              processedAlbums.push(album.id);
              if (result.trackBatchId) {
                trackPageBatchIds.push(result.trackBatchId);
              }
            } else {
              failedAlbums.push(album.id);
            }
            return result;
          })
      );
      
      await Promise.all(processingPromises);
      
      // Small delay between batches
      await sleep(200);
    }
    
    // Determine if this is the final page
    isFinal = albums.length < limit || (pageIndex === totalPages - 1);
    
    // Calculate success metrics
    const successRate = albums.length > 0 ? processedAlbums.length / albums.length : 0;
    
    // Update batch status with pagination metadata
    const batchStatus = {
      status: isFinal ? 'completed' : 'completed_with_next',
      metadata: {
        ...batch.metadata,
        offset,
        limit,
        page_index: pageIndex,
        total_pages: totalPages,
        is_final_page: isFinal,
        next_page_offset: isFinal ? null : offset + limit,
        processing_time_ms: Date.now() - startTime,
        success_rate: successRate,
        track_page_batch_ids: trackPageBatchIds
      },
      items_total: albums.length,
      items_processed: processedAlbums.length,
      items_failed: failedAlbums.length,
      completed_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    await supabase
      .from('processing_batches')
      .update(batchStatus)
      .eq('id', batchId);

    // Schedule next page processing if not final
    let nextPageScheduled = false;
    if (!isFinal) {
      const nextOffset = offset + limit;
      const nextPageIndex = pageIndex + 1;
      
      edgeRuntime.waitUntil(
        supabase.from('processing_batches')
          .insert({
            batch_type: 'process_album_page',
            status: 'pending',
            metadata: {
              artist_id: artistId,
              artist_name: artist.name,
              parent_batch_id: parentBatchId,
              spotify_id: artist.spotify_id,
              offset: nextOffset,
              limit,
              page_index: nextPageIndex,
              total_pages: totalPages,
              correlation_id: traceId,
              source: 'pagination'
            }
          })
          .then(async ({ data: nextBatch, error: nextPageError }) => {
            if (nextPageError) {
              await logSystemEvent('error', 'process_album_page',
                `Error scheduling next page: ${nextPageError.message}`,
                { artistId, offset: nextOffset, pageIndex: nextPageIndex, traceId },
                traceId
              );
            } else if (nextBatch) {
              await logSystemEvent('info', 'process_album_page',
                `Scheduled next page batch: ${nextBatch[0].id}`,
                { 
                  artistId, 
                  nextBatchId: nextBatch[0].id, 
                  offset: nextOffset, 
                  pageIndex: nextPageIndex, 
                  traceId 
                },
                traceId
              );
            }
          })
      );
      
      nextPageScheduled = true;
    }

    // Process the first track page for each album
    for (const trackBatchId of trackPageBatchIds.slice(0, 3)) { // Process first few batches to kickstart pipeline
      edgeRuntime.waitUntil(
        supabase.functions.invoke('process-track-page', {
          body: { batchId: trackBatchId, correlationId: traceId }
        }).catch(async (error) => {
          await logSystemEvent('error', 'process_album_page',
            `Error invoking track page processor: ${error.message}`,
            { trackBatchId, traceId },
            traceId
          );
        })
      );
    }

    return {
      success: true,
      message: `Processed ${processedAlbums.length} albums, failed ${failedAlbums.length} albums, created ${trackPageBatchIds.length} track batches${nextPageScheduled ? ', scheduled next page' : ''}`,
      batchId,
      processed: processedAlbums.length,
      failed: failedAlbums.length,
      status: batchStatus.status,
      nextPageScheduled
    };
    
  } catch (error) {
    // Log error
    console.error('Error in processAlbumPage:', error);
    
    await logSystemEvent('error', 'process_album_page', 
      `Error processing album page: ${error.message}`, 
      { batchId, stack: error.stack, traceId },
      traceId
    );
    
    // Update batch status
    await supabase
      .from('processing_batches')
      .update({
        status: 'error',
        error_message: error.message,
        updated_at: new Date().toISOString()
      })
      .eq('id', batchId);
    
    // Track error metrics
    await incrementMetric('album_page_errors', 1, {
      batch_id: batchId,
      error_type: error.name,
      error_message: error.message.substring(0, 100)  // Limit message length
    });
    
    return {
      success: false,
      message: `Error processing album page: ${error.message}`,
      batchId,
      processed: processedAlbums.length,
      failed: failedAlbums.length,
      status: 'error'
    };
  }
}

async function getArtistAlbums(
  spotifyId: string, 
  offset: number, 
  limit: number,
  traceId: string
): Promise<{ albums?: any[]; error?: Error }> {
  try {
    const response = await spotify.getArtistAlbums(
      spotifyId, 
      {
        offset,
        limit,
        include_groups: 'album,single,compilation',
        market: 'US'  // Prioritize US market for consistency
      }
    );
    
    // Update rate limits
    if (response.headers) {
      await updateRateLimit(
        'spotify',
        '/albums',
        response.headers,
        { status: response.status }
      );
    }
    
    // Check for errors
    if (!response.data || !response.data.items) {
      return { 
        error: new Error(`No albums data returned from Spotify. Status: ${response.status}`) 
      };
    }
    
    return { albums: response.data.items };
  } catch (error) {
    // Log the error
    await logSystemEvent('error', 'get_artist_albums', 
      `Error fetching albums from Spotify: ${error.message}`, 
      { spotifyId, offset, limit, stack: error.stack, traceId },
      traceId
    );
    
    return { error };
  }
}

async function processAlbum(
  spotifyAlbum: any, 
  artistId: string,
  traceId: string
): Promise<{ success: boolean; message: string; albumId?: string; trackBatchId?: string }> {
  try {
    // Prepare album data from Spotify response
    const albumData = {
      name: spotifyAlbum.name,
      spotify_id: spotifyAlbum.id,
      artist_id: artistId,
      spotify_url: spotifyAlbum.external_urls?.spotify || null,
      release_date: formatSpotifyReleaseDate(spotifyAlbum.release_date),
      album_type: spotifyAlbum.album_type,
      total_tracks: spotifyAlbum.total_tracks,
      popularity: spotifyAlbum.popularity,
      image_url: spotifyAlbum.images && spotifyAlbum.images.length > 0 ? 
                  spotifyAlbum.images[0].url : null,
      is_primary_artist_album: spotifyAlbum.artists && 
                              spotifyAlbum.artists.length > 0 && 
                              spotifyAlbum.artists[0].id === spotifyAlbum.artists[0].id,
      metadata: {
        processed_at: new Date().toISOString(),
        trace_id: traceId,
        spotify: spotifyAlbum
      }
    };
    
    // Upsert album
    const { data: album, error: albumError } = await supabase
      .from('albums')
      .upsert(albumData, { 
        onConflict: 'spotify_id',
        ignoreDuplicates: false  // Update if exists
      })
      .select('id')
      .single();
    
    if (albumError) {
      throw new Error(`Error upserting album: ${albumError.message}`);
    }
    
    if (!album) {
      throw new Error('No album data returned after upsert');
    }
    
    // Calculate data quality score
    const qualityScore = calculateAlbumQualityScore(spotifyAlbum);
    
    // Update data quality
    await updateDataQualityScore(
      'album',
      album.id,
      'spotify',
      qualityScore,
      undefined,
      undefined,
      {
        updated_at: new Date().toISOString(),
        trace_id: traceId
      }
    );

    // Create a track page batch to process tracks for this album
    const { data: trackBatch, error: trackBatchError } = await supabase
      .from('processing_batches')
      .insert({
        batch_type: 'process_track_page',
        status: 'pending',
        metadata: {
          album_id: album.id,
          album_name: spotifyAlbum.name,
          artist_id: artistId,
          spotify_id: spotifyAlbum.id,
          total_tracks: spotifyAlbum.total_tracks,
          offset: 0,
          limit: 50,  // Standard Spotify API pagination
          page_index: 0,
          total_pages: Math.ceil((spotifyAlbum.total_tracks || 0) / 50),
          correlation_id: traceId
        }
      })
      .select('id')
      .single();
    
    if (trackBatchError) {
      console.error(`Error creating track batch: ${trackBatchError.message}`);
      // Continue anyway, we've at least saved the album
    }
    
    // Track metrics
    await incrementMetric('albums_processed', 1, {
      album_type: spotifyAlbum.album_type,
      has_tracks: (spotifyAlbum.total_tracks || 0) > 0
    });
    
    return {
      success: true,
      message: `Processed album: ${spotifyAlbum.name}`,
      albumId: album.id,
      trackBatchId: trackBatch?.id
    };
  } catch (error) {
    // Log error
    await logSystemEvent('error', 'process_album', 
      `Error processing album ${spotifyAlbum.name}: ${error.message}`, 
      { albumName: spotifyAlbum.name, stack: error.stack, traceId },
      traceId
    );
    
    // Track error metrics
    await incrementMetric('album_processing_errors', 1, {
      error_type: error.name,
      album_name: spotifyAlbum.name.substring(0, 50)
    });
    
    return {
      success: false,
      message: `Error processing album: ${error.message}`
    };
  }
}

function calculateAlbumQualityScore(album: any): number {
  let score = 0.5; // Base score
  
  // Add points for various fields being present
  if (album.name) score += 0.1;
  if (album.release_date) score += 0.1;
  if (album.external_urls?.spotify) score += 0.05;
  if (album.images && album.images.length > 0) score += 0.1;
  if (album.total_tracks > 0) score += 0.05;
  if (album.artists && album.artists.length > 0) score += 0.1;
  
  // Cap at 1.0
  return Math.min(score, 1.0);
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  try {
    let request: AlbumPageRequest;
    
    if (req.method === 'POST') {
      // Parse request body
      try {
        request = await req.json();
      } catch (error) {
        return new Response(
          JSON.stringify({ 
            success: false, 
            message: `Invalid request body: ${error.message}` 
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
    } else {
      // Parse URL parameters
      const url = new URL(req.url);
      request = {
        batchId: url.searchParams.get('batchId') || '',
        artistId: url.searchParams.get('artistId') || undefined,
        offset: parseInt(url.searchParams.get('offset') || '0'),
        limit: parseInt(url.searchParams.get('limit') || '20'),
        correlationId: url.searchParams.get('correlationId') || undefined
      };
    }
    
    if (!request.batchId) {
      return new Response(
        JSON.stringify({ 
          success: false, 
          message: 'Missing required parameter: batchId' 
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
    
    const result = await processAlbumPage(request);
    
    return new Response(
      JSON.stringify(result),
      {
        status: result.success ? 200 : 500,
        headers: {
          'Content-Type': 'application/json',
          ...corsHeaders,
        },
      }
    );
  } catch (error) {
    console.error('Error handling process-album-page request:', error);
    
    // Log error to our database
    try {
      await supabase.rpc('log_error', {
        p_error_type: 'endpoint',
        p_source: 'process_album_page',
        p_message: 'Error handling process-album-page request',
        p_stack_trace: error.stack || '',
        p_context: { error: error.message },
      });
    } catch (logError) {
      console.error('Error logging to database:', logError);
    }

    return new Response(
      JSON.stringify({
        success: false,
        error: error.message,
      }),
      {
        status: 500,
        headers: {
          'Content-Type': 'application/json',
          ...corsHeaders,
        },
      }
    );
  }
});
