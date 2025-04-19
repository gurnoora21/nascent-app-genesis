
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { 
  logSystemEvent, 
  updateRateLimit, 
  isApiRateLimited, 
  updateItemStatus,
  calculateBackoffMs,
  sleep,
  incrementMetric,
  updateDataQualityScore,
  formatSpotifyReleaseDate
} from "../lib/pipeline-utils.ts";
import { supabase, spotify } from "../lib/api-clients.ts";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

interface AlbumPageRequest {
  batchId: string;
  offset?: number;
  limit?: number;
  artistId?: string;
  correlationId?: string;
}

async function processAlbumPage(request: AlbumPageRequest): Promise<{
  success: boolean;
  message: string;
  batchId: string;
  processed?: number;
  failed?: number;
  status?: string;
}> {
  const { batchId, correlationId } = request;
  const processedAlbums: string[] = [];
  const failedAlbums: string[] = [];
  
  // Generate a correlation ID if one wasn't provided
  const traceId = correlationId || crypto.randomUUID();
  
  // Start tracking metrics
  const startTime = Date.now();
  
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
        started_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', batchId);
    
    // Extract pagination parameters from metadata or request
    const offset = request.offset || batch.metadata?.offset || 0;
    const limit = request.limit || batch.metadata?.limit || 20;
    const artistId = request.artistId || batch.metadata?.artist_id;
    const artistName = batch.metadata?.artist_name || 'Unknown';
    
    // Ensure we have an artist ID
    if (!artistId) {
      throw new Error('No artist ID provided in request or batch metadata');
    }
    
    // Get artist details
    const { data: artist, error: artistError } = await supabase
      .from('artists')
      .select('id, name, spotify_id')
      .eq('id', artistId)
      .single();
    
    if (artistError || !artist) {
      throw new Error(`Artist ${artistId} not found: ${artistError?.message || 'No data returned'}`);
    }
    
    // Check for existing albums
    await logSystemEvent('info', 'process_album_page', 
      `Fetching albums for artist ${artist.name} (offset: ${offset}, limit: ${limit})`, 
      { artistId, artistName: artist.name, offset, limit, traceId },
      traceId
    );
    
    // Get albums from Spotify
    const { albums, error: spotifyError } = await getArtistAlbums(
      artist.spotify_id, 
      offset, 
      limit,
      traceId
    );
    
    if (spotifyError) {
      throw new Error(`Error fetching albums from Spotify: ${spotifyError.message}`);
    }
    
    if (!albums || albums.length === 0) {
      await logSystemEvent('info', 'process_album_page', 
        `No albums found for artist ${artist.name} at offset ${offset}`, 
        { artistId, artistName: artist.name, offset, limit, traceId },
        traceId
      );
      
      // Update batch as completed since there's nothing to process
      await supabase
        .from('processing_batches')
        .update({
          status: 'completed',
          items_total: 0,
          items_processed: 0,
          items_failed: 0,
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        })
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
    
    // Process each album
    await logSystemEvent('info', 'process_album_page', 
      `Processing ${albums.length} albums for artist ${artist.name}`, 
      { artistId, artistName: artist.name, albumCount: albums.length, traceId },
      traceId
    );
    
    // Update batch with total item count
    await supabase
      .from('processing_batches')
      .update({
        items_total: albums.length
      })
      .eq('id', batchId);
    
    // Process albums in smaller batches to avoid DB connection issues
    const batchSize = 5;
    const albumBatches = [];
    
    for (let i = 0; i < albums.length; i += batchSize) {
      albumBatches.push(albums.slice(i, i + batchSize));
    }
    
    for (const albumBatch of albumBatches) {
      const processingPromises = albumBatch.map(album => 
        processAlbum(album, artist.id, traceId)
          .then(result => {
            if (result.success) {
              processedAlbums.push(album.id);
            } else {
              failedAlbums.push(album.id);
            }
            return result;
          })
      );
      
      await Promise.all(processingPromises);
      
      // Small delay between batches
      await sleep(100);
    }
    
    // Track metrics for the different album types
    const primaryAlbums = albums.filter(a => 
      a.album_type === 'album' || a.album_type === 'single');
    const secondaryAlbums = albums.filter(a => 
      a.album_type === 'compilation' || a.album_type === 'appears_on');
    
    await incrementMetric('primary_albums_processed', primaryAlbums.length, {
      artist_id: artist.id,
      artist_name: artist.name.substring(0, 50)  // Limit name length
    });
    
    await incrementMetric('secondary_albums_processed', secondaryAlbums.length, {
      artist_id: artist.id,
      artist_name: artist.name.substring(0, 50)
    });
    
    // Calculate processing time and success rate
    const processingTimeMs = Date.now() - startTime;
    const successRate = processedAlbums.length / albums.length;
    
    // Update batch status
    await supabase
      .from('processing_batches')
      .update({
        status: 'completed',
        items_total: albums.length,
        items_processed: processedAlbums.length,
        items_failed: failedAlbums.length,
        completed_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        metadata: {
          ...batch.metadata,
          processing_time_ms: processingTimeMs,
          success_rate: successRate,
          completed_at: new Date().toISOString()
        }
      })
      .eq('id', batchId);
    
    await logSystemEvent('info', 'process_album_page', 
      `Completed album page processing for batch ${batchId}. Processed: ${processedAlbums.length}, Failed: ${failedAlbums.length}`, 
      { 
        batchId, 
        processed: processedAlbums.length, 
        failed: failedAlbums.length,
        processingTimeMs,
        successRate,
        traceId 
      },
      traceId
    );
    
    return {
      success: true,
      message: `Processed ${processedAlbums.length} albums, failed ${failedAlbums.length} albums`,
      batchId,
      processed: processedAlbums.length,
      failed: failedAlbums.length,
      status: 'completed'
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

// Get albums for an artist from Spotify
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
        include_groups: 'album,single,appears_on,compilation'
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

// Process a single album
async function processAlbum(
  spotifyAlbum: any, 
  artistId: string,
  traceId: string
): Promise<{ success: boolean; message: string; albumId?: string }> {
  try {
    // Format release date
    const releaseDate = formatSpotifyReleaseDate(spotifyAlbum.release_date);
    
    // Determine if this is a primary artist album
    const isPrimaryArtistAlbum = spotifyAlbum.album_type === 'album' || 
                                 spotifyAlbum.album_type === 'single';
    
    // Prepare album data
    const albumData = {
      name: spotifyAlbum.name,
      spotify_id: spotifyAlbum.id,
      artist_id: artistId,
      release_date: releaseDate,
      album_type: spotifyAlbum.album_type,
      total_tracks: spotifyAlbum.total_tracks,
      popularity: spotifyAlbum.popularity,
      image_url: spotifyAlbum.images?.[0]?.url || null,
      spotify_url: spotifyAlbum.external_urls?.spotify || null,
      is_primary_artist_album: isPrimaryArtistAlbum,
      metadata: {
        processed_at: new Date().toISOString(),
        trace_id: traceId,
        uri: spotifyAlbum.uri,
        markets: spotifyAlbum.available_markets,
        album_group: spotifyAlbum.album_group
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
    
    // Track metrics
    await incrementMetric('albums_processed', 1, {
      album_type: spotifyAlbum.album_type,
      is_primary: isPrimaryArtistAlbum
    });
    
    return {
      success: true,
      message: `Processed album: ${spotifyAlbum.name}`,
      albumId: album.id
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

// Calculate album data quality score based on completeness
function calculateAlbumQualityScore(album: any): number {
  let score = 0.5; // Base score
  
  // Add points for various fields being present
  if (album.name) score += 0.1;
  if (album.release_date) score += 0.1;
  if (album.total_tracks > 0) score += 0.05;
  if (album.popularity !== undefined) score += 0.05;
  if (album.images && album.images.length > 0) score += 0.1;
  if (album.external_urls?.spotify) score += 0.05;
  if (album.available_markets && album.available_markets.length > 0) score += 0.05;
  
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
        offset: parseInt(url.searchParams.get('offset') || '0'),
        limit: parseInt(url.searchParams.get('limit') || '20'),
        artistId: url.searchParams.get('artistId') || undefined,
        correlationId: url.searchParams.get('correlationId') || undefined
      };
    }
    
    // Validate required parameters
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
          ...corsHeaders 
        } 
      }
    );
  } catch (error) {
    console.error('Error in process-album-page function:', error);
    
    return new Response(
      JSON.stringify({ 
        success: false, 
        message: `Unhandled error: ${error.message}`
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
