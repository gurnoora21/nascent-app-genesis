
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

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

interface TrackPageRequest {
  batchId: string;
  offset?: number;
  limit?: number;
  albumId?: string;
  correlationId?: string;
}

async function processTrackPage(request: TrackPageRequest): Promise<{
  success: boolean;
  message: string;
  batchId: string;
  processed?: number;
  failed?: number;
  status?: string;
}> {
  const { batchId, correlationId } = request;
  const processedTracks: string[] = [];
  const failedTracks: string[] = [];
  
  // Generate a correlation ID if one wasn't provided
  const traceId = correlationId || crypto.randomUUID();
  
  // Start tracking metrics
  const startTime = Date.now();
  
  try {
    // Log start of processing
    await logSystemEvent('info', 'process_track_page', 
      `Starting track page processing for batch ${batchId}`, 
      { batchId, request, traceId },
      traceId
    );
    
    // Check API rate limits before proceeding
    const apiLimited = await isApiRateLimited('spotify', '/tracks');
    if (apiLimited) {
      await logSystemEvent('warning', 'process_track_page', 
        `Spotify API is rate limited, delaying track page processing`, 
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
    const limit = request.limit || batch.metadata?.limit || 50;
    const albumId = request.albumId || batch.metadata?.album_id;
    
    // Ensure we have an album ID
    if (!albumId) {
      throw new Error('No album ID provided in request or batch metadata');
    }
    
    // Get album details
    const { data: album, error: albumError } = await supabase
      .from('albums')
      .select('id, name, spotify_id, artist_id')
      .eq('id', albumId)
      .single();
    
    if (albumError || !album) {
      throw new Error(`Album ${albumId} not found: ${albumError?.message || 'No data returned'}`);
    }
    
    // Get artist details
    const { data: artist, error: artistError } = await supabase
      .from('artists')
      .select('id, name')
      .eq('id', album.artist_id)
      .single();
    
    if (artistError) {
      await logSystemEvent('warning', 'process_track_page', 
        `Artist not found for album ${album.name}: ${artistError.message}`, 
        { albumId, album: album.name, artistId: album.artist_id, traceId },
        traceId
      );
      // Continue anyway, we can still process tracks
    }
    
    const artistName = artist?.name || 'Unknown';
    
    // Fetch tracks from Spotify
    await logSystemEvent('info', 'process_track_page', 
      `Fetching tracks for album ${album.name} by ${artistName} (offset: ${offset}, limit: ${limit})`, 
      { albumId, albumName: album.name, artistName, offset, limit, traceId },
      traceId
    );
    
    const { tracks, error: spotifyError } = await getAlbumTracks(
      album.spotify_id, 
      offset, 
      limit,
      traceId
    );
    
    if (spotifyError) {
      throw new Error(`Error fetching tracks from Spotify: ${spotifyError.message}`);
    }
    
    if (!tracks || tracks.length === 0) {
      await logSystemEvent('info', 'process_track_page', 
        `No tracks found for album ${album.name} at offset ${offset}`, 
        { albumId, albumName: album.name, offset, limit, traceId },
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
        message: `No tracks found for album ${album.name} at offset ${offset}`,
        batchId,
        processed: 0,
        failed: 0,
        status: 'completed'
      };
    }
    
    // Process each track
    await logSystemEvent('info', 'process_track_page', 
      `Processing ${tracks.length} tracks for album ${album.name}`, 
      { albumId, albumName: album.name, trackCount: tracks.length, traceId },
      traceId
    );
    
    // Update batch with total item count
    await supabase
      .from('processing_batches')
      .update({
        items_total: tracks.length
      })
      .eq('id', batchId);
    
    // Process tracks in smaller batches to avoid DB connection issues
    const batchSize = 10;
    const trackBatches = [];
    
    for (let i = 0; i < tracks.length; i += batchSize) {
      trackBatches.push(tracks.slice(i, i + batchSize));
    }
    
    for (const trackBatch of trackBatches) {
      const processingPromises = trackBatch.map(track => 
        processTrack(track, album.id, album.artist_id, traceId)
          .then(result => {
            if (result.success) {
              processedTracks.push(track.id);
            } else {
              failedTracks.push(track.id);
            }
            return result;
          })
      );
      
      await Promise.all(processingPromises);
      
      // Small delay between batches
      await sleep(200);
    }
    
    // Calculate processing time and success rate
    const processingTimeMs = Date.now() - startTime;
    const successRate = processedTracks.length / tracks.length;
    
    // Update batch status
    await supabase
      .from('processing_batches')
      .update({
        status: 'completed',
        items_total: tracks.length,
        items_processed: processedTracks.length,
        items_failed: failedTracks.length,
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
    
    await logSystemEvent('info', 'process_track_page', 
      `Completed track page processing for batch ${batchId}. Processed: ${processedTracks.length}, Failed: ${failedTracks.length}`, 
      { 
        batchId, 
        processed: processedTracks.length, 
        failed: failedTracks.length,
        processingTimeMs,
        successRate,
        traceId 
      },
      traceId
    );
    
    // Track metrics
    await incrementMetric('track_page_completion', 1, {
      track_count: tracks.length,
      success_rate: successRate,
      processing_time_ms: processingTimeMs,
      album_id: album.id,
      album_name: album.name.substring(0, 50)
    });
    
    return {
      success: true,
      message: `Processed ${processedTracks.length} tracks, failed ${failedTracks.length} tracks`,
      batchId,
      processed: processedTracks.length,
      failed: failedTracks.length,
      status: 'completed'
    };
    
  } catch (error) {
    // Log error
    console.error('Error in processTrackPage:', error);
    
    await logSystemEvent('error', 'process_track_page', 
      `Error processing track page: ${error.message}`, 
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
    await incrementMetric('track_page_errors', 1, {
      batch_id: batchId,
      error_type: error.name,
      error_message: error.message.substring(0, 100)  // Limit message length
    });
    
    return {
      success: false,
      message: `Error processing track page: ${error.message}`,
      batchId,
      processed: processedTracks.length,
      failed: failedTracks.length,
      status: 'error'
    };
  }
}

// Get tracks for an album from Spotify
async function getAlbumTracks(
  spotifyId: string, 
  offset: number, 
  limit: number,
  traceId: string
): Promise<{ tracks?: any[]; error?: Error }> {
  try {
    const response = await spotify.getAlbumTracks(
      spotifyId, 
      {
        offset,
        limit,
        market: 'US'  // Prioritize US market for consistency
      }
    );
    
    // Update rate limits
    if (response.headers) {
      await updateRateLimit(
        'spotify',
        '/tracks',
        response.headers,
        { status: response.status }
      );
    }
    
    // Check for errors
    if (!response.data || !response.data.items) {
      return { 
        error: new Error(`No tracks data returned from Spotify. Status: ${response.status}`) 
      };
    }
    
    return { tracks: response.data.items };
  } catch (error) {
    // Log the error
    await logSystemEvent('error', 'get_album_tracks', 
      `Error fetching tracks from Spotify: ${error.message}`, 
      { spotifyId, offset, limit, stack: error.stack, traceId },
      traceId
    );
    
    return { error };
  }
}

// Process a single track
async function processTrack(
  spotifyTrack: any, 
  albumId: string,
  artistId: string,
  traceId: string
): Promise<{ success: boolean; message: string; trackId?: string }> {
  try {
    // Prepare track data
    const trackData = {
      name: spotifyTrack.name,
      spotify_id: spotifyTrack.id,
      artist_id: artistId,  // Main artist ID
      preview_url: spotifyTrack.preview_url,
      spotify_url: spotifyTrack.external_urls?.spotify || null,
      popularity: spotifyTrack.popularity,
      metadata: {
        processed_at: new Date().toISOString(),
        trace_id: traceId,
        uri: spotifyTrack.uri,
        duration_ms: spotifyTrack.duration_ms,
        explicit: spotifyTrack.explicit,
        disc_number: spotifyTrack.disc_number,
        track_number: spotifyTrack.track_number
      }
    };
    
    // Upsert track
    const { data: track, error: trackError } = await supabase
      .from('tracks')
      .upsert(trackData, { 
        onConflict: 'spotify_id',
        ignoreDuplicates: false  // Update if exists
      })
      .select('id')
      .single();
    
    if (trackError) {
      throw new Error(`Error upserting track: ${trackError.message}`);
    }
    
    if (!track) {
      throw new Error('No track data returned after upsert');
    }
    
    // Add relationship to album (track_albums junction table)
    const trackAlbumData = {
      track_id: track.id,
      album_id: albumId,
      disc_number: spotifyTrack.disc_number,
      track_number: spotifyTrack.track_number
    };
    
    const { error: relationError } = await supabase
      .from('track_albums')
      .upsert(trackAlbumData, {
        onConflict: 'track_id,album_id'
      });
    
    if (relationError) {
      throw new Error(`Error linking track to album: ${relationError.message}`);
    }
    
    // Add relationships to artists (track_artists junction table)
    if (spotifyTrack.artists && spotifyTrack.artists.length > 0) {
      // Process each artist
      for (const artist of spotifyTrack.artists) {
        // Check if this artist exists
        const { data: existingArtist } = await supabase
          .from('artists')
          .select('id')
          .eq('spotify_id', artist.id)
          .maybeSingle();
        
        let artistId = existingArtist?.id;
        
        // If artist doesn't exist, create a stub record
        if (!artistId) {
          const { data: newArtist, error: artistCreateError } = await supabase
            .from('artists')
            .insert({
              name: artist.name,
              spotify_id: artist.id,
              spotify_url: artist.external_urls?.spotify,
              metadata: {
                stub: true,
                created_from_track: true,
                trace_id: traceId,
                created_at: new Date().toISOString()
              }
            })
            .select('id')
            .single();
          
          if (artistCreateError) {
            console.error(`Error creating artist stub for ${artist.name}:`, artistCreateError);
            continue;
          }
          
          artistId = newArtist.id;
        }
        
        // Add track-artist relationship
        if (artistId) {
          const { error: artistRelationError } = await supabase
            .from('track_artists')
            .upsert({
              track_id: track.id,
              artist_id: artistId,
              is_primary: artist.id === spotifyTrack.artists[0].id  // First artist is primary
            }, {
              onConflict: 'track_id,artist_id'
            });
          
          if (artistRelationError) {
            console.error(`Error linking track to artist ${artist.name}:`, artistRelationError);
          }
        }
      }
    }
    
    // Calculate data quality score
    const qualityScore = calculateTrackQualityScore(spotifyTrack);
    
    // Update data quality
    await updateDataQualityScore(
      'track',
      track.id,
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
    await incrementMetric('tracks_processed', 1, {
      has_preview: spotifyTrack.preview_url ? true : false,
      is_explicit: spotifyTrack.explicit,
      artist_count: spotifyTrack.artists?.length || 1
    });
    
    return {
      success: true,
      message: `Processed track: ${spotifyTrack.name}`,
      trackId: track.id
    };
  } catch (error) {
    // Log error
    await logSystemEvent('error', 'process_track', 
      `Error processing track ${spotifyTrack.name}: ${error.message}`, 
      { trackName: spotifyTrack.name, stack: error.stack, traceId },
      traceId
    );
    
    // Track error metrics
    await incrementMetric('track_processing_errors', 1, {
      error_type: error.name,
      track_name: spotifyTrack.name.substring(0, 50)
    });
    
    return {
      success: false,
      message: `Error processing track: ${error.message}`
    };
  }
}

// Calculate track data quality score based on completeness
function calculateTrackQualityScore(track: any): number {
  let score = 0.5; // Base score
  
  // Add points for various fields being present
  if (track.name) score += 0.1;
  if (track.preview_url) score += 0.15;  // Preview URL is valuable
  if (track.external_urls?.spotify) score += 0.05;
  if (track.duration_ms) score += 0.05;
  if (track.artists && track.artists.length > 0) score += 0.1;
  if (track.disc_number) score += 0.025;
  if (track.track_number) score += 0.025;
  
  // Cap at 1.0
  return Math.min(score, 1.0);
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  try {
    let request: TrackPageRequest;
    
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
        limit: parseInt(url.searchParams.get('limit') || '50'),
        albumId: url.searchParams.get('albumId') || undefined,
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
    console.error('Error in process-track-page function:', error);
    
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
