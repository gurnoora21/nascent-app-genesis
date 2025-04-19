
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { 
  checkBatchCompletion, 
  validateBatchIntegrity, 
  logSystemEvent, 
  generateCorrelationId,
  incrementMetric,
  checkCircuitBreaker,
  checkBackpressure,
  trackErrorRates
} from "../lib/pipeline-utils.ts";
import { supabase, SpotifyClient } from "../lib/api-clients.ts";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

// Configure alert thresholds
const DLQ_ALERT_THRESHOLD = 100; // Alert if DLQ exceeds this size
const ERROR_RATE_ALERT_THRESHOLD = 0.25; // 25% error rate is concerning
const MAX_CONCURRENCY_PER_FUNCTION = 5; // Maximum concurrent executions per function

interface MonitorRequest {
  checkFinalizedBatches?: boolean;
  checkForStuckBatches?: boolean;
  checkDLQThreshold?: boolean;
  checkErrorRates?: boolean;
  checkCircuitBreakers?: boolean;
  notifyOnCompletion?: boolean;
  specificBatchId?: string;
  correlationId?: string;
}

async function monitorPipeline(request: MonitorRequest = {}): Promise<{
  success: boolean;
  message: string;
  alerts?: any[];
  completedBatches?: string[];
  stuckBatches?: string[];
}> {
  // Generate correlation ID for tracing this monitoring run
  const correlationId = request.correlationId || generateCorrelationId();
  const alerts: any[] = [];
  
  try {
    await logSystemEvent('info', 'monitor_pipeline', 
      'Starting pipeline monitoring run', 
      { correlationId, request },
      correlationId
    );
    
    // Start tracking metrics 
    const startTime = Date.now();
    
    // Check if DLQ is getting too large
    if (request.checkDLQThreshold !== false) {
      const { count: dlqCount, error: dlqError } = await supabase
        .from('dead_letter_items')
        .select('*', { count: 'exact', head: true });
      
      if (!dlqError && dlqCount && dlqCount > DLQ_ALERT_THRESHOLD) {
        const alertMessage = `Dead letter queue has grown to ${dlqCount} items, exceeding threshold of ${DLQ_ALERT_THRESHOLD}`;
        
        await logSystemEvent('warning', 'monitor_pipeline', alertMessage, 
          { dlqCount, threshold: DLQ_ALERT_THRESHOLD, correlationId },
          correlationId
        );
        
        alerts.push({
          type: 'dlq_threshold',
          message: alertMessage,
          count: dlqCount,
          threshold: DLQ_ALERT_THRESHOLD
        });
        
        await incrementMetric('dlq_threshold_alerts', 1);
      }
    }
    
    // Check if we need to bootstrap the pipeline by discovering artists
    await checkPipelineBootstrap(correlationId);
    
    // Check for completed batches and trigger next stages
    let completedBatches: string[] = [];
    
    if (request.checkFinalizedBatches !== false) {
      // Either check a specific batch or find recently completed batches
      if (request.specificBatchId) {
        await checkBatchCompletion(request.specificBatchId).then(async completionStatus => {
          if (completionStatus.complete) {
            completedBatches.push(request.specificBatchId!);
            
            await handleCompletedBatch(
              request.specificBatchId!, 
              completionStatus, 
              correlationId
            );
          }
        });
      } else {
        // Check batches by detecting completion signals
        await checkForCompletionSignals(correlationId);
        
        // Check the following types of processing batches:
        const batchTypes = [
          'discover_artists',      // Artist discovery
          'process_album_page',    // Page-based album processing
          'process_track_page',    // Page-based track processing
          'process_producers_batch' // Producer processing
        ];
        
        for (const batchType of batchTypes) {
          // Find batches to check for completion
          const { data: activeBatches, error: batchError } = await supabase
            .from('processing_batches')
            .select('id, batch_type, status, metadata')
            .eq('batch_type', batchType)
            .in('status', ['processing', 'completed_with_next'])
            .is('completed_at', null)
            .order('created_at', { ascending: true })
            .limit(20);  // Process in manageable chunks
          
          if (!batchError && activeBatches && activeBatches.length > 0) {
            for (const batch of activeBatches) {
              const completionStatus = await checkBatchCompletion(batch.id);
              
              if (completionStatus.complete) {
                completedBatches.push(batch.id);
                
                await handleCompletedBatch(
                  batch.id, 
                  completionStatus, 
                  correlationId
                );
              }
            }
          }
        }
      }
    }
    
    // Check for stuck or stalled batches
    let stuckBatches: string[] = [];
    
    if (request.checkForStuckBatches !== false) {
      // Find batches that have been "processing" for too long
      const staleCutoff = new Date();
      staleCutoff.setHours(staleCutoff.getHours() - 2); // 2 hours is too long
      
      const { data: staleBatches, error: staleError } = await supabase
        .from('processing_batches')
        .select('id, batch_type, started_at, claimed_by')
        .eq('status', 'processing')
        .lt('updated_at', staleCutoff.toISOString())
        .order('started_at', { ascending: true });
      
      if (!staleError && staleBatches && staleBatches.length > 0) {
        for (const batch of staleBatches) {
          stuckBatches.push(batch.id);
          
          // Log stalled batch
          await logSystemEvent('warning', 'monitor_pipeline', 
            `Detected stalled batch: ${batch.id} (${batch.batch_type})`, 
            { 
              batchId: batch.id, 
              batchType: batch.batch_type, 
              startedAt: batch.started_at,
              claimedBy: batch.claimed_by,
              correlationId 
            },
            correlationId
          );
          
          // Reset the stalled batch so it can be picked up again
          const { data: resetSuccess } = await supabase.rpc('reset_batch', {
            p_batch_id: batch.id
          });
          
          if (resetSuccess) {
            await logSystemEvent('info', 'monitor_pipeline', 
              `Reset stalled batch: ${batch.id}`, 
              { batchId: batch.id, correlationId },
              correlationId
            );
            
            await incrementMetric('stalled_batches_reset', 1, {
              batch_type: batch.batch_type
            });
          }
        }
        
        alerts.push({
          type: 'stalled_batches',
          message: `Found ${staleBatches.length} stalled batches`,
          count: staleBatches.length,
          batchIds: stuckBatches
        });
      }
    }
    
    // Check error rates across batch types
    if (request.checkErrorRates !== false) {
      // Get a list of active batch types
      const { data: batchTypes } = await supabase
        .from('processing_batches')
        .select('batch_type')
        .filter('created_at', 'gte', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString())
        .is('batch_type', 'not.null');
      
      // Get unique batch types
      const uniqueBatchTypes = [...new Set(batchTypes?.map(b => b.batch_type))];
      
      for (const batchType of uniqueBatchTypes) {
        const { errorRate, retryRate, alertThresholdExceeded } = 
          await trackErrorRates(batchType);
        
        if (alertThresholdExceeded) {
          alerts.push({
            type: 'error_rate',
            message: `High error rate (${(errorRate * 100).toFixed(1)}%) for batch type: ${batchType}`,
            batchType,
            errorRate,
            retryRate
          });
        }
      }
    }
    
    // Check circuit breakers for APIs
    if (request.checkCircuitBreakers !== false) {
      const apis = ['spotify', 'discogs', 'genius'];
      
      for (const api of apis) {
        const circuitOpen = await checkCircuitBreaker(api);
        
        if (circuitOpen) {
          alerts.push({
            type: 'circuit_breaker',
            message: `Circuit breaker open for ${api} API`,
            apiName: api
          });
        }
      }
    }
    
    // Calculate runtime and log metrics
    const runtime = Date.now() - startTime;
    await incrementMetric('monitor_pipeline_duration', runtime, {
      completed_batches: completedBatches.length,
      stuck_batches: stuckBatches.length,
      alerts: alerts.length
    });
    
    await logSystemEvent('info', 'monitor_pipeline', 
      `Completed pipeline monitoring run in ${runtime}ms`, 
      { 
        runtime, 
        completedBatchCount: completedBatches.length, 
        stuckBatchCount: stuckBatches.length,
        alertCount: alerts.length,
        correlationId 
      },
      correlationId
    );
    
    return {
      success: true,
      message: `Monitoring complete. Found ${completedBatches.length} completed batches, ${stuckBatches.length} stuck batches, and ${alerts.length} alerts.`,
      alerts,
      completedBatches,
      stuckBatches
    };
  } catch (error) {
    // Log error
    console.error('Error in monitorPipeline:', error);
    
    await logSystemEvent('error', 'monitor_pipeline', 
      `Error in pipeline monitoring: ${error.message}`, 
      { 
        stack: error.stack,
        correlationId 
      },
      correlationId
    );
    
    return {
      success: false,
      message: `Error in pipeline monitoring: ${error.message}`,
      alerts: []
    };
  }
}

// Check for completion signals and trigger the next stage
async function checkForCompletionSignals(correlationId: string): Promise<void> {
  try {
    // Check for artists_completed signal
    await checkArtistsCompletedSignal(correlationId);
    
    // Check for albums_completed signal
    await checkAlbumsCompletedSignal(correlationId);
    
    // Check for tracks_completed signal
    await checkTracksCompletedSignal(correlationId);
    
    // Check for producers_completed signal
    await checkProducersCompletedSignal(correlationId);
  } catch (error) {
    await logSystemEvent('error', 'check_completion_signals', 
      `Error checking completion signals: ${error.message}`, 
      { correlationId, stack: error.stack },
      correlationId
    );
  }
}

// Check for artists_completed signal
async function checkArtistsCompletedSignal(correlationId: string): Promise<void> {
  try {
    // Find batches with artists_completed = true that haven't been processed yet
    const { data: completedBatches, error } = await supabase
      .from('processing_batches')
      .select('id, metadata')
      .eq('status', 'completed')
      .eq('metadata->artists_completed', true)
      .is('metadata->albums_dispatched', null); // Not yet dispatched albums
    
    if (error || !completedBatches || completedBatches.length === 0) {
      return;
    }
    
    // For each batch, dispatch album processing
    for (const batch of completedBatches) {
      await logSystemEvent('info', 'monitor_pipeline', 
        `Found artists_completed signal for batch ${batch.id}`, 
        { batchId: batch.id, correlationId },
        correlationId
      );
      
      // Process each artist in this batch
      const { data: artistItems, error: itemsError } = await supabase
        .from('processing_items')
        .select('item_id')
        .eq('batch_id', batch.id)
        .eq('item_type', 'artist')
        .eq('status', 'completed');
      
      if (itemsError || !artistItems || artistItems.length === 0) {
        await logSystemEvent('warning', 'monitor_pipeline', 
          `No completed artists found in batch ${batch.id}`, 
          { batchId: batch.id, correlationId },
          correlationId
        );
        continue;
      }
      
      // Create album page batches for each artist
      for (const artistItem of artistItems) {
        // Get artist details to create album page batches
        const { data: artist, error: artistError } = await supabase
          .from('artists')
          .select('id, name, spotify_id')
          .eq('spotify_id', artistItem.item_id)
          .single();
        
        if (artistError || !artist) {
          await logSystemEvent('warning', 'monitor_pipeline', 
            `Could not find artist with spotify_id ${artistItem.item_id}`, 
            { artistSpotifyId: artistItem.item_id, correlationId },
            correlationId
          );
          continue;
        }
        
        // Start with a single album page batch - it will create more if needed
        await createAlbumPageBatch(
          artist.id, 
          artist.name, 
          artist.spotify_id, 
          0, // Starting offset
          20, // Page size
          correlationId,
          batch.id // parent batch ID
        );
      }
      
      // Mark this batch as having dispatched albums
      await supabase
        .from('processing_batches')
        .update({
          metadata: {
            ...batch.metadata,
            albums_dispatched: true,
            albums_dispatched_at: new Date().toISOString()
          }
        })
        .eq('id', batch.id);
      
      await logSystemEvent('info', 'monitor_pipeline', 
        `Dispatched album processing for all artists in batch ${batch.id}`, 
        { batchId: batch.id, artistCount: artistItems.length, correlationId },
        correlationId
      );
    }
  } catch (error) {
    await logSystemEvent('error', 'check_artists_completed', 
      `Error processing artists_completed signal: ${error.message}`, 
      { correlationId, stack: error.stack },
      correlationId
    );
  }
}

// Create an album page batch
async function createAlbumPageBatch(
  artistId: string,
  artistName: string,
  spotifyId: string,
  offset: number,
  limit: number,
  correlationId: string,
  parentBatchId?: string
): Promise<string | null> {
  try {
    // Check if we're still allowed to process
    const isAPIRateLimited = await checkCircuitBreaker('spotify');
    if (isAPIRateLimited) {
      await logSystemEvent('warning', 'create_album_page_batch', 
        `Spotify API is rate limited, delaying album page batch creation for artist ${artistName}`, 
        { artistId, spotifyId, correlationId },
        correlationId
      );
      return null;
    }
    
    // Check for backpressure
    const hasBackpressure = await checkBackpressure('process_album_page', MAX_CONCURRENCY_PER_FUNCTION);
    if (hasBackpressure) {
      await logSystemEvent('warning', 'create_album_page_batch', 
        `System has backpressure, delaying album page batch creation for artist ${artistName}`, 
        { artistId, spotifyId, correlationId },
        correlationId
      );
      return null;
    }
    
    // Create a new album page batch
    const { data: newBatch, error } = await supabase
      .from('processing_batches')
      .insert({
        batch_type: 'process_album_page',
        status: 'pending',
        metadata: {
          artist_id: artistId,
          artist_name: artistName,
          spotify_id: spotifyId,
          offset,
          limit,
          page_index: Math.floor(offset / limit),
          parent_batch_id: parentBatchId,
          correlation_id: correlationId,
          created_at: new Date().toISOString()
        }
      })
      .select('id')
      .single();
    
    if (error) {
      await logSystemEvent('error', 'create_album_page_batch', 
        `Error creating album page batch for artist ${artistName}: ${error.message}`, 
        { artistId, spotifyId, offset, limit, correlationId },
        correlationId
      );
      return null;
    }
    
    // Call the process-album-page function to process this batch
    EdgeRuntime.waitUntil(
      supabase.functions.invoke('process-album-page', {
        body: {
          batchId: newBatch.id,
          artistId,
          artistName,
          spotifyId,
          offset,
          limit,
          correlationId
        }
      })
    );
    
    await logSystemEvent('info', 'create_album_page_batch', 
      `Created album page batch ${newBatch.id} for artist ${artistName} (offset: ${offset}, limit: ${limit})`, 
      { artistId, spotifyId, batchId: newBatch.id, offset, limit, correlationId },
      correlationId
    );
    
    return newBatch.id;
  } catch (error) {
    await logSystemEvent('error', 'create_album_page_batch', 
      `Error creating album page batch: ${error.message}`, 
      { artistId, spotifyId, offset, limit, correlationId, stack: error.stack },
      correlationId
    );
    return null;
  }
}

// Check for albums_completed signal
async function checkAlbumsCompletedSignal(correlationId: string): Promise<void> {
  try {
    // Find all album page batches with is_final_page = true
    const { data: finalPages, error } = await supabase
      .from('processing_batches')
      .select('id, metadata')
      .eq('batch_type', 'process_album_page')
      .in('status', ['completed', 'completed_with_next'])
      .eq('metadata->is_final_page', true)
      .is('metadata->tracks_dispatched', null); // Not yet dispatched tracks
    
    if (error || !finalPages || finalPages.length === 0) {
      return;
    }
    
    // For each final page, check all pages for that artist to see if all completed
    for (const page of finalPages) {
      const artistId = page.metadata?.artist_id;
      const parentBatchId = page.metadata?.parent_batch_id;
      
      if (!artistId) {
        continue;
      }
      
      await logSystemEvent('info', 'monitor_pipeline', 
        `Found final album page for artist ${artistId}`, 
        { artistId, finalPageId: page.id, correlationId },
        correlationId
      );
      
      // Get all pages for this artist to check if all are complete
      const { data: allPages, error: allPagesError } = await supabase
        .from('processing_batches')
        .select('id, status')
        .eq('batch_type', 'process_album_page')
        .eq('metadata->artist_id', artistId);
      
      if (allPagesError || !allPages || allPages.length === 0) {
        continue;
      }
      
      // Check if all pages are completed
      const allCompleted = allPages.every(p => 
        p.status === 'completed' || p.status === 'completed_with_next' || p.status === 'error'
      );
      
      if (allCompleted) {
        await logSystemEvent('info', 'monitor_pipeline', 
          `All album pages completed for artist ${artistId}`, 
          { artistId, pageCount: allPages.length, correlationId },
          correlationId
        );
        
        // Mark all these pages as having albums completed
        for (const albumPage of allPages) {
          await supabase
            .from('processing_batches')
            .update({
              metadata: {
                ...page.metadata,
                albums_completed: true,
                albums_completed_at: new Date().toISOString(),
                tracks_dispatched: false
              }
            })
            .eq('id', albumPage.id);
        }
        
        // Find all albums for this artist in our database to process tracks
        const { data: artistAlbums, error: albumsError } = await supabase
          .from('albums')
          .select('id, name, spotify_id, total_tracks')
          .eq('artist_id', artistId);
        
        if (albumsError || !artistAlbums || artistAlbums.length === 0) {
          await logSystemEvent('warning', 'monitor_pipeline', 
            `No albums found for artist ${artistId}, but all album pages are complete`, 
            { artistId, correlationId },
            correlationId
          );
          continue;
        }
        
        // Create a track processing batch for this artist
        const { data: trackBatch, error: trackBatchError } = await supabase
          .from('processing_batches')
          .insert({
            batch_type: 'process_tracks',
            status: 'pending',
            metadata: {
              artist_id: artistId,
              album_count: artistAlbums.length,
              parent_batch_id: parentBatchId,
              correlation_id: correlationId,
              source: 'monitor_pipeline',
              created_at: new Date().toISOString()
            }
          })
          .select('id')
          .single();
        
        if (trackBatchError || !trackBatch) {
          await logSystemEvent('error', 'monitor_pipeline', 
            `Error creating track batch for artist ${artistId}: ${trackBatchError?.message || 'No data returned'}`, 
            { artistId, correlationId },
            correlationId
          );
          continue;
        }
        
        // Update the parent batch (if any) to show albums are completed
        if (parentBatchId) {
          await supabase
            .from('processing_batches')
            .update({
              metadata: {
                albums_completed: true,
                albums_completed_at: new Date().toISOString(),
                track_batch_id: trackBatch.id
              }
            })
            .eq('id', parentBatchId);
        }
        
        // Create a track page batch for each album
        for (const album of artistAlbums) {
          if (!album.spotify_id) continue;
          
          // Create the first track page batch for this album - it will create more if needed
          await createTrackPageBatch(
            album.id,
            album.name,
            album.spotify_id,
            0, // Starting offset
            50, // Page size
            trackBatch.id,
            artistId,
            correlationId
          );
        }
        
        // Mark the final page as tracks dispatched
        await supabase
          .from('processing_batches')
          .update({
            metadata: {
              ...page.metadata,
              tracks_dispatched: true,
              tracks_dispatched_at: new Date().toISOString()
            }
          })
          .eq('id', page.id);
        
        await logSystemEvent('info', 'monitor_pipeline', 
          `Dispatched track processing for all albums of artist ${artistId}`, 
          { artistId, albumCount: artistAlbums.length, correlationId },
          correlationId
        );
      }
    }
  } catch (error) {
    await logSystemEvent('error', 'check_albums_completed', 
      `Error processing albums_completed signal: ${error.message}`, 
      { correlationId, stack: error.stack },
      correlationId
    );
  }
}

// Create a track page batch
async function createTrackPageBatch(
  albumId: string,
  albumName: string,
  spotifyId: string,
  offset: number,
  limit: number,
  parentAlbumBatchId: string,
  artistId: string,
  correlationId: string
): Promise<string | null> {
  try {
    // Check if the API is rate limited
    const isAPIRateLimited = await checkCircuitBreaker('spotify');
    if (isAPIRateLimited) {
      await logSystemEvent('warning', 'create_track_page_batch', 
        `Spotify API is rate limited, delaying track page batch creation for album ${albumName}`, 
        { albumId, spotifyId, correlationId },
        correlationId
      );
      return null;
    }
    
    // Check for backpressure
    const hasBackpressure = await checkBackpressure('process_track_page', MAX_CONCURRENCY_PER_FUNCTION);
    if (hasBackpressure) {
      await logSystemEvent('warning', 'create_track_page_batch', 
        `System has backpressure, delaying track page batch creation for album ${albumName}`, 
        { albumId, spotifyId, correlationId },
        correlationId
      );
      return null;
    }
    
    // Create a new track page batch
    const { data: newBatch, error } = await supabase
      .from('processing_batches')
      .insert({
        batch_type: 'process_track_page',
        status: 'pending',
        metadata: {
          album_id: albumId,
          album_name: albumName,
          spotify_id: spotifyId,
          artist_id: artistId,
          offset,
          limit,
          page_index: Math.floor(offset / limit),
          parent_album_batch_id: parentAlbumBatchId,
          correlation_id: correlationId,
          created_at: new Date().toISOString()
        }
      })
      .select('id')
      .single();
    
    if (error) {
      await logSystemEvent('error', 'create_track_page_batch', 
        `Error creating track page batch for album ${albumName}: ${error.message}`, 
        { albumId, spotifyId, offset, limit, correlationId },
        correlationId
      );
      return null;
    }
    
    // Add this album to the parent batch as an item to track
    await supabase
      .from('processing_items')
      .insert({
        batch_id: parentAlbumBatchId,
        item_type: 'album',
        item_id: albumId,
        status: 'pending',
        priority: 5,
        metadata: {
          album_name: albumName,
          spotify_id: spotifyId,
          offset,
          limit,
          correlation_id: correlationId
        }
      });
    
    // Call the process-track-page function to process this batch
    EdgeRuntime.waitUntil(
      supabase.functions.invoke('process-track-page', {
        body: {
          batchId: newBatch.id,
          albumId,
          albumName,
          spotifyId,
          offset,
          limit,
          correlationId,
          parentAlbumBatchId
        }
      })
    );
    
    await logSystemEvent('info', 'create_track_page_batch', 
      `Created track page batch ${newBatch.id} for album ${albumName} (offset: ${offset}, limit: ${limit})`, 
      { albumId, spotifyId, batchId: newBatch.id, offset, limit, correlationId },
      correlationId
    );
    
    return newBatch.id;
  } catch (error) {
    await logSystemEvent('error', 'create_track_page_batch', 
      `Error creating track page batch: ${error.message}`, 
      { albumId, spotifyId, offset, limit, correlationId, stack: error.stack },
      correlationId
    );
    return null;
  }
}

// Check for tracks_completed signal
async function checkTracksCompletedSignal(correlationId: string): Promise<void> {
  try {
    // Find all track page batches with is_final_page = true
    const { data: finalPages, error } = await supabase
      .from('processing_batches')
      .select('id, metadata')
      .eq('batch_type', 'process_track_page')
      .in('status', ['completed', 'completed_with_next'])
      .eq('metadata->is_final_page', true)
      .is('metadata->producers_dispatched', null); // Not yet dispatched producers
    
    if (error || !finalPages || finalPages.length === 0) {
      return;
    }
    
    // For each final page, check all pages for that album to see if all completed
    for (const page of finalPages) {
      const albumId = page.metadata?.album_id;
      const parentAlbumBatchId = page.metadata?.parent_album_batch_id;
      
      if (!albumId || !parentAlbumBatchId) {
        continue;
      }
      
      await logSystemEvent('info', 'monitor_pipeline', 
        `Found final track page for album ${albumId}`, 
        { albumId, finalPageId: page.id, correlationId },
        correlationId
      );
      
      // Get all pages for this album to check if all are complete
      const { data: allPages, error: allPagesError } = await supabase
        .from('processing_batches')
        .select('id, status')
        .eq('batch_type', 'process_track_page')
        .eq('metadata->album_id', albumId);
      
      if (allPagesError || !allPages || allPages.length === 0) {
        continue;
      }
      
      // Check if all pages are completed
      const allCompleted = allPages.every(p => 
        p.status === 'completed' || p.status === 'completed_with_next' || p.status === 'error'
      );
      
      if (allCompleted) {
        await logSystemEvent('info', 'monitor_pipeline', 
          `All track pages completed for album ${albumId}`, 
          { albumId, pageCount: allPages.length, correlationId },
          correlationId
        );
        
        // Mark all these pages as having tracks completed
        for (const trackPage of allPages) {
          await supabase
            .from('processing_batches')
            .update({
              metadata: {
                ...trackPage.metadata,
                tracks_completed: true,
                tracks_completed_at: new Date().toISOString()
              }
            })
            .eq('id', trackPage.id);
        }
        
        // Update the album item in parent batch as completed
        const { data: albumItems, error: itemsError } = await supabase
          .from('processing_items')
          .select('id')
          .eq('batch_id', parentAlbumBatchId)
          .eq('item_id', albumId)
          .eq('item_type', 'album');
        
        if (!itemsError && albumItems && albumItems.length > 0) {
          await supabase
            .from('processing_items')
            .update({
              status: 'completed',
              updated_at: new Date().toISOString(),
              metadata: {
                tracks_completed_at: new Date().toISOString(),
                tracks_correlation_id: correlationId
              }
            })
            .eq('id', albumItems[0].id);
        }
        
        // Check if all albums in the parent batch are now completed
        const { data: allItems, error: allItemsError } = await supabase
          .from('processing_items')
          .select('id, status')
          .eq('batch_id', parentAlbumBatchId);
        
        if (!allItemsError && allItems && allItems.length > 0) {
          const allItemsCompleted = allItems.every(item => 
            item.status === 'completed' || item.status === 'error'
          );
          
          if (allItemsCompleted) {
            // All albums have completed tracks processing
            await supabase
              .from('processing_batches')
              .update({
                status: 'completed',
                completed_at: new Date().toISOString(),
                updated_at: new Date().toISOString(),
                metadata: {
                  tracks_completed: true,
                  tracks_completed_at: new Date().toISOString(),
                  tracks_correlation_id: correlationId
                }
              })
              .eq('id', parentAlbumBatchId);
            
            // Trigger producers processing
            await dispatchProducerProcessing(parentAlbumBatchId, correlationId);
            
            await logSystemEvent('info', 'monitor_pipeline', 
              `All tracks for all albums in batch ${parentAlbumBatchId} completed. Dispatching producers processing.`, 
              { parentAlbumBatchId, correlationId },
              correlationId
            );
          }
        }
        
        // Mark the final page as producers dispatched
        await supabase
          .from('processing_batches')
          .update({
            metadata: {
              ...page.metadata,
              producers_dispatched: true,
              producers_dispatched_at: new Date().toISOString()
            }
          })
          .eq('id', page.id);
      }
    }
  } catch (error) {
    await logSystemEvent('error', 'check_tracks_completed', 
      `Error processing tracks_completed signal: ${error.message}`, 
      { correlationId, stack: error.stack },
      correlationId
    );
  }
}

// Dispatch producer processing
async function dispatchProducerProcessing(parentTrackBatchId: string, correlationId: string): Promise<void> {
  try {
    // Get the batch info
    const { data: parentBatch, error: batchError } = await supabase
      .from('processing_batches')
      .select('metadata')
      .eq('id', parentTrackBatchId)
      .single();
    
    if (batchError || !parentBatch) {
      await logSystemEvent('error', 'dispatch_producer_processing', 
        `Error getting parent batch ${parentTrackBatchId}: ${batchError?.message || 'No data returned'}`, 
        { parentTrackBatchId, correlationId },
        correlationId
      );
      return;
    }
    
    // Create a producer processing batch
    const { data: producerBatch, error: producerBatchError } = await supabase
      .from('processing_batches')
      .insert({
        batch_type: 'process_producers_batch',
        status: 'pending',
        metadata: {
          parent_batch_id: parentTrackBatchId,
          artist_id: parentBatch.metadata?.artist_id,
          correlation_id: correlationId,
          source: 'monitor_pipeline',
          created_at: new Date().toISOString()
        }
      })
      .select('id')
      .single();
    
    if (producerBatchError || !producerBatch) {
      await logSystemEvent('error', 'dispatch_producer_processing', 
        `Error creating producer batch: ${producerBatchError?.message || 'No data returned'}`, 
        { parentTrackBatchId, correlationId },
        correlationId
      );
      return;
    }
    
    // Find all tracks for this artist to process producers
    const { data: tracks, error: tracksError } = await supabase
      .from('tracks')
      .select('id, name, spotify_id')
      .eq('artist_id', parentBatch.metadata?.artist_id);
    
    if (tracksError || !tracks || tracks.length === 0) {
      await logSystemEvent('warning', 'dispatch_producer_processing', 
        `No tracks found for artist ${parentBatch.metadata?.artist_id}`, 
        { artistId: parentBatch.metadata?.artist_id, correlationId },
        correlationId
      );
      return;
    }
    
    // Add each track as an item to process
    const trackItems = tracks.map(track => ({
      batch_id: producerBatch.id,
      item_type: 'track',
      item_id: track.id,
      status: 'pending',
      priority: 5,
      metadata: {
        track_name: track.name,
        spotify_id: track.spotify_id,
        correlation_id: correlationId
      }
    }));
    
    // Insert in chunks to avoid exceeding payload limits
    const chunkSize = 100;
    for (let i = 0; i < trackItems.length; i += chunkSize) {
      const chunk = trackItems.slice(i, i + chunkSize);
      
      const { error: insertError } = await supabase
        .from('processing_items')
        .insert(chunk);
      
      if (insertError) {
        await logSystemEvent('error', 'dispatch_producer_processing', 
          `Error adding tracks to producer batch: ${insertError.message}`, 
          { batchId: producerBatch.id, chunkIndex: i, chunkSize, correlationId },
          correlationId
        );
      }
    }
    
    // Update the batch with the accurate count
    await supabase
      .from('processing_batches')
      .update({
        items_total: tracks.length,
        updated_at: new Date().toISOString()
      })
      .eq('id', producerBatch.id);
    
    // Call the producer processing function
    EdgeRuntime.waitUntil(
      supabase.functions.invoke('process-producers-batch', {
        body: {
          batchId: producerBatch.id,
          correlationId
        }
      })
    );
    
    await logSystemEvent('info', 'dispatch_producer_processing', 
      `Dispatched producer processing for ${tracks.length} tracks`, 
      { batchId: producerBatch.id, trackCount: tracks.length, correlationId },
      correlationId
    );
    
    // Update the parent batch to link to the producer batch
    await supabase
      .from('processing_batches')
      .update({
        metadata: {
          ...parentBatch.metadata,
          producer_batch_id: producerBatch.id,
          producers_dispatched: true,
          producers_dispatched_at: new Date().toISOString()
        }
      })
      .eq('id', parentTrackBatchId);
  } catch (error) {
    await logSystemEvent('error', 'dispatch_producer_processing', 
      `Error dispatching producer processing: ${error.message}`, 
      { parentTrackBatchId, correlationId, stack: error.stack },
      correlationId
    );
  }
}

// Check for producers_completed signal - this completes the entire pipeline
async function checkProducersCompletedSignal(correlationId: string): Promise<void> {
  try {
    // Find all producer batches that are completed
    const { data: completedBatches, error } = await supabase
      .from('processing_batches')
      .select('id, metadata')
      .eq('batch_type', 'process_producers_batch')
      .eq('status', 'completed')
      .is('metadata->pipeline_completed', null); // Not yet marked as pipeline completed
    
    if (error || !completedBatches || completedBatches.length === 0) {
      return;
    }
    
    // For each completed producer batch, mark the entire pipeline as complete
    for (const batch of completedBatches) {
      const parentBatchId = batch.metadata?.parent_batch_id;
      
      if (!parentBatchId) {
        continue;
      }
      
      await logSystemEvent('info', 'monitor_pipeline', 
        `Producer processing completed for batch ${batch.id}`, 
        { batchId: batch.id, parentBatchId, correlationId },
        correlationId
      );
      
      // Mark this batch as pipeline completed
      await supabase
        .from('processing_batches')
        .update({
          metadata: {
            ...batch.metadata,
            pipeline_completed: true,
            pipeline_completed_at: new Date().toISOString()
          }
        })
        .eq('id', batch.id);
      
      // Find the original artist batch by tracing back through parents
      let currentBatchId = parentBatchId;
      let originalBatchId = null;
      let originalBatchType = null;
      let notifyOnCompletion = false;
      
      while (currentBatchId) {
        const { data: currentBatch, error: batchError } = await supabase
          .from('processing_batches')
          .select('metadata, batch_type')
          .eq('id', currentBatchId)
          .single();
        
        if (batchError || !currentBatch) {
          break;
        }
        
        // If this batch type is discovery or initial, this is the original
        if (currentBatch.batch_type === 'discover_artists') {
          originalBatchId = currentBatchId;
          originalBatchType = currentBatch.batch_type;
          notifyOnCompletion = currentBatch.metadata?.notifyOnCompletion || false;
          break;
        }
        
        // Check for parent batch ID in metadata
        currentBatchId = currentBatch.metadata?.parent_batch_id;
      }
      
      // If we found the original batch, mark it as pipeline completed
      if (originalBatchId) {
        await supabase
          .from('processing_batches')
          .update({
            metadata: {
              pipeline_completed: true,
              pipeline_completed_at: new Date().toISOString(),
              final_producer_batch_id: batch.id
            },
            updated_at: new Date().toISOString()
          })
          .eq('id', originalBatchId);
        
        await logSystemEvent('info', 'monitor_pipeline', 
          `Marked original ${originalBatchType} batch ${originalBatchId} as pipeline completed`, 
          { originalBatchId, originalBatchType, producerBatchId: batch.id, correlationId },
          correlationId
        );
        
        // Increment a metric for the complete pipeline
        await incrementMetric('pipeline_completed', 1, {
          original_batch_type: originalBatchType,
          original_batch_id: originalBatchId
        });
        
        // If this batch should notify on completion (e.g., from client request)
        if (notifyOnCompletion) {
          // TODO: Implement notification logic if needed
          await logSystemEvent('info', 'monitor_pipeline', 
            `Notification requested for completed pipeline starting with batch ${originalBatchId}`, 
            { originalBatchId, correlationId },
            correlationId
          );
        }
      }
    }
  } catch (error) {
    await logSystemEvent('error', 'check_producers_completed', 
      `Error processing producers_completed signal: ${error.message}`, 
      { correlationId, stack: error.stack },
      correlationId
    );
  }
}

// Check if we need to bootstrap the pipeline
async function checkPipelineBootstrap(correlationId: string): Promise<void> {
  try {
    // Check if there are any active discover_artists batches
    const { count: activeDiscoveryCount, error: countError } = await supabase
      .from('processing_batches')
      .select('*', { count: 'exact', head: true })
      .eq('batch_type', 'discover_artists')
      .in('status', ['pending', 'processing']);
    
    // If there are active discovery batches, no need to bootstrap
    if (countError || (activeDiscoveryCount && activeDiscoveryCount > 0)) {
      return;
    }
    
    // Check if we have any active batches at all
    const { count: totalActiveBatches, error: totalError } = await supabase
      .from('processing_batches')
      .select('*', { count: 'exact', head: true })
      .in('status', ['pending', 'processing']);
    
    // If there are any active batches, don't bootstrap yet
    if (totalError || (totalActiveBatches && totalActiveBatches > 0)) {
      return;
    }
    
    // Check when the last discovery batch was created
    const { data: lastDiscovery, error: lastError } = await supabase
      .from('processing_batches')
      .select('created_at')
      .eq('batch_type', 'discover_artists')
      .order('created_at', { ascending: false })
      .limit(1)
      .single();
    
    // If we've discovered in the last day, don't auto-discover again
    if (!lastError && lastDiscovery) {
      const lastDiscoveryDate = new Date(lastDiscovery.created_at);
      const dayAgo = new Date();
      dayAgo.setDate(dayAgo.getDate() - 1);
      
      if (lastDiscoveryDate > dayAgo) {
        return;
      }
    }
    
    await logSystemEvent('info', 'monitor_pipeline', 
      'No active pipelines found, bootstrapping with artist discovery', 
      { correlationId },
      correlationId
    );
    
    // Check if the system can handle a new discovery cycle
    const isAPIRateLimited = await checkCircuitBreaker('spotify');
    if (isAPIRateLimited) {
      await logSystemEvent('warning', 'bootstrap_pipeline', 
        'Cannot bootstrap pipeline - Spotify API is rate limited', 
        { correlationId },
        correlationId
      );
      return;
    }
    
    // Bootstrap by calling discover-artists
    EdgeRuntime.waitUntil(
      supabase.functions.invoke('discover-artists', {
        body: {
          genres: ['pop', 'rock', 'hip hop', 'electronic', 'r&b', 'indie', 'dance'],
          limit: 50,
          minPopularity: 50,
          maxArtistsPerGenre: 10,
          correlationId
        }
      })
    );
    
    await logSystemEvent('info', 'bootstrap_pipeline', 
      'Bootstrapped pipeline with new artist discovery batch', 
      { correlationId },
      correlationId
    );
  } catch (error) {
    await logSystemEvent('error', 'bootstrap_pipeline', 
      `Error bootstrapping pipeline: ${error.message}`, 
      { correlationId, stack: error.stack },
      correlationId
    );
  }
}

// Handle a completed batch - trigger the next stage in the pipeline
async function handleCompletedBatch(
  batchId: string,
  completionStatus: { 
    complete: boolean; 
    total: number; 
    completed: number; 
    failed: number 
  },
  correlationId: string
): Promise<void> {
  try {
    // Get batch details
    const { data: batch, error: batchError } = await supabase
      .from('processing_batches')
      .select('*')
      .eq('id', batchId)
      .single();
    
    if (batchError || !batch) {
      await logSystemEvent('error', 'handle_completed_batch', 
        `Error retrieving batch ${batchId}`, 
        { batchId, error: batchError?.message, correlationId },
        correlationId
      );
      return;
    }
    
    // Handle different batch types
    switch (batch.batch_type) {
      case 'discover_artists':
        // An artist batch is complete, check if it has the artists_completed flag
        if (batch.metadata?.artists_completed) {
          await logSystemEvent('info', 'handle_completed_batch', 
            `Artist discovery batch ${batchId} has completed with artists_completed flag`, 
            { batchId, correlationId },
            correlationId
          );
        }
        break;
        
      case 'process_album_page':
        // Check if this is the last page in a sequence
        if (batch.status === 'completed' && batch.metadata?.is_final_page) {
          await logSystemEvent('info', 'handle_completed_batch', 
            `Album page batch ${batchId} has completed as final page`, 
            { batchId, correlationId },
            correlationId
          );
        } else if (batch.status === 'completed_with_next') {
          // This has already scheduled the next page
          await logSystemEvent('info', 'handle_completed_batch', 
            `Album page batch ${batchId} has completed with next page scheduled`, 
            { batchId, correlationId },
            correlationId
          );
        }
        break;
        
      case 'process_track_page':
        // Check if this is the last page in a sequence
        if (batch.status === 'completed' && batch.metadata?.is_final_page) {
          await logSystemEvent('info', 'handle_completed_batch', 
            `Track page batch ${batchId} has completed as final page`, 
            { batchId, correlationId },
            correlationId
          );
        } else if (batch.status === 'completed_with_next') {
          // This has already scheduled the next page
          await logSystemEvent('info', 'handle_completed_batch', 
            `Track page batch ${batchId} has completed with next page scheduled`, 
            { batchId, correlationId },
            correlationId
          );
        }
        break;
        
      case 'process_producers_batch':
        await logSystemEvent('info', 'handle_completed_batch', 
          `Producer batch ${batchId} has completed`, 
          { batchId, correlationId },
          correlationId
        );
        break;
        
      default:
        // Log that we don't have a specific handler for this batch type
        await logSystemEvent('info', 'handle_completed_batch', 
          `No specific handler for batch type: ${batch.batch_type}`, 
          { batchId, batchType: batch.batch_type, correlationId },
          correlationId
        );
    }
    
    // Mark batch as completed if not already
    if (batch.status !== 'completed' && batch.status !== 'completed_with_next') {
      await supabase
        .from('processing_batches')
        .update({
          status: completionStatus.failed === 0 ? 'completed' : 'partial',
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          items_total: completionStatus.total,
          items_processed: completionStatus.completed,
          items_failed: completionStatus.failed
        })
        .eq('id', batchId);
      
      await logSystemEvent('info', 'handle_completed_batch', 
        `Marked batch ${batchId} as ${completionStatus.failed === 0 ? 'completed' : 'partial'}`, 
        { batchId, batchType: batch.batch_type, correlationId },
        correlationId
      );
    }
    
    // Track metrics
    await incrementMetric('batches_completed', 1, {
      batch_type: batch.batch_type,
      success_rate: completionStatus.total > 0 
        ? completionStatus.completed / completionStatus.total 
        : 0
    });
    
  } catch (error) {
    await logSystemEvent('error', 'handle_completed_batch', 
      `Error handling completed batch ${batchId}: ${error.message}`, 
      { batchId, stack: error.stack, correlationId },
      correlationId
    );
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  try {
    // Parse request (POST body or URL query parameters)
    let request: MonitorRequest = {};
    
    if (req.method === 'POST') {
      try {
        request = await req.json();
      } catch (e) {
        // If parsing fails, use empty request
      }
    } else if (req.method === 'GET') {
      // Parse query parameters
      const url = new URL(req.url);
      request.checkFinalizedBatches = url.searchParams.get('checkFinalizedBatches') !== 'false';
      request.checkForStuckBatches = url.searchParams.get('checkForStuckBatches') !== 'false';
      request.checkDLQThreshold = url.searchParams.get('checkDLQThreshold') !== 'false';
      request.checkErrorRates = url.searchParams.get('checkErrorRates') !== 'false';
      request.checkCircuitBreakers = url.searchParams.get('checkCircuitBreakers') !== 'false';
      
      if (url.searchParams.has('specificBatchId')) {
        request.specificBatchId = url.searchParams.get('specificBatchId') || undefined;
      }
    }
    
    const result = await monitorPipeline(request);
    
    return new Response(
      JSON.stringify(result),
      { 
        headers: { 
          'Content-Type': 'application/json',
          ...corsHeaders 
        } 
      }
    );
  } catch (error) {
    console.error('Error in monitor-pipeline function:', error);
    
    return new Response(
      JSON.stringify({ 
        success: false, 
        message: `Error: ${error.message}`
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
