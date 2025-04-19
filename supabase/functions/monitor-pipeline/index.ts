
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
import { supabase } from "../lib/api-clients.ts";

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

// Configure alert thresholds
const DLQ_ALERT_THRESHOLD = 100; // Alert if DLQ exceeds this size
const ERROR_RATE_ALERT_THRESHOLD = 0.25; // 25% error rate is concerning

interface MonitorRequest {
  checkFinalizedBatches?: boolean;
  checkForStuckBatches?: boolean;
  checkDLQThreshold?: boolean;
  checkErrorRates?: boolean;
  checkCircuitBreakers?: boolean;
  notifyOnCompletion?: boolean;
  specificBatchId?: string;
}

async function monitorPipeline(request: MonitorRequest = {}): Promise<{
  success: boolean;
  message: string;
  alerts?: any[];
  completedBatches?: string[];
  stuckBatches?: string[];
}> {
  // Generate correlation ID for tracing this monitoring run
  const correlationId = generateCorrelationId();
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
    
    // Check for completed batches and trigger next stages
    let completedBatches: string[] = [];
    
    if (request.checkFinalizedBatches !== false) {
      // Either check a specific batch or find recently completed batches
      if (request.specificBatchId) {
        const completionStatus = await checkBatchCompletion(request.specificBatchId);
        
        if (completionStatus.complete) {
          completedBatches.push(request.specificBatchId);
          
          await handleCompletedBatch(
            request.specificBatchId, 
            completionStatus, 
            correlationId
          );
        }
      } else {
        // Check the following types of processing batches:
        const batchTypes = [
          'process_artists',         // Main artist batches
          'process_albums',          // Main album dispatcher batches
          'process_album_page',      // Page-based album processing
          'process_track_page'       // Page-based track processing
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
        
        // Check if any parent batches need to be updated based on their child pages
        await checkPagedBatchesCompletion(correlationId);
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

// Check for album and track page batches that have completed and update their parent batches
async function checkPagedBatchesCompletion(correlationId: string): Promise<void> {
  try {
    // Check for album pages completion - once all pages for an artist are done, we need to create track page batches
    await checkAlbumPagesCompletion(correlationId);
    
    // Check for track pages completion - once all pages for an album are done, we need to update parent album batch
    await checkTrackPagesCompletion(correlationId);
  } catch (error) {
    await logSystemEvent('error', 'check_paged_batches_completion', 
      `Error checking paged batches completion: ${error.message}`, 
      { 
        stack: error.stack,
        correlationId 
      },
      correlationId
    );
  }
}

// Check if all album pages for an artist have completed
async function checkAlbumPagesCompletion(correlationId: string): Promise<void> {
  try {
    // Get any album page batches with final page = true that have been completed
    const { data: finalPages, error: pagesError } = await supabase
      .from('processing_batches')
      .select('id, metadata')
      .eq('batch_type', 'process_album_page')
      .in('status', ['completed', 'completed_with_next'])
      .eq('metadata->is_final_page', true)
      .is('metadata->processed_by_monitor', null); // Only process each one once
    
    if (pagesError || !finalPages || finalPages.length === 0) {
      return;
    }
    
    // For each final page, check all pages for that artist
    for (const page of finalPages) {
      const artistId = page.metadata?.artist_id;
      const parentBatchId = page.metadata?.parent_batch_id;
      
      if (!artistId || !parentBatchId) {
        continue;
      }
      
      // Get all pages for this artist
      const { data: allPages, error: allPagesError } = await supabase
        .from('processing_batches')
        .select('id, status, metadata')
        .eq('batch_type', 'process_album_page')
        .eq('metadata->artist_id', artistId)
        .eq('metadata->parent_batch_id', parentBatchId);
      
      if (allPagesError || !allPages || allPages.length === 0) {
        continue;
      }
      
      // Check if all pages are completed
      const allCompleted = allPages.every(p => 
        p.status === 'completed' || p.status === 'completed_with_next' || p.status === 'error'
      );
      
      if (allCompleted) {
        await logSystemEvent('info', 'check_album_pages_completion',
          `All album pages completed for artist ${artistId}`,
          { artistId, parentBatchId, pageCount: allPages.length, correlationId },
          correlationId
        );
        
        // Mark the final page as processed
        await supabase
          .from('processing_batches')
          .update({
            metadata: {
              ...page.metadata,
              processed_by_monitor: true,
              processed_at: new Date().toISOString()
            }
          })
          .eq('id', page.id);
        
        // Create track page batches for all albums
        await createTrackPageBatches(parentBatchId, artistId, correlationId);
      }
    }
  } catch (error) {
    await logSystemEvent('error', 'check_album_pages_completion', 
      `Error checking album pages completion: ${error.message}`, 
      { stack: error.stack, correlationId },
      correlationId
    );
  }
}

// Check if all track pages for an album have completed
async function checkTrackPagesCompletion(correlationId: string): Promise<void> {
  try {
    // Get any track page batches with final page = true that have been completed
    const { data: finalPages, error: pagesError } = await supabase
      .from('processing_batches')
      .select('id, metadata')
      .eq('batch_type', 'process_track_page')
      .in('status', ['completed', 'completed_with_next'])
      .eq('metadata->is_final_page', true)
      .is('metadata->processed_by_monitor', null); // Only process each one once
    
    if (pagesError || !finalPages || finalPages.length === 0) {
      return;
    }
    
    // For each final page, check all pages for that album
    for (const page of finalPages) {
      const albumId = page.metadata?.album_id;
      const parentBatchId = page.metadata?.parent_album_batch_id;
      
      if (!albumId || !parentBatchId) {
        continue;
      }
      
      // Get all pages for this album
      const { data: allPages, error: allPagesError } = await supabase
        .from('processing_batches')
        .select('id, status, metadata')
        .eq('batch_type', 'process_track_page')
        .eq('metadata->album_id', albumId)
        .eq('metadata->parent_album_batch_id', parentBatchId);
      
      if (allPagesError || !allPages || allPages.length === 0) {
        continue;
      }
      
      // Check if all pages are completed
      const allCompleted = allPages.every(p => 
        p.status === 'completed' || p.status === 'completed_with_next' || p.status === 'error'
      );
      
      if (allCompleted) {
        await logSystemEvent('info', 'check_track_pages_completion',
          `All track pages completed for album ${albumId}`,
          { albumId, parentBatchId, pageCount: allPages.length, correlationId },
          correlationId
        );
        
        // Mark the final page as processed
        await supabase
          .from('processing_batches')
          .update({
            metadata: {
              ...page.metadata,
              processed_by_monitor: true,
              processed_at: new Date().toISOString()
            }
          })
          .eq('id', page.id);
        
        // Update album completion status in parent batch
        await updateAlbumCompletionStatus(parentBatchId, albumId, correlationId);
      }
    }
  } catch (error) {
    await logSystemEvent('error', 'check_track_pages_completion', 
      `Error checking track pages completion: ${error.message}`, 
      { stack: error.stack, correlationId },
      correlationId
    );
  }
}

// Create track page batches for each album of an artist
async function createTrackPageBatches(
  parentArtistBatchId: string,
  artistId: string,
  correlationId: string
): Promise<void> {
  try {
    // Get all primary albums for this artist
    const { data: albums, error: albumsError } = await supabase
      .from('albums')
      .select('id, name, spotify_id, total_tracks')
      .eq('artist_id', artistId)
      .eq('is_primary_artist_album', true);
    
    if (albumsError) {
      throw new Error(`Error fetching albums: ${albumsError.message}`);
    }

    if (!albums || albums.length === 0) {
      await logSystemEvent('info', 'create_track_page_batches',
        `No albums found for artist ${artistId}`,
        { parentArtistBatchId, artistId, correlationId },
        correlationId
      );
      return;
    }

    await logSystemEvent('info', 'create_track_page_batches',
      `Creating track page batches for ${albums.length} albums`,
      { parentArtistBatchId, artistId, albumCount: albums.length, correlationId },
      correlationId
    );

    // Create a parent batch for all track processing for this artist
    const { data: trackParentBatch, error: parentBatchError } = await supabase
      .from('processing_batches')
      .insert({
        batch_type: 'process_tracks',
        status: 'pending',
        metadata: {
          artist_id: artistId,
          parent_batch_id: parentArtistBatchId,
          album_count: albums.length,
          source: 'monitor_pipeline',
          correlation_id: correlationId
        }
      })
      .select('id')
      .single();

    if (parentBatchError || !trackParentBatch) {
      throw new Error(`Error creating parent track batch: ${parentBatchError?.message || 'No data returned'}`);
    }

    // Constants for pagination
    const PAGE_SIZE = 50; // Tracks per page
    
    // Create a track page batch for each album
    for (const album of albums) {
      if (!album.spotify_id) {
        continue; // Skip albums without a Spotify ID
      }

      const totalTracks = album.total_tracks || 1;
      const pages = Math.ceil(totalTracks / PAGE_SIZE);

      // Create the first page batch
      const { data: firstPageBatch, error: firstPageError } = await supabase
        .from('processing_batches')
        .insert({
          batch_type: 'process_track_page',
          status: 'pending',
          metadata: {
            album_id: album.id,
            album_name: album.name,
            artist_id: artistId,
            parent_album_batch_id: trackParentBatch.id,
            offset: 0,
            limit: PAGE_SIZE,
            page_index: 0,
            total_pages: pages,
            spotify_id: album.spotify_id,
            correlation_id: correlationId
          }
        })
        .select('id')
        .single();

      if (firstPageError || !firstPageBatch) {
        await logSystemEvent('error', 'create_track_page_batches',
          `Error creating track page batch for album ${album.name}: ${firstPageError?.message || 'No data returned'}`,
          { albumId: album.id, albumName: album.name, correlationId },
          correlationId
        );
        continue;
      }

      // Create an album item in the parent batch
      await supabase
        .from('processing_items')
        .insert({
          batch_id: trackParentBatch.id,
          item_type: 'album_for_tracks',
          item_id: album.id,
          status: 'pending',
          priority: 5,
          metadata: {
            album_name: album.name,
            artist_id: artistId,
            total_tracks: totalTracks,
            total_pages: pages,
            correlation_id: correlationId
          }
        });

      // Call the track page processing function for the first page
      // Do this asynchronously to not block this loop
      edgeRuntime.waitUntil(
        supabase.functions.invoke("process-track-page", {
          body: {
            batchId: firstPageBatch.id,
            albumId: album.id,
            offset: 0,
            limit: PAGE_SIZE,
            correlationId,
            parentAlbumBatchId: trackParentBatch.id
          }
        })
      );

      await logSystemEvent('info', 'create_track_page_batches',
        `Created track page batch for album ${album.name}`,
        { 
          albumId: album.id, 
          albumName: album.name, 
          batchId: firstPageBatch.id,
          totalPages: pages,
          correlationId 
        },
        correlationId
      );
    }

    // Update the parent artist batch as completed
    await supabase
      .from('processing_batches')
      .update({
        status: 'completed',
        completed_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        metadata: {
          albums_completed: true,
          tracks_batch_id: trackParentBatch.id,
          albums_completed_at: new Date().toISOString(),
          correlation_id: correlationId
        }
      })
      .eq('id', parentArtistBatchId);

    await incrementMetric('track_page_batches_created', albums.length, {
      parent_batch_id: parentArtistBatchId,
      track_parent_batch_id: trackParentBatch.id
    });
  } catch (error) {
    await logSystemEvent('error', 'create_track_page_batches',
      `Error creating track page batches: ${error.message}`,
      { parentArtistBatchId, artistId, stack: error.stack, correlationId },
      correlationId
    );
  }
}

// Update the album completion status in parent batch
async function updateAlbumCompletionStatus(
  parentBatchId: string,
  albumId: string,
  correlationId: string
): Promise<void> {
  try {
    // Update the album item in parent batch as completed
    const { data: albumItems, error: itemsError } = await supabase
      .from('processing_items')
      .select('id')
      .eq('batch_id', parentBatchId)
      .eq('item_id', albumId)
      .eq('item_type', 'album_for_tracks');
    
    if (itemsError || !albumItems || albumItems.length === 0) {
      await logSystemEvent('warning', 'update_album_completion_status',
        `No album item found for album ${albumId} in batch ${parentBatchId}`,
        { parentBatchId, albumId, correlationId },
        correlationId
      );
      return;
    }
    
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
    
    // Check if all albums in the batch are now completed
    const { data: allItems, error: allItemsError } = await supabase
      .from('processing_items')
      .select('id, status')
      .eq('batch_id', parentBatchId);
    
    if (allItemsError || !allItems || allItems.length === 0) {
      return;
    }
    
    const allItemsCompleted = allItems.every(item => 
      item.status === 'completed' || item.status === 'error'
    );
    
    if (allItemsCompleted) {
      // All albums are completed, mark the parent batch as completed
      await supabase
        .from('processing_batches')
        .update({
          status: 'tracks_completed',
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          metadata: {
            tracks_completed_at: new Date().toISOString(),
            tracks_correlation_id: correlationId
          }
        })
        .eq('id', parentBatchId);
      
      await logSystemEvent('info', 'update_album_completion_status',
        `Marked parent batch ${parentBatchId} as tracks_completed`,
        { parentBatchId, correlationId },
        correlationId
      );
      
      await incrementMetric('track_batches_completed', 1, {
        parent_batch_id: parentBatchId,
        album_count: allItems.length
      });
    }
  } catch (error) {
    await logSystemEvent('error', 'update_album_completion_status',
      `Error updating album completion status: ${error.message}`,
      { parentBatchId, albumId, stack: error.stack, correlationId },
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
      case 'process_artists':
        // An artist batch is complete, trigger the album page batches
        await handleCompletedArtistBatch(batch, completionStatus, correlationId);
        break;
        
      case 'process_albums':
        // The album dispatcher has created all necessary album page batches
        await logSystemEvent('info', 'handle_completed_batch', 
          `Album dispatcher batch ${batchId} has completed`, 
          { batchId, correlationId },
          correlationId
        );
        break;
        
      case 'process_album_page':
        // A single album page has completed, monitor-pipeline handles orchestration
        await logSystemEvent('info', 'handle_completed_batch', 
          `Album page batch ${batchId} has completed`, 
          { batchId, correlationId },
          correlationId
        );
        break;
        
      case 'process_track_page':
        // A single track page has completed, monitor-pipeline handles orchestration
        await logSystemEvent('info', 'handle_completed_batch', 
          `Track page batch ${batchId} has completed`, 
          { batchId, correlationId },
          correlationId
        );
        break;
        
      default:
        // Log that we don't have a handler for this batch type
        await logSystemEvent('info', 'handle_completed_batch', 
          `No specific handler for batch type: ${batch.batch_type}`, 
          { batchId, batchType: batch.batch_type, correlationId },
          correlationId
        );
    }
    
    // Mark batch as completed if not already
    if (batch.status !== 'completed' && batch.status !== 'completed_with_next' && batch.status !== 'tracks_completed') {
      await supabase
        .from('processing_batches')
        .update({
          status: completionStatus.failed === 0 ? 'completed' : 'partial',
          completed_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
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

// Handle completed artist batch - trigger album page processing
async function handleCompletedArtistBatch(
  batch: any,
  completionStatus: { 
    complete: boolean; 
    total: number; 
    completed: number; 
    failed: number 
  },
  correlationId: string
): Promise<void> {
  try {
    // Find all the artists that were processed successfully
    const { data: processedArtists, error: artistsError } = await supabase
      .from('processing_items')
      .select('item_id, item_type')
      .eq('batch_id', batch.id)
      .eq('status', 'completed')
      .eq('item_type', 'artist');
    
    if (artistsError || !processedArtists || processedArtists.length === 0) {
      await logSystemEvent('warning', 'handle_artist_batch', 
        `No successfully processed artists found in batch ${batch.id}`, 
        { batchId: batch.id, error: artistsError?.message, correlationId },
        correlationId
      );
      return;
    }
    
    // For each artist, trigger album page processing
    for (const artist of processedArtists) {
      // Get artist details
      const { data: artistData, error: artistError } = await supabase
        .from('artists')
        .select('id, name, spotify_id')
        .eq('id', artist.item_id)
        .single();
      
      if (artistError || !artistData) {
        await logSystemEvent('warning', 'handle_artist_batch', 
          `Could not find artist ${artist.item_id}`, 
          { artistId: artist.item_id, error: artistError?.message, correlationId },
          correlationId
        );
        continue;
      }
      
      // Create a new album dispatcher batch for this artist
      const { data: albumBatch, error: batchError } = await supabase
        .from('processing_batches')
        .insert({
          batch_type: 'process_albums',
          status: 'pending',
          metadata: {
            artist_id: artistData.id,
            artist_name: artistData.name,
            spotify_id: artistData.spotify_id,
            parent_batch_id: batch.id,
            source: 'pipeline',
            created_at: new Date().toISOString(),
            correlation_id: correlationId
          }
        })
        .select('id')
        .single();
      
      if (batchError || !albumBatch) {
        await logSystemEvent('error', 'handle_artist_batch', 
          `Error creating album batch for artist ${artistData.name}`, 
          { artistId: artistData.id, error: batchError?.message, correlationId },
          correlationId
        );
        continue;
      }
      
      // Add artist to the batch
      const { error: itemError } = await supabase
        .from('processing_items')
        .insert({
          batch_id: albumBatch.id,
          item_type: 'artist',
          item_id: artistData.id,
          status: 'pending',
          priority: 5,
          metadata: {
            artist_name: artistData.name,
            spotify_id: artistData.spotify_id,
            source: 'pipeline',
            parent_batch_id: batch.id,
            correlation_id: correlationId
          }
        });
      
      if (itemError) {
        await logSystemEvent('error', 'handle_artist_batch', 
          `Error adding artist to album batch: ${itemError.message}`, 
          { artistId: artistData.id, batchId: albumBatch.id, correlationId },
          correlationId
        );
        continue;
      }
      
      await logSystemEvent('info', 'handle_artist_batch', 
        `Created album batch ${albumBatch.id} for artist ${artistData.name}`, 
        { artistId: artistData.id, batchId: albumBatch.id, correlationId },
        correlationId
      );
      
      // Call the album processing edge function asynchronously
      edgeRuntime.waitUntil(
        supabase.functions.invoke('process-album-dispatcher', {
          body: {
            batchId: albumBatch.id,
            correlationId
          }
        })
      );
      
      await incrementMetric('album_batches_created', 1, {
        artist_id: artistData.id,
        artist_name: artistData.name
      });
    }
  } catch (error) {
    await logSystemEvent('error', 'handle_artist_batch', 
      `Error handling artist batch ${batch.id}: ${error.message}`, 
      { batchId: batch.id, stack: error.stack, correlationId },
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
