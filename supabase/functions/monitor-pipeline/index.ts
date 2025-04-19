
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
    
    // Check for completed parent batches and trigger next stages
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
        // Find "processing" batches that might be complete by looking at child batches
        const { data: parentBatches, error: parentError } = await supabase
          .from('processing_batches')
          .select('id, batch_type')
          .in('status', ['processing'])
          .is('completed_at', null)
          .order('created_at', { ascending: true })
          .limit(10);  // Process in manageable chunks
        
        if (!parentError && parentBatches) {
          for (const batch of parentBatches) {
            // Skip non-parent batch types
            if (batch.batch_type === 'process_album_page' || 
                batch.batch_type === 'process_track_page') {
              continue;
            }
            
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
        // An album batch is complete, trigger the track page batches
        await handleCompletedAlbumBatch(batch, completionStatus, correlationId);
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
    if (batch.status !== 'completed') {
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
      
      // Create a new album batch for this artist
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
        supabase.functions.invoke('process-albums-batch', {
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

// Handle completed album batch - trigger track page processing
async function handleCompletedAlbumBatch(
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
    // Find all successfully processed albums
    const { data: processedAlbums, error: albumsError } = await supabase
      .from('processing_items')
      .select('item_id, item_type, metadata')
      .eq('batch_id', batch.id)
      .eq('status', 'completed')
      .in('item_type', ['album', 'album_for_tracks']);
    
    if (albumsError || !processedAlbums || processedAlbums.length === 0) {
      await logSystemEvent('warning', 'handle_album_batch', 
        `No successfully processed albums found in batch ${batch.id}`, 
        { batchId: batch.id, error: albumsError?.message, correlationId },
        correlationId
      );
      return;
    }
    
    // Create a tracks batch
    const { data: tracksBatch, error: batchError } = await supabase
      .from('processing_batches')
      .insert({
        batch_type: 'process_tracks',
        status: 'pending',
        metadata: {
          parent_batch_id: batch.id,
          album_count: processedAlbums.length,
          source: 'pipeline',
          created_at: new Date().toISOString(),
          correlation_id: correlationId
        }
      })
      .select('id')
      .single();
    
    if (batchError || !tracksBatch) {
      await logSystemEvent('error', 'handle_album_batch', 
        `Error creating tracks batch: ${batchError?.message}`, 
        { batchId: batch.id, correlationId },
        correlationId
      );
      return;
    }
    
    // Add each album to the tracks batch
    const batchItems = processedAlbums.map(album => {
      return {
        batch_id: tracksBatch.id,
        item_type: 'album_for_tracks',
        item_id: album.item_id,
        status: 'pending',
        priority: 5,
        metadata: {
          parent_batch_id: batch.id,
          source: 'pipeline',
          correlation_id: correlationId,
          artist_id: album.metadata?.artist_id
        }
      };
    });
    
    const { error: itemsError } = await supabase
      .from('processing_items')
      .insert(batchItems);
    
    if (itemsError) {
      await logSystemEvent('error', 'handle_album_batch', 
        `Error adding albums to tracks batch: ${itemsError.message}`, 
        { batchId: tracksBatch.id, albumCount: processedAlbums.length, correlationId },
        correlationId
      );
      return;
    }
    
    // Update the batch with the correct totals
    await supabase
      .from('processing_batches')
      .update({
        items_total: processedAlbums.length
      })
      .eq('id', tracksBatch.id);
    
    await logSystemEvent('info', 'handle_album_batch', 
      `Created tracks batch ${tracksBatch.id} with ${processedAlbums.length} albums`, 
      { batchId: tracksBatch.id, albumCount: processedAlbums.length, correlationId },
      correlationId
    );
    
    // Call the tracks processing edge function asynchronously  
    edgeRuntime.waitUntil(
      supabase.functions.invoke('process-tracks-batch', {
        body: {
          batchId: tracksBatch.id,
          correlationId
        }
      })
    );
    
    await incrementMetric('track_batches_created', 1, {
      album_count: processedAlbums.length
    });
  } catch (error) {
    await logSystemEvent('error', 'handle_album_batch', 
      `Error handling album batch ${batch.id}: ${error.message}`, 
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
