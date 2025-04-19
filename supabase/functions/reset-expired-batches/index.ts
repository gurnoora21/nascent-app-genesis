
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { resetExpiredBatches, incrementMetric, logSystemEvent, trackErrorRates, checkBackpressure } from "../lib/pipeline-utils.ts";

// CORS headers for browser requests
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  try {
    const requestId = crypto.randomUUID();
    await logSystemEvent('info', 'reset-expired-batches', 
      `Starting reset of expired batches`, { requestId });

    const startTime = Date.now();
    
    // Parse request body for advanced options
    let includeOnlyBatchTypes: string[] | undefined = undefined;
    let olderThanMinutes: number | undefined = undefined;
    
    if (req.method === 'POST') {
      try {
        const body = await req.json();
        includeOnlyBatchTypes = body.includeOnlyBatchTypes;
        olderThanMinutes = body.olderThanMinutes;
      } catch (e) {
        // If parsing fails, use default options
      }
    }
    
    // Reset expired batches
    const resetCount = await resetExpiredBatches();
    const duration = Date.now() - startTime;

    // Check for high error rates across batch types
    const batchTypes = [
      'process_artists', 
      'process_albums', 
      'process_tracks',
      'process_album_page',
      'process_track_page'
    ];
    
    let highErrorRateFound = false;
    
    for (const batchType of batchTypes) {
      const { errorRate, alertThresholdExceeded } = await trackErrorRates(batchType);
      
      if (alertThresholdExceeded) {
        highErrorRateFound = true;
        
        await logSystemEvent('warning', 'reset-expired-batches', 
          `High error rate detected for ${batchType}: ${(errorRate * 100).toFixed(1)}%`, 
          { batchType, errorRate, requestId }
        );
      }
    }
    
    // Check for backpressure
    const backpressureTables = ['album', 'track', 'artist'];
    let backpressureDetected = false;
    
    for (const table of backpressureTables) {
      const isUnderPressure = await checkBackpressure(table);
      
      if (isUnderPressure) {
        backpressureDetected = true;
        
        await logSystemEvent('warning', 'reset-expired-batches', 
          `Backpressure detected for ${table}`, 
          { table, requestId }
        );
      }
    }
    
    // Track metrics
    await incrementMetric('expired_batches_reset', resetCount, { 
      requestId,
      duration_ms: duration,
      high_error_rate: highErrorRateFound,
      backpressure: backpressureDetected
    });
    
    await logSystemEvent('info', 'reset-expired-batches', 
      `Reset ${resetCount} expired batches in ${duration}ms`, { 
        requestId, 
        resetCount, 
        duration_ms: duration,
        high_error_rate: highErrorRateFound,
        backpressure: backpressureDetected
      });

    // Call the monitor-pipeline function to check for completed batches
    // We do this asynchronously to avoid blocking the response
    edgeRuntime.waitUntil(
      fetch('https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/monitor-pipeline', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${Deno.env.get('SUPABASE_ANON_KEY')}`
        },
        body: JSON.stringify({
          checkFinalizedBatches: true,
          checkForStuckBatches: true
        })
      })
    );

    return new Response(
      JSON.stringify({ 
        success: true, 
        batchesReset: resetCount,
        duration_ms: duration,
        highErrorRateFound,
        backpressureDetected
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error('Error in reset-expired-batches function:', error);
    
    await logSystemEvent('error', 'reset-expired-batches', 
      `Exception in function: ${error.message}`, { 
        stack: error.stack,
        error: error.toString()
      });

    return new Response(
      JSON.stringify({ 
        error: 'Internal server error', 
        message: error.message 
      }),
      { 
        status: 500, 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
      }
    );
  }
});
