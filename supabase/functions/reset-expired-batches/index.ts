
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { resetExpiredBatches, incrementMetric, logSystemEvent } from "../lib/pipeline-utils.ts";

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
    const resetCount = await resetExpiredBatches();
    const duration = Date.now() - startTime;

    // Track metrics
    await incrementMetric('expired_batches_reset', resetCount, { 
      requestId,
      duration_ms: duration
    });
    
    await logSystemEvent('info', 'reset-expired-batches', 
      `Reset ${resetCount} expired batches in ${duration}ms`, { 
        requestId, 
        resetCount, 
        duration_ms: duration 
      });

    return new Response(
      JSON.stringify({ 
        success: true, 
        batchesReset: resetCount,
        duration_ms: duration
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
