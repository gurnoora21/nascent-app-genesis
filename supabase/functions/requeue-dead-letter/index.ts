
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { requeueDeadLetterItem, incrementMetric, logSystemEvent } from "../lib/pipeline-utils.ts";

// CORS headers for browser requests
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  try {
    if (req.method !== 'POST') {
      return new Response(
        JSON.stringify({ error: 'Method not allowed' }),
        { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const requestId = crypto.randomUUID();
    await logSystemEvent('info', 'requeue-dead-letter', 
      `Received request to requeue dead letter item`, { requestId });

    // Parse request
    const requestData = await req.json();
    const { deadLetterId } = requestData;

    if (!deadLetterId) {
      return new Response(
        JSON.stringify({ error: 'Missing required parameter: deadLetterId' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Process the request
    const newItemId = await requeueDeadLetterItem(deadLetterId);
    
    if (!newItemId) {
      await logSystemEvent('error', 'requeue-dead-letter', 
        `Failed to requeue dead letter item`, { deadLetterId, requestId });
      
      return new Response(
        JSON.stringify({ error: 'Failed to requeue dead letter item' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Track metrics
    await incrementMetric('dead_letter_items_requeued', 1, { deadLetterId });
    await logSystemEvent('info', 'requeue-dead-letter', 
      `Successfully requeued dead letter item`, { deadLetterId, newItemId, requestId });

    return new Response(
      JSON.stringify({ 
        success: true, 
        message: 'Item requeued successfully',
        newItemId 
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error('Error in requeue-dead-letter function:', error);
    
    await logSystemEvent('error', 'requeue-dead-letter', 
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
