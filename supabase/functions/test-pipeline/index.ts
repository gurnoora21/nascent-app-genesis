
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { supabase } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Define the pipeline steps for better clarity
enum PipelineStep {
  DISCOVER_ARTISTS = 1,
  PROCESS_ARTISTS = 2,
  COLLECT_TRACKS = 3,
  IDENTIFY_PRODUCERS = 4
}

// Test function to execute a specific pipeline step
async function testPipelineStep(step: PipelineStep, options: any = {}): Promise<{
  success: boolean;
  message: string;
  data?: any;
}> {
  try {
    console.log(`Testing pipeline step ${step}: ${PipelineStep[step]}`);
    
    switch (step) {
      case PipelineStep.DISCOVER_ARTISTS: {
        // Step 1: Discover Artists (with limited genres and count)
        const genres = options.genres || ["r&b", "rap", "hip hop", "pop"];
        const limit = options.limit || 30;
        const minPopularity = options.minPopularity || 50;
        const maxArtistsPerGenre = options.maxArtistsPerGenre || 10;
        
        console.log(`Discovering artists with genres: ${genres.join(", ")}, limit: ${limit}`);
        
        const response = await fetch(
          `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/discover-artists`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
            },
            body: JSON.stringify({
              genres,
              limit,
              minPopularity,
              maxArtistsPerGenre
            })
          }
        );
        
        const result = await response.json();
        return {
          success: result.success,
          message: `Discover artists step completed. Batch ID: ${result.batchId}`,
          data: result
        };
      }
      
      case PipelineStep.PROCESS_ARTISTS: {
        // Step 2: Process Artists
        const batchId = options.batchId;
        
        if (!batchId) {
          // If no batch ID provided, find the latest discover_artists batch
          const { data: latestBatch, error: batchError } = await supabase
            .from("processing_batches")
            .select("id")
            .eq("batch_type", "discover_artists")
            .eq("status", "completed")
            .order("created_at", { ascending: false })
            .limit(1)
            .single();
            
          if (batchError) {
            throw new Error(`No completed discover_artists batch found: ${batchError.message}`);
          }
          
          options.batchId = latestBatch.id;
        }
        
        console.log(`Processing artists from batch ID: ${options.batchId}`);
        
        const response = await fetch(
          `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/process-batch`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
            },
            body: JSON.stringify({
              batchType: "process_artists"
            })
          }
        );
        
        const result = await response.json();
        return {
          success: result.success,
          message: `Process artists step completed. ${result.message}`,
          data: result
        };
      }
      
      case PipelineStep.COLLECT_TRACKS: {
        // Step 3: Collect Tracks
        console.log(`Starting track collection process`);
        
        const response = await fetch(
          `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/process-batch`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
            },
            body: JSON.stringify({
              batchType: "collect_tracks"
            })
          }
        );
        
        const result = await response.json();
        return {
          success: result.success,
          message: `Collect tracks step completed. ${result.message}`,
          data: result
        };
      }
      
      case PipelineStep.IDENTIFY_PRODUCERS: {
        // Step 4: Identify Producers
        const limit = options.limit || 10;
        console.log(`Starting producer identification process with limit: ${limit}`);
        
        const response = await fetch(
          `https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/process-tracks-batch`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")}`
            },
            body: JSON.stringify({
              action: "create",
              limit
            })
          }
        );
        
        const result = await response.json();
        return {
          success: result.success,
          message: `Created producer identification batch. ${result.message}`,
          data: result
        };
      }
      
      default:
        return {
          success: false,
          message: `Unknown pipeline step: ${step}`
        };
    }
  } catch (error) {
    console.error(`Error testing pipeline step ${step}:`, error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "testing",
        p_source: "test_pipeline",
        p_message: `Error testing pipeline step ${step}`,
        p_stack_trace: error.stack || "",
        p_context: { step, options },
      });
    } catch (logError) {
      console.error("Error logging to database:", logError);
    }
    
    return {
      success: false,
      message: `Error testing pipeline step ${step}: ${error.message}`
    };
  }
}

// Main handler function
serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Parse request body
    const { step, options = {} } = await req.json();
    
    if (!step || isNaN(Number(step))) {
      return new Response(
        JSON.stringify({
          success: false,
          message: "Invalid step. Please provide a step number (1-4).",
          validSteps: {
            "1": "DISCOVER_ARTISTS",
            "2": "PROCESS_ARTISTS",
            "3": "COLLECT_TRACKS",
            "4": "IDENTIFY_PRODUCERS"
          }
        }),
        {
          status: 400,
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
    }
    
    const pipelineStep = Number(step) as PipelineStep;
    const result = await testPipelineStep(pipelineStep, options);
    
    return new Response(
      JSON.stringify(result),
      {
        status: result.success ? 200 : 500,
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  } catch (error) {
    console.error("Error handling test-pipeline request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "test_pipeline",
        p_message: "Error handling test-pipeline request",
        p_stack_trace: error.stack || "",
        p_context: { error: error.message },
      });
    } catch (logError) {
      console.error("Error logging to database:", logError);
    }

    return new Response(
      JSON.stringify({
        success: false,
        error: error.message,
      }),
      {
        status: 500,
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  }
});

// Function to get the status of all batch types
async function getPipelineStatus(): Promise<any> {
  const batchTypes = ["discover_artists", "process_artists", "collect_tracks", "identify_producers"];
  const result: Record<string, any> = {};
  
  for (const batchType of batchTypes) {
    const { data, error } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("batch_type", batchType)
      .order("created_at", { ascending: false })
      .limit(1);
      
    if (!error && data.length > 0) {
      result[batchType] = data[0];
    } else {
      result[batchType] = null;
    }
  }
  
  return result;
}
