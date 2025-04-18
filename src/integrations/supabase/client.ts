
// This file is automatically generated. Do not edit it directly.
import { createClient } from '@supabase/supabase-js';
import type { Database } from './types';

const SUPABASE_URL = "https://nsxxzhhbcwzatvlulfyp.supabase.co";
const SUPABASE_PUBLISHABLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5zeHh6aGhiY3d6YXR2bHVsZnlwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ4NDQ4NDYsImV4cCI6MjA2MDQyMDg0Nn0.CR3TFPYipFCs6sL_51rJ3kOKR3iQGr8tJgZJ2GLlrDk";

// Import the supabase client like this:
// import { supabase } from "@/integrations/supabase/client";

export const supabase = createClient<Database>(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY);

// Format function for Spotify release dates
export function formatSpotifyReleaseDate(releaseDate: string | null): string | null {
  if (!releaseDate) return null;
  
  const dateParts = releaseDate.split('-');
  
  if (dateParts.length === 1 && dateParts[0].length === 4) {
    // Year only format (e.g., "2012") - append "-01-01" to make it January 1st
    return `${releaseDate}-01-01`;
  } else if (dateParts.length === 2) {
    // Year-month format (e.g., "2012-03") - append "-01" to make it the 1st day of the month
    return `${releaseDate}-01`;
  } else if (dateParts.length === 3) {
    // Already full date format, return as is
    return releaseDate;
  } else {
    // Unknown format, log it and return null
    console.error(`Unknown release date format: ${releaseDate}`);
    return null;
  }
}

// Process an artist through the batch system
export async function processArtist(artistId: string, isTestMode = false): Promise<{
  success: boolean;
  message: string;
  batchId?: string;
}> {
  try {
    // Create a new processing batch
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "process_artists",
        status: "pending",
        metadata: {
          is_test_mode: isTestMode,
          created_from_client: true,
          created_at: new Date().toISOString(),
          source: "client_request" 
        }
      })
      .select("id")
      .single();
    
    if (batchError) {
      console.error('Error creating processing batch:', batchError);
      return {
        success: false,
        message: `Error: ${batchError.message}`,
      };
    }
    
    // Add the artist to the batch
    const { error: itemError } = await supabase
      .from("processing_items")
      .insert({
        batch_id: batch.id,
        item_type: "artist",
        item_id: artistId,
        status: "pending",
        priority: 10, // High priority for manual requests
        metadata: {
          is_test_mode: isTestMode,
          requested_at: new Date().toISOString(),
          source: "client_request"
        }
      });
    
    if (itemError) {
      console.error('Error adding artist to batch:', itemError);
      return {
        success: false,
        message: `Error: ${itemError.message}`,
      };
    }
    
    // Trigger the processing function
    const { data, error } = await supabase.functions.invoke("process-artists-batch", {
      body: {
        notifyOnCompletion: true,
        clientRequestedBatchId: batch.id
      },
    });
    
    if (error) {
      console.error('Error calling process-artists-batch function:', error);
      return {
        success: false,
        message: `Error: ${error.message}`,
      };
    }
    
    return {
      success: true,
      message: `Artist processing initiated. Batch ID: ${batch.id}`,
      batchId: batch.id
    };
  } catch (error) {
    console.error('Error in processArtist:', error);
    return {
      success: false,
      message: `Exception: ${error instanceof Error ? error.message : 'Unknown error'}`,
    };
  }
}

// Get batch status - new function for checking batch progress
export async function getBatchStatus(batchId: string): Promise<{
  success: boolean;
  status?: string;
  progress?: {
    total: number;
    processed: number;
    failed: number;
    percentComplete: number;
  };
  message?: string;
}> {
  try {
    const { data: batch, error } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", batchId)
      .single();
    
    if (error) {
      return {
        success: false,
        message: `Error retrieving batch: ${error.message}`
      };
    }
    
    if (!batch) {
      return {
        success: false,
        message: `Batch ${batchId} not found`
      };
    }
    
    const total = batch.items_total || 0;
    const processed = batch.items_processed || 0;
    const failed = batch.items_failed || 0;
    const percentComplete = total > 0 ? Math.round((processed / total) * 100) : 0;
    
    return {
      success: true,
      status: batch.status,
      progress: {
        total,
        processed,
        failed,
        percentComplete
      }
    };
  } catch (error) {
    return {
      success: false,
      message: `Exception: ${error instanceof Error ? error.message : 'Unknown error'}`
    };
  }
}

// Get dead letter queue items - new function for viewing failed items
export async function getDeadLetterItems(
  limit = 50, 
  offset = 0, 
  itemType?: string
): Promise<{
  success: boolean;
  items?: any[];
  count?: number;
  message?: string;
}> {
  try {
    let query = supabase
      .from("dead_letter_items")
      .select("*", { count: "exact" });
    
    if (itemType) {
      query = query.eq("item_type", itemType);
    }
    
    const { data, count, error } = await query
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);
    
    if (error) {
      return {
        success: false,
        message: `Error retrieving dead letter items: ${error.message}`
      };
    }
    
    return {
      success: true,
      items: data,
      count: count || 0
    };
  } catch (error) {
    return {
      success: false,
      message: `Exception: ${error instanceof Error ? error.message : 'Unknown error'}`
    };
  }
}

// Requeue a dead letter item - new function for retrying failed items
export async function requeueDeadLetterItem(deadLetterId: string): Promise<{
  success: boolean;
  message: string;
  batchId?: string;
}> {
  try {
    const { data, error } = await supabase.functions.invoke("requeue-dead-letter", {
      body: {
        deadLetterId
      },
    });
    
    if (error) {
      return {
        success: false,
        message: `Error requeuing item: ${error.message}`
      };
    }
    
    return {
      success: true,
      message: `Item requeued successfully`,
      batchId: data?.batchId
    };
  } catch (error) {
    return {
      success: false,
      message: `Exception: ${error instanceof Error ? error.message : 'Unknown error'}`
    };
  }
}
