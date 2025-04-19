import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { SpotifyClient, supabase } from "../lib/api-clients.ts";
import { 
  logSystemEvent, 
  logProcessingError,
  updateItemStatus 
} from "../lib/pipeline-utils.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Define target markets
const TARGET_MARKETS = [
  // North America
  'US', 'CA',
  // Europe
  'GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'SE', 'NO', 'DK', 
  'FI', 'IE', 'PT', 'AT', 'CH', 'PL', 'CZ', 'HU', 'SK', 'GR'
];

interface DiscoverArtistsOptions {
  genres?: string[];
  limit?: number;
  minPopularity?: number;
  maxArtistsPerGenre?: number;
}

// Create a new batch for discovering artists
async function createDiscoveryBatch(options: DiscoverArtistsOptions): Promise<string> {
  try {
    const { data, error } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "discover_artists",
        status: "pending",
        metadata: options,
      })
      .select("id")
      .single();

    if (error) throw error;
    await logSystemEvent(
      "info", 
      "discover_artists", 
      `Created discovery batch for genres: ${options.genres?.join(", ")}`,
      { options }
    );
    return data.id;
  } catch (error) {
    console.error("Error creating discovery batch:", error);
    await logProcessingError(
      "batch_creation",
      "discover_artists",
      "Error creating discovery batch",
      error,
      { options }
    );
    throw error;
  }
}

// Add artist to a batch for processing
async function addArtistToBatch(
  batchId: string, 
  spotifyId: string, 
  priority: number = 5
): Promise<void> {
  try {
    // First check if an item for this artist already exists in this batch
    const { data: existingItem, error: checkError } = await supabase
      .from("processing_items")
      .select("id")
      .eq("batch_id", batchId)
      .eq("item_id", spotifyId)
      .eq("item_type", "artist")
      .maybeSingle();
    
    if (checkError && checkError.code !== "PGRST116") {
      throw checkError;
    }
    
    // Only insert if it doesn't exist
    if (!existingItem) {
      const { error } = await supabase
        .from("processing_items")
        .insert({
          batch_id: batchId,
          item_type: "artist",
          item_id: spotifyId,
          status: "pending",
          priority,
        });

      if (error) {
        console.error(`Error adding artist ${spotifyId} to batch:`, error);
        throw error;
      }
    }
  } catch (error) {
    console.error(`Error adding artist ${spotifyId} to batch:`, error);
    await logProcessingError(
      "item_creation",
      "discover_artists",
      `Error adding artist ${spotifyId} to batch`,
      error,
      { batchId, spotifyId, priority }
    );
    throw error;
  }
}

// Process genre to discover artists
async function discoverArtistsInGenre(
  spotify: SpotifyClient,
  batchId: string,
  genre: string,
  maxArtists: number = 10,
  minPopularity: number = 40
): Promise<number> {
  try {
    await logSystemEvent("info", "discover_artists", `Discovering artists in genre: ${genre}`, { genre, maxArtists });
    
    // Update search to include market parameter
    const searchResult = await spotify.searchArtists(
      `genre:${genre}`, 
      50, 
      { market: TARGET_MARKETS.join(',') }
    );
    
    if (!searchResult?.artists?.items || searchResult.artists.items.length === 0) {
      await logSystemEvent("info", "discover_artists", `No artists found for genre: ${genre}`, { genre });
      return 0;
    }

    // Filter and sort artists by popularity
    const artists = searchResult.artists.items
      .filter((artist: any) => {
        // Check if artist has presence in target markets
        const hasTargetMarket = artist.markets ? 
          artist.markets.some((market: string) => TARGET_MARKETS.includes(market)) :
          true; // If no market data, assume they're available
        
        return hasTargetMarket && artist.popularity >= minPopularity;
      })
      .sort((a: any, b: any) => b.popularity - a.popularity)
      .slice(0, maxArtists);

    await logSystemEvent(
      "info", 
      "discover_artists", 
      `Found ${artists.length} artists for genre: ${genre} in target markets`,
      { genre, artistCount: artists.length, markets: TARGET_MARKETS }
    );

    // Process each artist
    for (let i = 0; i < artists.length; i++) {
      const artist = artists[i];
      const priority = Math.floor(artist.popularity / 10); // 0-10 scale

      // Store the artist in our database
      const { data: existingArtist, error: checkError } = await supabase
        .from("artists")
        .select("id")
        .eq("spotify_id", artist.id)
        .single();

      if (checkError && checkError.code !== "PGRST116") { // PGRST116 = not found
        console.error(`Error checking for existing artist ${artist.id}:`, checkError);
        continue;
      }

      // Only insert if artist doesn't exist
      if (!existingArtist) {
        const { error: insertError } = await supabase
          .from("artists")
          .insert({
            name: artist.name,
            spotify_id: artist.id,
            genres: artist.genres || [],
            popularity: artist.popularity,
            spotify_url: artist.external_urls?.spotify,
            image_url: artist.images?.[0]?.url,
            metadata: {
              followers: artist.followers,
              images: artist.images,
              external_urls: artist.external_urls,
            },
          });

        if (insertError) {
          console.error(`Error inserting artist ${artist.id}:`, insertError);
          continue;
        }
      }

      // Add artist to the processing batch
      await addArtistToBatch(batchId, artist.id, priority);
    }

    return artists.length;
  } catch (error) {
    console.error(`Error discovering artists in genre ${genre}:`, error);
    await logProcessingError(
      "discovery",
      "discover_artists",
      `Error discovering artists in genre ${genre}`,
      error,
      { genre, batchId }
    );
    return 0;
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const spotify = new SpotifyClient();
    
    // Parse request body
    const { genres = ["pop", "rock", "hip hop", "electronic"], limit = 50, minPopularity = 40, maxArtistsPerGenre = 10 } = 
      req.method === "POST" ? await req.json() : {};

    // Create a new batch
    const batchId = await createDiscoveryBatch({ genres, limit, minPopularity, maxArtistsPerGenre });
    
    await logSystemEvent(
      "info", 
      "discover_artists", 
      `Created discovery batch ${batchId} for genres: ${genres.join(", ")}`,
      { batchId, genres }
    );

    // Process discovery in the background
    EdgeRuntime.waitUntil((async () => {
      try {
        let totalDiscovered = 0;
        
        // Process each genre
        for (const genre of genres) {
          const discovered = await discoverArtistsInGenre(
            spotify, 
            batchId, 
            genre, 
            maxArtistsPerGenre, 
            minPopularity
          );
          
          totalDiscovered += discovered;
          
          // If we've reached our limit, stop
          if (totalDiscovered >= limit) {
            break;
          }
        }
        
        // FIX 1: Get the actual count of items in this batch from the database
        const { count: actualItemCount, error: countError } = await supabase
          .from("processing_items")
          .select("*", { count: "exact", head: true })
          .eq("batch_id", batchId);
          
        if (countError) {
          console.error("Error getting item count:", countError);
        }
        
        // Determine the accurate count
        const itemsTotal = countError ? totalDiscovered : (actualItemCount || 0);
        
        // FIX 2: Mark all items in this batch as completed
        const { error: updateItemsError } = await supabase
          .from("processing_items")
          .update({
            status: "completed"
          })
          .eq("batch_id", batchId);
          
        if (updateItemsError) {
          console.error("Error updating items status:", updateItemsError);
        }
        
        // Update batch status and mark with accurate counts
        await supabase
          .from("processing_batches")
          .update({
            status: "completed",
            items_total: itemsTotal,
            items_processed: itemsTotal,
            completed_at: new Date().toISOString(),
          })
          .eq("id", batchId);
        
        // Create next batch in pipeline for processing artists
        await createNextBatchInPipeline(batchId, "process_artists");
          
        await logSystemEvent(
          "info", 
          "discover_artists", 
          `Discovery batch ${batchId} completed with ${itemsTotal} artists discovered.`,
          { batchId, itemsTotal }
        );
      } catch (error) {
        console.error(`Error processing discovery batch ${batchId}:`, error);
        
        await logProcessingError(
          "batch_processing",
          "discover_artists",
          `Error processing discovery batch ${batchId}`,
          error,
          { batchId }
        );
        
        // Update batch with error
        await supabase
          .from("processing_batches")
          .update({
            status: "error",
            error_message: error.message,
          })
          .eq("id", batchId);
      }
    })());

    // Return immediate response
    return new Response(
      JSON.stringify({
        success: true,
        message: "Artist discovery started",
        batchId,
      }),
      {
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  } catch (error) {
    console.error("Error handling discover-artists request:", error);
    
    // Log error using our utility function
    await logProcessingError(
      "endpoint",
      "discover_artists",
      "Error handling discover-artists request",
      error
    );

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

// Create the next batch in the pipeline based on the current batch type
async function createNextBatchInPipeline(parentBatchId: string, batchType: string): Promise<string | null> {
  try {
    // Get the parent batch for reference
    const { data: parentBatch, error: batchError } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", parentBatchId)
      .single();
    
    if (batchError) {
      console.error(`Error getting parent batch ${parentBatchId}:`, batchError);
      return null;
    }
    
    // Create the next batch in the pipeline
    const { data: newBatch, error: createError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: batchType,
        status: "pending",
        metadata: {
          parent_batch_id: parentBatchId,
          parent_batch_type: parentBatch.batch_type,
        }
      })
      .select("id")
      .single();
    
    if (createError) {
      console.error(`Error creating next batch in pipeline:`, createError);
      return null;
    }
    
    await logSystemEvent(
      "info", 
      "discover_artists", 
      `Created next batch in pipeline: ${batchType} with ID ${newBatch.id}`,
      { batchType, newBatchId: newBatch.id, parentBatchId }
    );
    
    // For process_artists batch, add all processed artists from parent batch as items
    if (batchType === "process_artists") {
      // FIX 3: Get ONLY completed artists from the parent batch
      const { data: completedItems, error: itemsError } = await supabase
        .from("processing_items")
        .select("item_id, priority")
        .eq("batch_id", parentBatchId)
        .eq("item_type", "artist")
        .eq("status", "completed");  // Only get completed items
      
      if (itemsError) {
        console.error(`Error getting completed items from parent batch:`, itemsError);
        return newBatch.id;
      }
      
      if (completedItems && completedItems.length > 0) {
        // Create new processing items for each artist
        const newItems = completedItems.map(item => ({
          batch_id: newBatch.id,
          item_type: "artist",
          item_id: item.item_id,
          status: "pending",
          priority: item.priority
        }));
        
        const { error: insertError } = await supabase
          .from("processing_items")
          .insert(newItems);
        
        if (insertError) {
          console.error(`Error creating items for next batch:`, insertError);
        } else {
          await logSystemEvent(
            "info", 
            "discover_artists", 
            `Added ${newItems.length} artists to process_artists batch ${newBatch.id}`,
            { newBatchId: newBatch.id, itemCount: newItems.length }
          );
          
          // Update the batch with the accurate total number of items
          await supabase
            .from("processing_batches")
            .update({
              items_total: newItems.length
            })
            .eq("id", newBatch.id);
        }
      }
    }
    
    return newBatch.id;
  } catch (error) {
    console.error(`Error creating next batch in pipeline:`, error);
    await logProcessingError(
      "batch_creation",
      "discover_artists",
      `Error creating next batch in pipeline for ${parentBatchId}`,
      error,
      { parentBatchId }
    );
    return null;
  }
}
