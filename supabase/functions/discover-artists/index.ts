
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { SpotifyClient, supabase } from "../lib/api-clients.ts";
import { 
  logSystemEvent, 
  logProcessingError,
  updateItemStatus,
  generateCorrelationId,
  checkCircuitBreaker,
  checkBackpressure,
  incrementMetric
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
  correlationId?: string;
  notifyOnCompletion?: boolean;
}

// Create a new batch for discovering artists
async function createDiscoveryBatch(options: DiscoverArtistsOptions): Promise<string> {
  try {
    const { data, error } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "discover_artists",
        status: "pending",
        metadata: {
          ...options,
          created_at: new Date().toISOString()
        }
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
  minPopularity: number = 40,
  correlationId: string
): Promise<number> {
  try {
    await logSystemEvent(
      "info", 
      "discover_artists", 
      `Discovering artists in genre: ${genre}`, 
      { genre, maxArtists, correlationId },
      correlationId
    );
    
    // Check if the API is rate limited
    const isRateLimited = await checkCircuitBreaker('spotify');
    if (isRateLimited) {
      await logSystemEvent(
        "warning", 
        "discover_artists", 
        `Spotify API is rate limited, skipping genre: ${genre}`,
        { genre, correlationId },
        correlationId
      );
      return 0;
    }
    
    // Update search to include market parameter
    const searchResult = await spotify.searchArtists(
      `genre:${genre}`, 
      50, 
      { market: TARGET_MARKETS.join(',') }
    );
    
    if (!searchResult?.artists?.items || searchResult.artists.items.length === 0) {
      await logSystemEvent(
        "info", 
        "discover_artists", 
        `No artists found for genre: ${genre}`, 
        { genre, correlationId },
        correlationId
      );
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
      { genre, artistCount: artists.length, markets: TARGET_MARKETS, correlationId },
      correlationId
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
              correlation_id: correlationId
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
      { genre, batchId, correlationId }
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
    // Generate a correlation ID for tracing this request
    const correlationId = generateCorrelationId();
    
    // Create Spotify client
    const spotify = new SpotifyClient();
    
    // Parse request body
    const { 
      genres = ["pop", "rock", "hip hop", "electronic"], 
      limit = 50, 
      minPopularity = 40, 
      maxArtistsPerGenre = 10,
      notifyOnCompletion = false
    } = req.method === "POST" ? await req.json() : {};

    // Check backpressure - don't discover artists if we're already processing too many
    const hasBackpressure = await checkBackpressure('discover_artists', 2);
    if (hasBackpressure) {
      return new Response(
        JSON.stringify({
          success: false,
          message: "System is under high load, please try again later",
        }),
        {
          status: 429,
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
    }

    // Create a new batch
    const batchId = await createDiscoveryBatch({ 
      genres, 
      limit, 
      minPopularity, 
      maxArtistsPerGenre,
      correlationId,
      notifyOnCompletion
    });
    
    await logSystemEvent(
      "info", 
      "discover_artists", 
      `Created discovery batch ${batchId} for genres: ${genres.join(", ")}`,
      { batchId, genres, correlationId },
      correlationId
    );

    // Process discovery in the background
    EdgeRuntime.waitUntil((async () => {
      try {
        // Register this worker
        const workerId = crypto.randomUUID();
        
        await logSystemEvent(
          "info", 
          "discover_artists", 
          `Worker ${workerId} starting processing for batch ${batchId}`,
          { workerId, batchId, correlationId },
          correlationId
        );
        
        // Mark batch as processing
        await supabase
          .from("processing_batches")
          .update({
            status: "processing",
            started_at: new Date().toISOString(),
            claimed_by: workerId,
            updated_at: new Date().toISOString()
          })
          .eq("id", batchId);
        
        let totalDiscovered = 0;
        
        // Process each genre
        for (const genre of genres) {
          // Check if we've been rate limited
          const isRateLimited = await checkCircuitBreaker('spotify');
          if (isRateLimited) {
            await logSystemEvent(
              "warning", 
              "discover_artists", 
              `Spotify API is rate limited, pausing discovery for batch ${batchId}`,
              { batchId, workerId, correlationId },
              correlationId
            );
            
            // Mark batch for retry
            await supabase
              .from("processing_batches")
              .update({
                status: "rate_limited",
                error_message: "Spotify API rate limited",
                updated_at: new Date().toISOString()
              })
              .eq("id", batchId);
              
            return;
          }
          
          const discovered = await discoverArtistsInGenre(
            spotify, 
            batchId, 
            genre, 
            maxArtistsPerGenre, 
            minPopularity,
            correlationId
          );
          
          totalDiscovered += discovered;
          
          // If we've reached our limit, stop
          if (totalDiscovered >= limit) {
            break;
          }
        }
        
        // Get the actual count of items in this batch from the database
        const { count: actualItemCount, error: countError } = await supabase
          .from("processing_items")
          .select("*", { count: "exact", head: true })
          .eq("batch_id", batchId);
          
        if (countError) {
          console.error("Error getting item count:", countError);
        }
        
        // Determine the accurate count
        const itemsTotal = countError ? totalDiscovered : (actualItemCount || 0);
        
        // Mark all items in this batch as completed
        const { error: updateItemsError } = await supabase
          .from("processing_items")
          .update({
            status: "completed",
            updated_at: new Date().toISOString()
          })
          .eq("batch_id", batchId);
          
        if (updateItemsError) {
          console.error("Error updating items status:", updateItemsError);
        }
        
        // Update batch status and mark with accurate counts and completion signal
        await supabase
          .from("processing_batches")
          .update({
            status: "completed",
            items_total: itemsTotal,
            items_processed: itemsTotal,
            completed_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            metadata: {
              genres,
              limit,
              minPopularity,
              maxArtistsPerGenre,
              correlationId,
              artists_completed: true, // Signal for next pipeline stage
              artists_completed_at: new Date().toISOString(),
              notifyOnCompletion
            }
          })
          .eq("id", batchId);
        
        await incrementMetric('artists_discovered', itemsTotal, {
          batch_id: batchId,
          genre_count: genres.length
        });
          
        await logSystemEvent(
          "info", 
          "discover_artists", 
          `Discovery batch ${batchId} completed with ${itemsTotal} artists discovered. Signaled artists_completed.`,
          { batchId, itemsTotal, correlationId },
          correlationId
        );
        
        // Trigger the monitor pipeline to detect our completion
        EdgeRuntime.waitUntil(
          supabase.functions.invoke("monitor-pipeline", {
            body: {
              specificBatchId: batchId,
              correlationId
            }
          })
        );
        
      } catch (error) {
        console.error(`Error processing discovery batch ${batchId}:`, error);
        
        await logProcessingError(
          "batch_processing",
          "discover_artists",
          `Error processing discovery batch ${batchId}`,
          error,
          { batchId, correlationId }
        );
        
        // Update batch with error
        await supabase
          .from("processing_batches")
          .update({
            status: "error",
            error_message: error.message,
            updated_at: new Date().toISOString()
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
        correlationId
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
