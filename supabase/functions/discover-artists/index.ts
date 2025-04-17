
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { SpotifyClient, supabase } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

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
    return data.id;
  } catch (error) {
    console.error("Error creating discovery batch:", error);
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
      // If the error is not a duplicate, throw it
      if (!error.message.includes("duplicate key value")) {
        throw error;
      }
    }
  } catch (error) {
    console.error(`Error adding artist ${spotifyId} to batch:`, error);
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
    console.log(`Discovering artists in genre: ${genre}`);
    
    // Search for artists in the genre
    const searchResult = await spotify.searchArtists(`genre:${genre}`, 50);
    
    if (!searchResult?.artists?.items || searchResult.artists.items.length === 0) {
      console.log(`No artists found for genre: ${genre}`);
      return 0;
    }

    // Filter and sort artists by popularity
    const artists = searchResult.artists.items
      .filter((artist: any) => artist.popularity >= minPopularity)
      .sort((a: any, b: any) => b.popularity - a.popularity)
      .slice(0, maxArtists);

    console.log(`Found ${artists.length} artists for genre: ${genre}`);

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
    await supabase.rpc("log_error", {
      p_error_type: "discovery",
      p_source: "discover_artists",
      p_message: `Error discovering artists in genre ${genre}`,
      p_stack_trace: error.stack || "",
      p_context: { genre },
    });
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
    
    console.log(`Created discovery batch ${batchId} for genres: ${genres.join(", ")}`);

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
        
        // Update batch status
        await supabase
          .from("processing_batches")
          .update({
            status: "completed",
            items_total: totalDiscovered,
            items_processed: totalDiscovered,
            completed_at: new Date().toISOString(),
          })
          .eq("id", batchId);

        // Create next batch in pipeline for processing artists
        await createNextBatchInPipeline(batchId, "process_artists");
          
        console.log(`Discovery batch ${batchId} completed with ${totalDiscovered} artists discovered. Created process_artists batch.`);
      } catch (error) {
        console.error(`Error processing discovery batch ${batchId}:`, error);
        
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
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "discover_artists",
        p_message: "Error handling discover-artists request",
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
    
    console.log(`Created next batch in pipeline: ${batchType} with ID ${newBatch.id}`);
    
    // For process_artists batch, add all artists from parent batch as items to process
    if (batchType === "process_artists") {
      const { data: items, error: itemsError } = await supabase
        .from("processing_items")
        .select("item_id, priority")
        .eq("batch_id", parentBatchId)
        .eq("item_type", "artist");
      
      if (itemsError) {
        console.error(`Error getting items from parent batch:`, itemsError);
        return newBatch.id;
      }
      
      if (items && items.length > 0) {
        // Create new processing items for each artist
        const newItems = items.map(item => ({
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
          console.log(`Added ${newItems.length} artists to process_artists batch ${newBatch.id}`);
          
          // Update the batch with the total number of items
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
    return null;
  }
}
