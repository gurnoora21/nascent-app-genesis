
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { SpotifyClient, supabase } from "../lib/api-clients.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Process an artist's albums and tracks
async function processArtist(artistId: string): Promise<{
  success: boolean;
  message: string;
  processedAlbums?: number;
}> {
  try {
    const spotify = new SpotifyClient();
    
    // Get artist details from our database
    const { data: artist, error: artistError } = await supabase
      .from("artists")
      .select("id, name, spotify_id")
      .eq("id", artistId)
      .single();
    
    if (artistError) {
      console.error(`Error getting artist ${artistId}:`, artistError);
      throw artistError;
    }
    
    if (!artist.spotify_id) {
      throw new Error(`No Spotify ID found for artist ${artistId}`);
    }

    console.log(`Processing artist: ${artist.name} (${artist.spotify_id})`);
    
    // Get all albums from Spotify
    let offset = 0;
    const limit = 50;
    let totalProcessed = 0;
    let hasMore = true;
    
    while (hasMore) {
      const albumsResult = await spotify.getArtistAlbums(
        artist.spotify_id,
        limit,
        offset,
        "album,single,ep" // Explicitly exclude compilations and appears_on
      );
      
      if (!albumsResult?.items) {
        console.log(`No more albums found for artist ${artist.name}`);
        break;
      }

      const albums = albumsResult.items.filter(album => 
        // Additional filtering to ensure clean data
        album.artists[0].id === artist.spotify_id // Artist is the primary artist
      );
      
      console.log(`Found ${albums.length} albums to process for ${artist.name}`);
      
      // Process each album
      for (const album of albums) {
        try {
          // Store album in our database
          const { data: newAlbum, error: albumError } = await supabase
            .from("albums")
            .upsert({
              spotify_id: album.id,
              name: album.name,
              artist_id: artist.id,
              album_type: album.album_type,
              release_date: album.release_date,
              total_tracks: album.total_tracks,
              spotify_url: album.external_urls?.spotify,
              image_url: album.images?.[0]?.url,
              popularity: album.popularity,
              is_primary_artist_album: true,
              metadata: {
                spotify: album
              }
            })
            .select("id")
            .single();
          
          if (albumError) {
            console.error(`Error storing album ${album.id}:`, albumError);
            continue;
          }
          
          totalProcessed++;
        } catch (albumError) {
          console.error(`Error processing album ${album.id}:`, albumError);
          continue;
        }
      }
      
      offset += limit;
      hasMore = albumsResult.next !== null;
    }
    
    // Create a batch for processing the artist's tracks
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "process_tracks",
        status: "pending",
        metadata: {
          artist_spotify_id: artist.spotify_id,
          artist_name: artist.name
        }
      })
      .select("id")
      .single();
    
    if (batchError) {
      console.error(`Error creating tracks batch for artist ${artist.name}:`, batchError);
      throw batchError;
    }
    
    console.log(`Created tracks batch ${batch.id} for artist ${artist.name}`);
    
    return {
      success: true,
      message: `Processed ${totalProcessed} albums for artist ${artist.name}`,
      processedAlbums: totalProcessed
    };
  } catch (error) {
    console.error(`Error processing artist ${artistId}:`, error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "process_artist",
      p_message: `Error processing artist`,
      p_stack_trace: error.stack || "",
      p_context: { artistId },
      p_item_id: artistId,
      p_item_type: "artist"
    });
    
    return {
      success: false,
      message: `Error processing artist: ${error.message}`
    };
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { artistId } = await req.json();
    
    if (!artistId) {
      return new Response(
        JSON.stringify({
          success: false,
          error: "artistId is required",
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

    const result = await processArtist(artistId);
    
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
    console.error("Error handling process-artist request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_artist",
        p_message: "Error handling process-artist request",
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
