import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { SpotifyClient, supabase } from "../lib/api-clients.ts";
import { 
  logSystemEvent, 
  logProcessingError,
  updateDataQualityScore 
} from "../lib/pipeline-utils.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

async function processArtist(artistIdentifier: string, isTestMode = false): Promise<{
  success: boolean;
  message: string;
  processedAlbums?: number;
}> {
  try {
    // Create Spotify client with appropriate throttle settings
    const spotifyThrottle = isTestMode ? 200 : 500; // Faster in test mode
    const spotify = new SpotifyClient(spotifyThrottle, true);
    
    console.log(`Processing artist ${artistIdentifier} (Test mode: ${isTestMode})`);
    
    let artist;
    const isUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(artistIdentifier);
    
    // Get artist details - either from our DB or create new
    if (isUUID) {
      const { data: foundArtist, error: artistError } = await supabase
        .from("artists")
        .select("id, name, spotify_id")
        .eq("id", artistIdentifier)
        .single();
      
      if (artistError) throw artistError;
      artist = foundArtist;
    } else {
      // Try to find by Spotify ID first
      const { data: foundArtist, error: artistError } = await supabase
        .from("artists")
        .select("id, name, spotify_id")
        .eq("spotify_id", artistIdentifier)
        .single();
      
      if (!artistError && foundArtist) {
        artist = foundArtist;
      } else {
        // Create new artist from Spotify data
        const spotifyArtist = await spotify.getArtist(artistIdentifier);
        
        if (!spotifyArtist?.id) {
          throw new Error(`Could not find artist with Spotify ID ${artistIdentifier}`);
        }
        
        const { data: newArtist, error: insertError } = await supabase
          .from("artists")
          .insert({
            name: spotifyArtist.name,
            spotify_id: spotifyArtist.id,
            popularity: spotifyArtist.popularity,
            genres: spotifyArtist.genres,
            image_url: spotifyArtist.images?.[0]?.url,
            spotify_url: spotifyArtist.external_urls?.spotify,
            metadata: { spotify: spotifyArtist }
          })
          .select("id, name, spotify_id")
          .single();
        
        if (insertError) throw insertError;
        artist = newArtist;
        
        await updateDataQualityScore(
          "artist",
          newArtist.id,
          "spotify",
          spotifyArtist.popularity ? spotifyArtist.popularity / 100 : 0.5,
          spotifyArtist.images && spotifyArtist.genres ? 0.8 : 0.5,
          0.9
        );
      }
    }
    
    if (!artist?.spotify_id) {
      throw new Error(`No Spotify ID found for artist ${artist?.id || artistIdentifier}`);
    }

    await logSystemEvent(
      "info", 
      "process_artist", 
      `Processing artist: ${artist.name} (${artist.spotify_id})`,
      { artistId: artist.id, spotifyId: artist.spotify_id }
    );
    
    // Check for existing albums first
    const { data: existingAlbums } = await supabase
      .from("albums")
      .select("id")
      .eq("artist_id", artist.id)
      .eq("is_primary_artist_album", true);
    
    let processedAlbumIds = existingAlbums?.map(album => album.id) || [];
    console.log(`Found ${processedAlbumIds.length} existing albums for ${artist.name}`);
    
    // Get new albums from Spotify without artificial limits
    try {
      let offset = 0;
      let hasMore = true;
      const limit = 50; // Maximum allowed by Spotify API
      
      while (hasMore) {
        const albumsResult = await spotify.getArtistAlbums(
          artist.spotify_id,
          limit,
          offset,
          "album,single,ep"
        );
        
        if (!albumsResult?.items?.length) break;
        
        // Filter for primary artist albums
        const albums = albumsResult.items.filter(album => 
          album.artists[0].id === artist.spotify_id
        );
        
        console.log(`Processing ${albums.length} albums for ${artist.name} (offset: ${offset})`);
        
        for (const album of albums) {
          // Check if we already have this album
          const { data: existingAlbum } = await supabase
            .from("albums")
            .select("id")
            .eq("spotify_id", album.id)
            .maybeSingle();
          
          if (existingAlbum) {
            processedAlbumIds.push(existingAlbum.id);
            continue;
          }
          
          // Store new album
          const { data: newAlbum, error: albumError } = await supabase
            .from("albums")
            .insert({
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
              metadata: { spotify: album }
            })
            .select("id")
            .single();
          
          if (albumError) {
            console.error(`Error storing album ${album.id}:`, albumError);
            continue;
          }
          
          processedAlbumIds.push(newAlbum.id);
          
          await updateDataQualityScore(
            "album",
            newAlbum.id,
            "spotify",
            album.popularity ? album.popularity / 100 : 0.6,
            album.total_tracks && album.release_date ? 0.8 : 0.6,
            0.9
          );
        }
        
        offset += limit;
        hasMore = albumsResult.next !== null;
      }
    } catch (albumsError) {
      console.error(`Error fetching albums for ${artist.name}:`, albumsError);
      
      // If we have some albums already, continue with those
      if (processedAlbumIds.length === 0) {
        throw albumsError;
      }
      
      await logSystemEvent(
        "warning",
        "process_artist",
        `Error fetching albums but continuing with ${processedAlbumIds.length} existing albums`,
        { 
          artistId: artist.id, 
          error: albumsError.message,
          albumCount: processedAlbumIds.length
        }
      );
    }
    
    // Create tracks processing batch with all discovered albums
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "process_tracks",
        status: "pending",
        metadata: {
          artist_spotify_id: artist.spotify_id,
          artist_name: artist.name,
          is_test_mode: isTestMode
        }
      })
      .select("id")
      .single();
    
    if (batchError) throw batchError;
    
    if (processedAlbumIds.length > 0) {
      // Create processing items in chunks of 100
      const chunkSize = 100;
      for (let i = 0; i < processedAlbumIds.length; i += chunkSize) {
        const chunk = processedAlbumIds.slice(i, i + chunkSize);
        const processingItems = chunk.map(albumId => ({
          batch_id: batch.id,
          item_type: 'album_for_tracks',
          item_id: albumId,
          status: 'pending',
          priority: 5
        }));
        
        const { error: itemsError } = await supabase
          .from("processing_items")
          .insert(processingItems);
        
        if (itemsError) {
          console.error(`Error adding batch chunk ${i / chunkSize + 1}:`, itemsError);
          continue;
        }
      }
      
      console.log(`Added ${processedAlbumIds.length} albums to batch ${batch.id}`);
    }
    
    const message = `Processed ${processedAlbumIds.length} albums for artist ${artist.name}${isTestMode ? " (Test Mode)" : ""}`;
    console.log(message);
    
    return {
      success: true,
      message,
      processedAlbums: processedAlbumIds.length
    };
  } catch (error) {
    console.error(`Error processing artist ${artistIdentifier}:`, error);
    
    await logProcessingError(
      "processing",
      "process_artist",
      `Error processing artist`,
      error,
      { artistIdentifier },
      artistIdentifier,
      "artist"
    );
    
    return {
      success: false,
      message: `Error processing artist: ${error.message}`,
      processedAlbums: 0
    };
  }
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { artistId, isTestMode } = await req.json();
    
    if (!artistId) {
      return new Response(
        JSON.stringify({
          success: false,
          error: "artistId is required",
        }),
        {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders },
        }
      );
    }

    const result = await processArtist(artistId, isTestMode);
    
    return new Response(
      JSON.stringify(result),
      {
        status: result.success ? 200 : 500,
        headers: { "Content-Type": "application/json", ...corsHeaders },
      }
    );
  } catch (error) {
    console.error("Error handling process-artist request:", error);
    
    await logProcessingError(
      "endpoint",
      "process_artist",
      "Error handling process-artist request",
      error
    );

    return new Response(
      JSON.stringify({
        success: false,
        error: error.message,
      }),
      {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders },
      }
    );
  }
});
