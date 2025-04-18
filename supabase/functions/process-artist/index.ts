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

// Process an artist's albums and tracks
async function processArtist(artistIdentifier: string, isTestMode = false): Promise<{
  success: boolean;
  message: string;
  processedAlbums?: number;
}> {
  try {
    const spotify = new SpotifyClient();
    let artist;
    
    // Check if it's a UUID or a Spotify ID
    const isUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(artistIdentifier);
    
    if (isUUID) {
      // Get artist details from our database using UUID
      const { data: foundArtist, error: artistError } = await supabase
        .from("artists")
        .select("id, name, spotify_id")
        .eq("id", artistIdentifier)
        .single();
      
      if (artistError) {
        console.error(`Error getting artist ${artistIdentifier}:`, artistError);
        throw artistError;
      }
      
      artist = foundArtist;
    } else {
      // It's a Spotify ID, try to find the artist by spotify_id
      const { data: foundArtist, error: artistError } = await supabase
        .from("artists")
        .select("id, name, spotify_id")
        .eq("spotify_id", artistIdentifier)
        .single();
      
      if (artistError) {
        // Artist doesn't exist yet, create it first
        await logSystemEvent(
          "info", 
          "process_artist", 
          `Artist with Spotify ID ${artistIdentifier} not found. Creating new entry.`,
          { spotifyId: artistIdentifier }
        );
        
        // Get artist info from Spotify
        const spotifyArtist = await spotify.getArtist(artistIdentifier);
        
        if (!spotifyArtist || !spotifyArtist.id) {
          throw new Error(`Could not find artist with Spotify ID ${artistIdentifier}`);
        }
        
        // Insert the artist into our database
        const { data: newArtist, error: insertError } = await supabase
          .from("artists")
          .insert({
            name: spotifyArtist.name,
            spotify_id: spotifyArtist.id,
            popularity: spotifyArtist.popularity,
            genres: spotifyArtist.genres,
            image_url: spotifyArtist.images?.[0]?.url,
            spotify_url: spotifyArtist.external_urls?.spotify,
            metadata: {
              spotify: spotifyArtist
            }
          })
          .select("id, name, spotify_id")
          .single();
        
        if (insertError) {
          console.error(`Error creating artist with Spotify ID ${artistIdentifier}:`, insertError);
          throw insertError;
        }
        
        artist = newArtist;
        
        // Track data quality score
        await updateDataQualityScore(
          "artist",
          newArtist.id,
          "spotify",
          spotifyArtist.popularity ? spotifyArtist.popularity / 100 : 0.5,
          spotifyArtist.images && spotifyArtist.genres ? 0.8 : 0.5,
          0.9, // Spotify artist data tends to be accurate
          { spotify_popularity: spotifyArtist.popularity }
        );
      } else {
        artist = foundArtist;
      }
    }
    
    if (!artist.spotify_id) {
      throw new Error(`No Spotify ID found for artist ${artist.id}`);
    }

    await logSystemEvent(
      "info", 
      "process_artist", 
      `Processing artist: ${artist.name} (${artist.spotify_id})`,
      { artistId: artist.id, spotifyId: artist.spotify_id }
    );
    
    // If API is rate limited, check if we can use existing albums for test mode
    let processedAlbumIds: string[] = [];
    let totalProcessed = 0;
    let apiRateLimited = false;
    
    try {
      // Check if we have existing albums for this artist that we can use
      if (isTestMode) {
        console.log(`Checking for existing albums for artist ${artist.id} (${artist.name})`);
        const { data: existingAlbums, error: albumsError } = await supabase
          .from("albums")
          .select("id")
          .eq("artist_id", artist.id)
          .eq("is_primary_artist_album", true)
          .limit(10);
        
        if (!albumsError && existingAlbums && existingAlbums.length > 0) {
          processedAlbumIds = existingAlbums.map(album => album.id);
          totalProcessed = processedAlbumIds.length;
          console.log(`Found ${totalProcessed} existing albums for ${artist.name} to use for test mode`);
        } else if (albumsError) {
          console.error(`Error fetching existing albums for artist ${artist.id}:`, albumsError);
        }
      }
    } catch (error) {
      console.error(`Error checking existing albums for artist ${artist.id}:`, error);
    }
    
    // Try to get albums from Spotify if we don't have any yet or if this is not test mode
    if (!isTestMode || processedAlbumIds.length === 0) {
      // Get all albums from Spotify
      let offset = 0;
      const limit = 50;
      let hasMore = true;
      
      try {
        while (hasMore) {
          try {
            const albumsResult = await spotify.getArtistAlbums(
              artist.spotify_id,
              limit,
              offset,
              "album,single,ep" // Explicitly exclude compilations and appears_on
            );
            
            if (!albumsResult?.items) {
              await logSystemEvent(
                "info", 
                "process_artist", 
                `No more albums found for artist ${artist.name}`,
                { artistId: artist.id }
              );
              break;
            }

            const albums = albumsResult.items.filter(album => 
              // Additional filtering to ensure clean data
              album.artists[0].id === artist.spotify_id // Artist is the primary artist
            );
            
            await logSystemEvent(
              "info", 
              "process_artist", 
              `Found ${albums.length} albums to process for ${artist.name}`,
              { artistId: artist.id, albumCount: albums.length }
            );
            
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

                processedAlbumIds.push(newAlbum.id);
                
                // Update data quality score for the album
                await updateDataQualityScore(
                  "album",
                  newAlbum.id,
                  "spotify",
                  album.popularity ? album.popularity / 100 : 0.6,
                  album.total_tracks && album.release_date ? 0.8 : 0.6,
                  0.9
                );
                
                totalProcessed++;
              } catch (albumError) {
                console.error(`Error processing album ${album.id}:`, albumError);
                await logProcessingError(
                  "album_processing", 
                  "process_artist", 
                  `Error processing album ${album.id}`,
                  albumError,
                  { albumId: album.id, artistId: artist.id }
                );
                continue;
              }
            }
            
            offset += limit;
            hasMore = albumsResult.next !== null;
          } catch (fetchError) {
            console.error(`Error fetching albums for artist ${artist.spotify_id}:`, fetchError);
            
            // Check if this is a rate limit error
            if (fetchError.message && (
                fetchError.message.includes("rate limit") || 
                fetchError.message.includes("Too many requests")
              )) {
              apiRateLimited = true;
              await logSystemEvent(
                "warning", 
                "process_artist", 
                `Spotify API rate limited while processing artist ${artist.name}`,
                { artistId: artist.id, spotifyId: artist.spotify_id }
              );
              
              // In test mode, we might have some albums already, so we can continue
              // Otherwise, we should stop and report the error
              if (!isTestMode || processedAlbumIds.length === 0) {
                throw fetchError;
              } else {
                console.log(`Using ${processedAlbumIds.length} albums already processed for test mode despite rate limiting`);
                break;
              }
            } else {
              throw fetchError;
            }
          }
        }
      } catch (albumsError) {
        if (!apiRateLimited || processedAlbumIds.length === 0) {
          await logProcessingError(
            "albums_fetch", 
            "process_artist", 
            `Error fetching albums for artist ${artist.name}`,
            albumsError,
            { artistId: artist.id, spotifyId: artist.spotify_id }
          );
          
          if (!isTestMode) {
            throw albumsError;
          }
        }
      }
    }
    
    // Create a batch for processing the artist's tracks
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
    
    if (batchError) {
      console.error(`Error creating tracks batch for artist ${artist.name}:`, batchError);
      throw batchError;
    }

    // If in test mode, populate the batch with processing items
    if (isTestMode && processedAlbumIds.length > 0) {
      const processingItems = processedAlbumIds.map(albumId => ({
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
        console.error(`Error creating processing items for test batch:`, itemsError);
        throw itemsError;
      }

      await logSystemEvent(
        "info",
        "process_artist",
        `Created test batch ${batch.id} with ${processedAlbumIds.length} albums for artist ${artist.name}`,
        { batchId: batch.id, artistId: artist.id, albumCount: processedAlbumIds.length }
      );
    }
    
    let message = `Processed ${totalProcessed} albums for artist ${artist.name}`;
    if (apiRateLimited) {
      message += " (API rate limited, used existing albums)";
    }
    
    return {
      success: true,
      message,
      processedAlbums: totalProcessed
    };
  } catch (error) {
    console.error(`Error processing artist ${artistIdentifier}:`, error);
    
    // Log error using our utility function
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
  // Handle CORS preflight requests
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
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
    }

    const result = await processArtist(artistId, isTestMode);
    
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
    
    // Log error using our utility function
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
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  }
});
