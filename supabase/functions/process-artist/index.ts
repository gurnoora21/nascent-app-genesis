
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
    // Create Spotify client with appropriate settings
    // Use slower throttle (500ms) in production for reliability, faster in test mode
    const spotifyThrottle = isTestMode ? 200 : 500;
    const spotify = new SpotifyClient(spotifyThrottle, true); // Enable caching
    
    console.log(`Processing artist ${artistIdentifier} (Test mode: ${isTestMode})`);
    
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
        
        try {
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
        } catch (spotifyError) {
          console.error(`Error fetching artist from Spotify: ${spotifyError.message}`);
          
          // Check if this is just a rate limit - if we're in test mode, we can bypass
          const isRateLimit = spotifyError.message && (
            spotifyError.message.includes("rate limit") || 
            spotifyError.message.includes("Too many requests") ||
            spotifyError.message.includes("429")
          );
          
          if (isRateLimit && isTestMode) {
            console.log("Rate limited while creating artist, but we're in test mode so we'll try to find a matching artist");
            
            // Try to find artist by name if available
            const artistName = artistIdentifier.split(":").pop(); // Try to extract name from ID format
            
            if (artistName) {
              const { data: existingArtists } = await supabase
                .from("artists")
                .select("id, name, spotify_id")
                .ilike("name", `%${artistName}%`)
                .limit(1);
                
              if (existingArtists && existingArtists.length > 0) {
                console.log(`Found existing artist for testing: ${existingArtists[0].name}`);
                artist = existingArtists[0];
                
                // Log that we're using an existing artist for test mode
                await logSystemEvent(
                  "info",
                  "process_artist",
                  `Using existing artist ${artist.name} for test mode due to rate limiting`,
                  { spotifyId: artist.spotify_id, artistId: artist.id }
                );
              } else {
                throw new Error(`Rate limited and couldn't find artist match for testing: ${artistName}`);
              }
            } else {
              throw spotifyError; // Can't bypass in this case
            }
          } else {
            throw spotifyError;
          }
        }
      } else {
        artist = foundArtist;
      }
    }
    
    if (!artist || !artist.spotify_id) {
      throw new Error(`No Spotify ID found for artist ${artist?.id || artistIdentifier}`);
    }

    await logSystemEvent(
      "info", 
      "process_artist", 
      `Processing artist: ${artist.name} (${artist.spotify_id})`,
      { artistId: artist.id, spotifyId: artist.spotify_id }
    );
    
    // Variables to track our progress
    let processedAlbumIds: string[] = [];
    let totalProcessed = 0;
    let apiRateLimited = false;
    
    // In test mode or if rate limited, check if we can use existing albums
    try {
      // Check if we have existing albums for this artist that we can use
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
        console.log(`Found ${totalProcessed} existing albums for ${artist.name}`);
        
        // If we're in test mode and have albums, we can use them
        if (isTestMode) {
          console.log(`Using existing albums for ${artist.name} in test mode`);
          
          // For test mode, we'll limit to just a few albums
          processedAlbumIds = processedAlbumIds.slice(0, 3);
          totalProcessed = processedAlbumIds.length;
        }
      } else if (albumsError) {
        console.error(`Error fetching existing albums for artist ${artist.id}:`, albumsError);
      }
    } catch (error) {
      console.error(`Error checking existing albums for artist ${artist.id}:`, error);
    }
    
    // Try to get albums from Spotify if we're not in test mode or if we don't have any albums yet
    if (!isTestMode || processedAlbumIds.length === 0) {
      try {
        await logSystemEvent(
          "info",
          "process_artist",
          `Fetching albums from Spotify for ${artist.name}`,
          { artistId: artist.id, spotifyId: artist.spotify_id }
        );
        
        // Set a reasonable batch size for albums
        const limit = 20; // Smaller batch size to avoid hitting rate limits
        let offset = 0;
        let hasMore = true;
        
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
            
            // Process each album - for test mode, limit to 3 albums max
            const albumsToProcess = isTestMode ? albums.slice(0, 3) : albums;
            
            for (const album of albumsToProcess) {
              try {
                // Check if we already have this album to avoid duplicates
                const { data: existingAlbum } = await supabase
                  .from("albums")
                  .select("id")
                  .eq("spotify_id", album.id)
                  .maybeSingle();
                
                if (existingAlbum) {
                  // Already have this album, just add its ID to our processed list
                  processedAlbumIds.push(existingAlbum.id);
                  totalProcessed++;
                  continue;
                }
                
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
                
                // In test mode, limit the number of albums we process
                if (isTestMode && totalProcessed >= 3) {
                  console.log(`Reached limit of 3 albums for test mode`);
                  break;
                }
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
            
            // Break early if we're in test mode and have enough albums
            if (isTestMode && totalProcessed >= 3) {
              console.log(`Limiting to ${totalProcessed} albums for test mode`);
              break;
            }
            
            offset += limit;
            hasMore = albumsResult.next !== null && !isTestMode; // Only continue paging in non-test mode
          } catch (fetchError) {
            console.error(`Error fetching albums for artist ${artist.spotify_id}:`, fetchError);
            
            // Check if this is a rate limit error
            if (fetchError.message && (
                fetchError.message.includes("rate limit") || 
                fetchError.message.includes("Too many requests") ||
                fetchError.message.includes("429")
              )) {
              apiRateLimited = true;
              await logSystemEvent(
                "warning", 
                "process_artist", 
                `Spotify API rate limited while processing artist ${artist.name}`,
                { artistId: artist.id, spotifyId: artist.spotify_id }
              );
              
              // If we have some albums already, we can continue with those
              if (processedAlbumIds.length > 0) {
                console.log(`Using ${processedAlbumIds.length} albums already processed despite rate limiting`);
                break;
              } else {
                throw fetchError;
              }
            } else {
              throw fetchError;
            }
          }
        }
      } catch (albumsError) {
        if (processedAlbumIds.length === 0) {
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
        } else {
          // Log the error but continue with the albums we already have
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
      }
    }
    
    // Make sure we have at least some albums to process
    if (processedAlbumIds.length === 0) {
      const message = `No albums found or processed for artist ${artist.name} (${artist.spotify_id})`;
      console.warn(message);
      
      // If in test mode and we have no albums, try to create at least one fake album for testing
      if (isTestMode) {
        console.log('Creating test album for ' + artist.name);
        
        // Create a test album
        try {
          const { data: testAlbum, error: testAlbumError } = await supabase
            .from('albums')
            .insert({
              name: `Test Album for ${artist.name}`,
              artist_id: artist.id,
              is_primary_artist_album: true,
              album_type: 'album',
              total_tracks: 1,
              release_date: new Date().toISOString().split('T')[0],
              metadata: {
                test: true,
                created_for_testing: true
              }
            })
            .select('id')
            .single();
            
          if (testAlbumError) {
            console.error('Error creating test album:', testAlbumError);
          } else {
            processedAlbumIds.push(testAlbum.id);
            totalProcessed = 1;
            
            console.log(`Created test album ${testAlbum.id} for ${artist.name}`);
          }
        } catch (testError) {
          console.error('Error in test album creation:', testError);
        }
      }
      
      // If we still have no albums, return an error
      if (processedAlbumIds.length === 0) {
        if (isTestMode) {
          // In test mode, we'll continue with the batch creation but log a warning
          await logSystemEvent(
            "warning",
            "process_artist",
            message,
            { artistId: artist.id, spotifyId: artist.spotify_id }
          );
        } else {
          // In production mode, this is an error
          throw new Error(message);
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

    await logSystemEvent(
      "info",
      "process_artist",
      `Created batch ${batch.id} for processing tracks of artist ${artist.name}`,
      { batchId: batch.id, artistId: artist.id }
    );

    // Populate the batch with processing items
    if (processedAlbumIds.length > 0) {
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
        console.error(`Error creating processing items for batch:`, itemsError);
        throw itemsError;
      }

      await logSystemEvent(
        "info",
        "process_artist",
        `Added ${processedAlbumIds.length} albums to batch ${batch.id} for artist ${artist.name}`,
        { batchId: batch.id, artistId: artist.id, albumCount: processedAlbumIds.length }
      );
    }
    
    let message = `Processed ${totalProcessed} albums for artist ${artist.name}`;
    if (apiRateLimited) {
      message += " (API rate limited, using available albums)";
    }
    
    if (isTestMode) {
      message += " [TEST MODE]";
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
