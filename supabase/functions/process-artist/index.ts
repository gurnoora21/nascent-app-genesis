
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { RetryHelper, SpotifyClient, supabase } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * Helper to split large arrays into smaller chunks
 */
function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

/**
 * Enhanced process artist albums helper function with improved pagination
 * Modified to better handle appears_on and compilation albums
 */
async function fetchArtistAlbums(spotify: SpotifyClient, spotifyId: string): Promise<any[]> {
  let allAlbums = [];
  let offset = 0;
  const limit = 50; // Maximum allowed by Spotify API
  let hasMore = true;
  let totalAlbums = 0;
  let retryCount = 0;
  const maxRetries = 3;
  let failedBatches = 0;
  
  // First fetch the artist's own albums (album, single)
  console.log(`Fetching primary albums for artist ${spotifyId} in chunks of ${limit}`);
  
  // Process albums - artist's own albums
  while (hasMore && failedBatches < 5) {
    try {
      const albumsResponse = await RetryHelper.retryOperation(
        async () => spotify.getArtistAlbums(
          spotifyId, 
          limit, 
          offset, 
          "album,single" // Only get the artist's own albums, not appears_on or compilations
        ),
        3,
        2000,
        `Fetch primary albums batch at offset ${offset}`
      );
      
      if (!albumsResponse) {
        console.error(`Empty response received for albums at offset ${offset}`);
        failedBatches++;
        offset += limit;
        continue;
      }
      
      if (!Array.isArray(albumsResponse?.items) || albumsResponse.items.length === 0) {
        console.log(`No more primary albums found at offset ${offset}`);
        hasMore = false;
        break;
      }
      
      if (offset === 0 && albumsResponse.total != null) {
        totalAlbums = albumsResponse.total;
        console.log(`Artist has ${totalAlbums} total primary albums to process`);
      }
      
      const validAlbums = albumsResponse.items.filter(item => {
        if (!item || !item.id) {
          console.warn(`Invalid album entry found in response`);
          return false;
        }
        return true;
      });
      
      const existingIds = new Set(allAlbums.map(a => a.id));
      const newAlbums = validAlbums.filter(album => !existingIds.has(album.id));
      const duplicates = validAlbums.length - newAlbums.length;
      
      if (duplicates > 0) {
        console.log(`Found ${duplicates} duplicate albums in this batch that were filtered out`);
      }
      
      // Add primary album flag to indicate this is the artist's own album
      newAlbums.forEach(album => {
        album.is_primary_artist_album = true;
      });
      
      allAlbums.push(...newAlbums);
      
      if (validAlbums.length < albumsResponse.items.length) {
        console.warn(`Filtered out ${albumsResponse.items.length - validAlbums.length} invalid album entries`);
      }
      
      offset += limit;
      console.log(`Fetched ${allAlbums.length}/${totalAlbums} primary albums so far (offset: ${offset})`);
      
      hasMore = validAlbums.length > 0 && 
               (albumsResponse.next || offset < totalAlbums);
      
      retryCount = 0;
    } catch (albumFetchError) {
      console.error(`Error fetching primary albums at offset ${offset}:`, albumFetchError);
      
      retryCount++;
      if (retryCount >= maxRetries) {
        console.warn(`Max retries (${maxRetries}) reached for offset ${offset}, moving to next batch`);
        offset += limit;
        retryCount = 0;
        failedBatches++;
      }
      
      await supabase.rpc("log_error", {
        p_error_type: "processing",
        p_source: "process_artist",
        p_message: `Error fetching albums batch`,
        p_stack_trace: albumFetchError.stack || "",
        p_context: { spotifyId, offset, limit, retryCount, failedBatches },
        p_item_id: spotifyId,
        p_item_type: "artist"
      });
      
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
  
  // Now fetch appears_on and compilation albums separately
  console.log(`Fetching appears_on and compilation albums for artist ${spotifyId}`);
  
  offset = 0;
  hasMore = true;
  totalAlbums = 0;
  retryCount = 0;
  failedBatches = 0;
  
  // Process albums - appears_on and compilations
  while (hasMore && failedBatches < 5) {
    try {
      const albumsResponse = await RetryHelper.retryOperation(
        async () => spotify.getArtistAlbums(
          spotifyId, 
          limit, 
          offset, 
          "appears_on,compilation" // Only get albums where artist appears on or compilations
        ),
        3,
        2000,
        `Fetch appears_on albums batch at offset ${offset}`
      );
      
      if (!albumsResponse) {
        console.error(`Empty response received for appears_on albums at offset ${offset}`);
        failedBatches++;
        offset += limit;
        continue;
      }
      
      if (!Array.isArray(albumsResponse?.items) || albumsResponse.items.length === 0) {
        console.log(`No more appears_on albums found at offset ${offset}`);
        hasMore = false;
        break;
      }
      
      if (offset === 0 && albumsResponse.total != null) {
        totalAlbums = albumsResponse.total;
        console.log(`Artist appears on ${totalAlbums} total albums to process`);
      }
      
      const validAlbums = albumsResponse.items.filter(item => {
        if (!item || !item.id) {
          console.warn(`Invalid album entry found in response`);
          return false;
        }
        return true;
      });
      
      const existingIds = new Set(allAlbums.map(a => a.id));
      const newAlbums = validAlbums.filter(album => !existingIds.has(album.id));
      const duplicates = validAlbums.length - newAlbums.length;
      
      if (duplicates > 0) {
        console.log(`Found ${duplicates} duplicate appears_on albums that were filtered out`);
      }
      
      // Add flag to indicate this is not the artist's own album
      newAlbums.forEach(album => {
        album.is_primary_artist_album = false;
      });
      
      allAlbums.push(...newAlbums);
      
      offset += limit;
      console.log(`Fetched ${allAlbums.length - totalAlbums}/${totalAlbums} appears_on albums so far (offset: ${offset})`);
      
      hasMore = validAlbums.length > 0 && 
               (albumsResponse.next || offset < totalAlbums);
      
      retryCount = 0;
    } catch (albumFetchError) {
      console.error(`Error fetching appears_on albums at offset ${offset}:`, albumFetchError);
      
      retryCount++;
      if (retryCount >= maxRetries) {
        console.warn(`Max retries (${maxRetries}) reached for offset ${offset}, moving to next batch`);
        offset += limit;
        retryCount = 0;
        failedBatches++;
      }
      
      await supabase.rpc("log_error", {
        p_error_type: "processing",
        p_source: "process_artist",
        p_message: `Error fetching appears_on albums batch`,
        p_stack_trace: albumFetchError.stack || "",
        p_context: { spotifyId, offset, limit, retryCount, failedBatches },
        p_item_id: spotifyId,
        p_item_type: "artist"
      });
      
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  // Final deduplication pass to ensure no duplicates
  const uniqueAlbumIds = new Set<string>();
  const uniqueAlbums = [];
  
  for (const album of allAlbums) {
    if (!uniqueAlbumIds.has(album.id)) {
      uniqueAlbumIds.add(album.id);
      uniqueAlbums.push(album);
    }
  }
  
  console.log(`Found ${uniqueAlbums.length} unique albums for artist (${allAlbums.length} total with duplicates removed)`);
  
  // Add detailed analytics
  const albumTypes = {
    album: 0,
    single: 0,
    compilation: 0,
    appears_on: 0
  };
  
  uniqueAlbums.forEach(album => {
    if (album.album_type && albumTypes.hasOwnProperty(album.album_type)) {
      albumTypes[album.album_type]++;
    }
  });
  
  console.log(`Album type breakdown: ${JSON.stringify(albumTypes)}`);
  
  return uniqueAlbums;
}

/**
 * Enhanced process album tracks function with better pagination
 */
async function fetchAlbumTracks(spotify: SpotifyClient, albumId: string): Promise<any[]> {
  let albumTracks = [];
  let trackOffset = 0;
  const trackLimit = 50; // Increased to Spotify's maximum
  let hasMoreTracks = true;
  let totalTracks = 0;
  let retryCount = 0;
  const maxRetries = 3;

  try {
    console.log(`Starting to fetch tracks for album ${albumId}`);
    
    while (hasMoreTracks) {
      try {
        // Use retry operation for track fetching
        const tracksResponse = await RetryHelper.retryOperation(
          async () => spotify.getAlbumTracks(albumId, trackLimit, trackOffset),
          3,
          2000,
          `Fetch tracks for album ${albumId} at offset ${trackOffset}`
        );
        
        // Enhanced validation of response
        if (!tracksResponse) {
          console.error(`Empty response for album tracks at offset ${trackOffset}`);
          if (++retryCount >= maxRetries) {
            console.warn(`Max retries (${maxRetries}) reached for album ${albumId} at offset ${trackOffset}, stopping`);
            break;
          }
          await new Promise(resolve => setTimeout(resolve, 2000));
          continue;
        }
        
        // Reset retry counter on success
        retryCount = 0;
        
        if (!Array.isArray(tracksResponse?.items) || tracksResponse.items.length === 0) {
          console.log(`No more tracks found for album ${albumId} at offset ${trackOffset}`);
          hasMoreTracks = false;
          break;
        }
        
        // Update total on first batch
        if (trackOffset === 0 && tracksResponse.total != null) {
          totalTracks = tracksResponse.total;
          console.log(`Album has ${totalTracks} total tracks to process`);
        }
        
        // Add retrieved tracks to our collection with better filtering
        const validTracks = tracksResponse.items.filter(item => {
          if (!item || !item.id) {
            console.warn(`Invalid track entry found in album ${albumId}`);
            return false;
          }
          return true;
        });
        
        // Check for duplicate tracks within this batch
        const existingIds = new Set(albumTracks.map(t => t.id));
        const newTracks = validTracks.filter(track => !existingIds.has(track.id));
        const duplicates = validTracks.length - newTracks.length;
        
        if (duplicates > 0) {
          console.log(`Found ${duplicates} duplicate tracks in this album batch that were filtered out`);
        }
        
        albumTracks.push(...newTracks);
        
        if (validTracks.length < tracksResponse.items.length) {
          console.warn(`Filtered out ${tracksResponse.items.length - validTracks.length} invalid track entries`);
        }
        
        trackOffset += trackLimit;
        console.log(`Fetched ${albumTracks.length}/${totalTracks} tracks for album ${albumId} so far`);
        
        // Enhanced check if there are more tracks to fetch
        hasMoreTracks = validTracks.length > 0 && 
                       (tracksResponse.next || trackOffset < totalTracks);
        
      } catch (tracksError) {
        console.error(`Error fetching tracks for album ${albumId} at offset ${trackOffset}:`, tracksError);
        
        retryCount++;
        if (retryCount >= maxRetries) {
          console.warn(`Max retries (${maxRetries}) reached for album ${albumId} at offset ${trackOffset}, moving to next offset`);
          trackOffset += trackLimit;
          retryCount = 0;
        }
        
        await supabase.rpc("log_error", {
          p_error_type: "processing",
          p_source: "process_artist",
          p_message: `Error fetching album tracks`,
          p_stack_trace: tracksError.stack || "",
          p_context: { albumId, offset: trackOffset, retryCount },
          p_item_id: albumId,
          p_item_type: "album"
        });
        
        // Short delay before retry
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        // If we couldn't get any tracks after multiple retries, break out
        if (trackOffset === 0 && retryCount >= maxRetries) {
          break;
        }
      }
    }
    
    // Final deduplication pass
    const uniqueTrackIds = new Set<string>();
    const uniqueTracks = [];
    
    for (const track of albumTracks) {
      if (!uniqueTrackIds.has(track.id)) {
        uniqueTrackIds.add(track.id);
        uniqueTracks.push(track);
      }
    }
    
    if (uniqueTracks.length !== albumTracks.length) {
      console.log(`Removed ${albumTracks.length - uniqueTracks.length} duplicate tracks from album ${albumId}`);
    }
    
    console.log(`Found ${uniqueTracks.length} unique tracks for album ${albumId}`);
    return uniqueTracks;
    
  } catch (error) {
    console.error(`Error in fetchAlbumTracks for album ${albumId}:`, error);
    return [];
  }
}

/**
 * Create or get artist record
 */
async function getOrCreateArtist(spotify: SpotifyClient, spotifyId: string): Promise<any> {
  try {
    // First check if artist exists in our database
    const { data: existingArtist, error: checkError } = await supabase
      .from("artists")
      .select("*")
      .eq("spotify_id", spotifyId)
      .single();

    if (!checkError) {
      console.log(`Artist ${existingArtist.name} already exists in the database`);
      return existingArtist;
    }

    if (checkError.code === "PGRST116") { // Not found
      // Get artist details from Spotify
      const artistData = await spotify.getArtist(spotifyId);
      
      if (!artistData || !artistData.id) {
        throw new Error(`Failed to fetch artist data from Spotify for ID: ${spotifyId}`);
      }
      
      // Insert the artist into our database
      const { data: insertedArtist, error: insertError } = await supabase
        .from("artists")
        .insert({
          name: artistData.name,
          spotify_id: artistData.id,
          genres: artistData.genres || [],
          popularity: artistData.popularity,
          spotify_url: artistData.external_urls?.spotify,
          image_url: artistData.images?.[0]?.url,
          metadata: {
            followers: artistData.followers,
            images: artistData.images,
            external_urls: artistData.external_urls,
          },
          last_processed_at: new Date().toISOString(),
        })
        .select("*")
        .single();

      if (insertError) {
        throw new Error(`Failed to insert artist: ${insertError.message}`);
      }
      
      console.log(`Inserted new artist: ${artistData.name}`);
      return insertedArtist;
    } else {
      throw new Error(`Failed to check for existing artist: ${checkError.message}`);
    }
  } catch (error) {
    console.error(`Error in getOrCreateArtist for ${spotifyId}:`, error);
    throw error;
  }
}

/**
 * Enhanced process tracks from an album function with better handling of featured artists
 */
async function processAlbumTracks(
  spotify: SpotifyClient, 
  album: any, 
  artistRecord: any, 
  artistCache: Map<string, string>,
  processedTrackIds: Set<string>,
  trackStats: {
    total: number;
    new: number;
    duplicates: number;
    primary: number;
    features: number;
    byAlbumType: Record<string, number>;
  }
): Promise<any[]> {
  const processedTracks = [];
  
  try {
    console.log(`Processing tracks for album "${album.name}" (ID: ${album.id}, Type: ${album.album_type})`);
    
    // Get all tracks for this album
    const albumTracks = await fetchAlbumTracks(spotify, album.id);
    
    if (albumTracks.length === 0) {
      console.log(`No tracks found for album "${album.name}"`);
      return [];
    }
    
    // Enhanced duplicate detection logging
    const newTracks = [];
    const duplicateTracks = [];
    
    // Filter out tracks we've already processed with better logging
    for (const track of albumTracks) {
      if (!track || !track.id) {
        console.warn(`Invalid track data in album ${album.name}`);
        continue;
      }
      
      // IMPORTANT: Only process tracks where the artist is involved
      // This is the key change to only get tracks featuring the artist on other albums
      const artistInvolved = track.artists && Array.isArray(track.artists) && 
                            track.artists.some(artist => artist.id === artistRecord.spotify_id);
      
      // Skip tracks that don't involve our artist if this is not the artist's primary album
      if (!album.is_primary_artist_album && !artistInvolved) {
        console.log(`Skipping track "${track.name}" from album "${album.name}" - artist not involved`);
        continue;
      }
      
      if (processedTrackIds.has(track.id)) {
        duplicateTracks.push(track);
      } else {
        newTracks.push(track);
        processedTrackIds.add(track.id);
      }
    }
    
    // Update statistics
    trackStats.total += albumTracks.length;
    trackStats.new += newTracks.length;
    trackStats.duplicates += duplicateTracks.length;
    
    // Track by album type
    const albumType = album.album_type || 'unknown';
    trackStats.byAlbumType[albumType] = (trackStats.byAlbumType[albumType] || 0) + newTracks.length;
    
    console.log(`Found ${newTracks.length} new tracks and ${duplicateTracks.length} duplicates in album "${album.name}"`);
    
    if (newTracks.length === 0) {
      console.log(`No new tracks to process in album "${album.name}"`);
      return [];
    }
    
    // Process tracks in smaller batches
    const trackChunks = chunkArray(newTracks, 20);
    
    for (const [chunkIndex, trackChunk] of trackChunks.entries()) {
      console.log(`Processing track chunk ${chunkIndex + 1}/${trackChunks.length} for album "${album.name}"`);
      
      try {
        // Create the track records for insertion
        const trackRecords = trackChunk.map(track => {
          if (!track || !track.id) {
            console.warn(`Invalid track data in album ${album.name}:`, track);
            return null;
          }
          
          return {
            name: track.name,
            spotify_id: track.id,
            artist_id: artistRecord.id, // Primary artist is the one we're processing
            album_name: album.name,
            release_date: album.release_date,
            spotify_url: track.external_urls?.spotify,
            preview_url: track.preview_url,
            metadata: {
              disc_number: track.disc_number,
              track_number: track.track_number,
              duration_ms: track.duration_ms,
              explicit: track.explicit,
              external_urls: track.external_urls,
              album: {
                id: album.id,
                name: album.name,
                type: album.album_type,
                total_tracks: album.total_tracks,
                release_date: album.release_date,
                release_date_precision: album.release_date_precision,
                is_primary_artist_album: album.is_primary_artist_album || false
              }
            }
          };
        }).filter(record => record !== null); // Filter out any null records
        
        if (trackRecords.length === 0) {
          console.warn(`No valid track records to insert for chunk in album "${album.name}"`);
          continue;
        }
        
        // Insert tracks with ON CONFLICT DO UPDATE
        const { data: insertedTracks, error: insertTracksError } = await supabase
          .from("tracks")
          .upsert(trackRecords, {
            onConflict: 'spotify_id',
            returning: 'id,spotify_id,name'
          });

        if (insertTracksError) {
          console.error(`Error upserting tracks:`, insertTracksError);
          continue;
        }
        
        if (!insertedTracks || insertedTracks.length === 0) {
          console.warn(`No tracks were inserted/updated for chunk in album "${album.name}"`);
          continue;
        }
        
        console.log(`Upserted ${insertedTracks.length} tracks from album "${album.name}"`);
        processedTracks.push(...insertedTracks);
        
        // Now process all artist relationships for these tracks
        for (let k = 0; k < trackChunk.length; k++) {
          const track = trackChunk[k];
          const insertedTrack = insertedTracks[k];
          
          if (!track || !insertedTrack || !Array.isArray(track.artists)) {
            console.warn(`Missing data for track-artist relationship at index ${k}`);
            continue;
          }
          
          // Process each artist on the track
          for (const [index, trackArtist] of track.artists.entries()) {
            if (!trackArtist || !trackArtist.id) {
              console.warn(`Invalid artist data for track ${track.name}:`, trackArtist);
              continue;
            }
            
            try {
              let dbArtistId;
              
              // Check if we've already seen this artist
              if (artistCache.has(trackArtist.id)) {
                dbArtistId = artistCache.get(trackArtist.id);
              } else {
                // Try to get the artist from the database
                const { data: existingTrackArtist, error: artistError } = await supabase
                  .from("artists")
                  .select("id")
                  .eq("spotify_id", trackArtist.id)
                  .single();
                
                if (artistError && artistError.code === "PGRST116") {
                  // Artist doesn't exist, so insert it with basic information
                  const { data: newArtist, error: insertArtistError } = await supabase
                    .from("artists")
                    .insert({
                      name: trackArtist.name,
                      spotify_id: trackArtist.id,
                      spotify_url: trackArtist.external_urls?.spotify,
                      metadata: {
                        external_urls: trackArtist.external_urls,
                        needs_full_processing: true // Flag to fully process this artist later
                      }
                    })
                    .select("id")
                    .single();
                  
                  if (insertArtistError) {
                    console.error(`Error inserting artist ${trackArtist.id}:`, insertArtistError);
                    continue;
                  }
                  
                  dbArtistId = newArtist.id;
                } else if (artistError) {
                  console.error(`Error checking for artist ${trackArtist.id}:`, artistError);
                  continue;
                } else {
                  dbArtistId = existingTrackArtist.id;
                }
                
                // Cache the artist ID
                artistCache.set(trackArtist.id, dbArtistId);
              }
              
              // Create the track-artist relationship
              // Is primary if it's our main artist or it's the first artist listed
              const isPrimary = trackArtist.id === artistRecord.spotify_id || index === 0;
              const { error: relationshipError } = await supabase
                .from("track_artists")
                .upsert({
                  track_id: insertedTrack.id,
                  artist_id: dbArtistId,
                  is_primary: isPrimary
                }, {
                  onConflict: 'track_id,artist_id'
                });
              
              if (relationshipError) {
                console.error(`Error creating track-artist relationship:`, relationshipError);
              } else {
                console.log(`Created track-artist relationship for "${track.name}" and "${trackArtist.name}"`);
              }
            } catch (artistError) {
              console.error(`Error processing artist relationship for track ${track.name}:`, artistError);
              // Continue with next artist
            }
          }
        }
      } catch (chunkError) {
        console.error(`Error processing track chunk ${chunkIndex + 1} for album "${album.name}":`, chunkError);
        // Continue with next chunk
      }
    }
  } catch (error) {
    console.error(`Error in processAlbumTracks for album ${album?.name || album?.id}:`, error);
  }
  
  return processedTracks;
}

// Process a specific artist by Spotify ID with enhanced validation
async function processArtist(spotifyId: string): Promise<{success: boolean, message: string, data?: any}> {
  try {
    console.log(`Processing artist with Spotify ID: ${spotifyId}`);
    
    const spotify = new SpotifyClient();
    
    // Get or create the artist record
    const artistRecord = await getOrCreateArtist(spotify, spotifyId);
    
    // Cache for artists we encounter to avoid duplicate lookups
    const artistCache = new Map<string, string>();
    artistCache.set(spotifyId, artistRecord.id);

    // Enhanced getting all artist albums from Spotify
    const allAlbums = await fetchArtistAlbums(spotify, spotifyId);
    
    console.log(`Processing ${allAlbums.length} albums for artist ${artistRecord.name}`);
    
    // Track deduplication map using Spotify IDs
    const processedTrackIds = new Set<string>();
    const processedTracks = [];
    
    // Enhanced tracking statistics
    const trackStats = {
      total: 0,
      new: 0,
      duplicates: 0,
      primary: 0,
      features: 0,
      byAlbumType: {
        album: 0,
        single: 0,
        compilation: 0,
        appears_on: 0,
        unknown: 0
      }
    };
    
    // Process albums in smaller chunks to avoid timeouts
    const albumChunkSize = 3; // Kept at 3 as per original code
    const albumChunks = chunkArray(allAlbums, albumChunkSize);
    
    console.log(`Split ${allAlbums.length} albums into ${albumChunks.length} chunks of size ${albumChunkSize}`);
    
    // Counters for error tracking
    let successfulAlbums = 0;
    let failedAlbums = 0;
    
    for (let i = 0; i < albumChunks.length; i++) {
      const albumChunk = albumChunks[i];
      console.log(`Processing album chunk ${i + 1}/${albumChunks.length}`);
      
      // Process each album in the chunk
      const albumPromises = albumChunk.map(async (album) => {
        if (!album || !album.id || !album.name) {
          console.warn(`Invalid album data:`, album);
          failedAlbums++;
          return;
        }
        
        try {
          const tracks = await processAlbumTracks(
            spotify, 
            album, 
            artistRecord, 
            artistCache, 
            processedTrackIds,
            trackStats
          );
          
          if (tracks.length > 0) {
            processedTracks.push(...tracks);
            
            // Count primary vs. feature tracks
            if (album.is_primary_artist_album) {
              trackStats.primary += tracks.length;
            } else {
              trackStats.features += tracks.length;
            }
          }
          
          successfulAlbums++;
        } catch (albumError) {
          console.error(`Error processing album ${album.id} (${album.name}):`, albumError);
          failedAlbums++;
          
          await supabase.rpc("log_error", {
            p_error_type: "processing",
            p_source: "process_artist",
            p_message: `Error processing album`,
            p_stack_trace: albumError.stack || "",
            p_context: { albumId: album.id, albumName: album.name },
            p_item_id: spotifyId,
            p_item_type: "artist"
          });
          
          // Continue with next album
        }
      });
      
      // Wait for all albums in this chunk to be processed
      await Promise.all(albumPromises);
      
      // Log progress after each chunk
      console.log(`Processed ${processedTrackIds.size} tracks so far (${i + 1}/${albumChunks.length} album chunks)`);
      console.log(`Album processing stats: ${successfulAlbums} successful, ${failedAlbums} failed`);
      
      // Add more detailed tracking progress
      console.log(`Track breakdown - Primary: ${trackStats.primary}, Features: ${trackStats.features}`);
      
      // Prevent long-running function timeouts by adding a small delay
      if (i < albumChunks.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      // If we've already gathered a large number of tracks, break to avoid timeout
      if (processedTrackIds.size > 10000) {
        console.warn(`Reached large track count (${processedTrackIds.size}), stopping album processing to avoid timeout`);
        break;
      }
    }

    // Update the artist's last_processed_at timestamp
    await supabase
      .from("artists")
      .update({
        last_processed_at: new Date().toISOString()
      })
      .eq("spotify_id", spotifyId);

    // Enhanced statistics reporting
    console.log(`=== ARTIST PROCESSING STATS FOR ${artistRecord.name} ===`);
    console.log(`Total unique tracks: ${processedTrackIds.size}`);
    console.log(`Tracks stored in database: ${processedTracks.length}`);
    console.log(`Albums processed: ${successfulAlbums} successful, ${failedAlbums} failed`);
    console.log(`Primary artist tracks: ${trackStats.primary}`);
    console.log(`Feature tracks: ${trackStats.features}`);
    console.log(`Tracks by album type: ${JSON.stringify(trackStats.byAlbumType, null, 2)}`);
    console.log(`Total tracks seen: ${trackStats.total} (${trackStats.new} new, ${trackStats.duplicates} duplicates)`);
    
    // Verification stage - making a direct count query to verify database state
    try {
      const { count: dbTrackCount, error: countError } = await supabase
        .from("tracks")
        .select("*", { count: "exact", head: true })
        .eq("artist_id", artistRecord.id);
        
      if (!countError) {
        console.log(`Database verification: ${dbTrackCount} tracks for this artist in database`);
        
        if (dbTrackCount !== processedTracks.length) {
          console.log(`Note: Database count (${dbTrackCount}) differs from tracks stored in this session (${processedTracks.length}). This may indicate tracks from previous runs.`);
        }
      }
    } catch (verificationError) {
      console.error(`Error verifying track count in database:`, verificationError);
    }

    // Prepare a reduced response to avoid response size limits
    const summaryData = {
      artist: artistRecord.name,
      artistId: artistRecord.id,
      tracksProcessed: processedTrackIds.size,
      tracksStored: processedTracks.length,
      albumsProcessed: {
        total: allAlbums.length,
        successful: successfulAlbums,
        failed: failedAlbums
      },
      trackStats: {
        primary: trackStats.primary,
        features: trackStats.features,
        byAlbumType: trackStats.byAlbumType,
        total: trackStats.total,
        new: trackStats.new,
        duplicates: trackStats.duplicates
      }
      // Do NOT include track details to avoid response size issues
    };

    return {
      success: true,
      message: `Successfully processed artist ${artistRecord.name} with ${processedTrackIds.size} unique tracks`,
      data: summaryData
    };
  } catch (error) {
    console.error(`Error processing artist ${spotifyId}:`, error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "processing",
        p_source: "process_artist",
        p_message: `Error processing artist`,
        p_stack_trace: error.stack || "",
        p_context: { spotifyId },
        p_item_id: spotifyId,
        p_item_type: "artist"
      });
    } catch (logError) {
      console.error("Failed to log error to database:", logError);
    }
    
    return {
      success: false,
      message: `Error processing artist: ${error instanceof Error ? error.message : "Unknown error"}`
    };
  }
}

// Process all artists in a batch
async function processBatch(batchId: string): Promise<{
  success: boolean, 
  message: string, 
  processed: number,
  failed: number,
  data?: any
}> {
  try {
    console.log(`Processing all artists in batch: ${batchId}`);
    
    // Get all pending artist items from the batch
    const { data: items, error: itemsError } = await supabase
      .from("processing_items")
      .select("*")
      .eq("batch_id", batchId)
      .eq("status", "pending")
      .eq("item_type", "artist");
    
    if (itemsError) {
      throw new Error(`Failed to get items from batch: ${itemsError.message}`);
    }
    
    if (!items || items.length === 0) {
      return {
        success: true,
        message: "No pending artist items found in the batch",
        processed: 0,
        failed: 0
      };
    }
    
    console.log(`Found ${items.length} pending artists to process in batch ${batchId}`);
    
    const processed = [];
    const failed = [];
    
    // Process artists in smaller chunks to avoid timeouts
    const artistChunkSize = 3; // Reduced from 5 to 3
    const itemChunks = chunkArray(items, artistChunkSize);
    
    for (let i = 0; i < itemChunks.length; i++) {
      const itemChunk = itemChunks[i];
      console.log(`Processing artist chunk ${i + 1}/${itemChunks.length}`);
      
      // Process each artist in the chunk
      for (const item of itemChunk) {
        if (!item || !item.item_id) {
          console.warn(`Invalid item in batch ${batchId}:`, item);
          continue;
        }
        
        try {
          console.log(`Processing item ${item.id}: artist with Spotify ID ${item.item_id}`);
          
          // Process the artist
          const result = await processArtist(item.item_id);
          
          if (result.success) {
            // Mark item as processed with completed status
            await supabase
              .from("processing_items")
              .update({
                status: "completed",
                metadata: {
                  ...item.metadata,
                  result
                }
              })
              .eq("id", item.id);
            
            processed.push(item.id);
          } else {
            // Mark as error
            await supabase
              .from("processing_items")
              .update({
                status: "error",
                retry_count: (item.retry_count || 0) + 1,
                last_error: result.message
              })
              .eq("id", item.id);
            
            failed.push(item.id);
          }
        } catch (itemError) {
          console.error(`Error processing item ${item.id}:`, itemError);
          
          // Mark as error
          await supabase
            .from("processing_items")
            .update({
              status: "error",
              retry_count: (item.retry_count || 0) + 1,
              last_error: itemError.message
            })
            .eq("id", item.id);
          
          // Log error
          await supabase.rpc("log_error", {
            p_error_type: "processing",
            p_source: "process_artist",
            p_message: `Error processing artist`,
            p_stack_trace: itemError.stack || "",
            p_context: item,
            p_item_id: item.item_id,
            p_item_type: item.item_type
          });
          
          failed.push(item.id);
        }
      }
      
      // Update batch progress after each chunk to reflect current status
      await updateBatchProgress(batchId);
    }
    
    // Final update of the batch with accurate progress
    await updateBatchProgress(batchId);
    
    // Check if all items are processed
    const { count: pendingItems } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "pending");
    
    // If no pending items, mark batch as completed
    if (pendingItems === 0) {
      await supabase.rpc(
        "release_processing_batch",
        {
          p_batch_id: batchId,
          p_worker_id: crypto.randomUUID(), // Using a random UUID as worker ID
          p_status: "completed"
        }
      );
    }
    
    return {
      success: true,
      message: `Processed ${processed.length} artists, failed ${failed.length} artists`,
      processed: processed.length,
      failed: failed.length,
      data: {
        processedItems: processed,
        failedItems: failed
      }
    };
  } catch (error) {
    console.error(`Error processing batch ${batchId}:`, error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "processing",
        p_source: "process_artist",
        p_message: `Error processing batch`,
        p_stack_trace: error.stack || "",
        p_context: { batchId },
        p_item_id: batchId,
        p_item_type: "batch"
      });
    } catch (logError) {
      console.error("Error logging to database:", logError);
    }
    
    return {
      success: false,
      message: `Error processing batch: ${error.message}`,
      processed: 0,
      failed: 0
    };
  }
}

// Helper function to update batch progress
async function updateBatchProgress(batchId: string): Promise<void> {
  try {
    // Get accurate counts from the database for this batch
    const { count: totalItems } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId);
    
    const { count: completedItems } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "completed");
    
    const { count: errorItems } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "error");
    
    // Update the batch with accurate progress
    await supabase
      .from("processing_batches")
      .update({
        items_total: totalItems || 0,
        items_processed: completedItems || 0,
        items_failed: errorItems || 0,
      })
      .eq("id", batchId);
  } catch (error) {
    console.error(`Error updating batch progress for ${batchId}:`, error);
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Parse request body
    const requestBody = await req.json();
    const { spotifyId, batchId } = requestBody;
    
    // Check if we're processing a batch or a single artist
    if (batchId) {
      // Process all artists in a batch
      const result = await processBatch(batchId);
      
      // Ensure response is properly formatted as JSON
      try {
        const safeResponse = {
          success: result.success,
          message: result.message,
          processed: result.processed,
          failed: result.failed,
          // Minimize data to avoid large responses
          data: result.processed > 0 ? {
            processedCount: result.processed,
            failedCount: result.failed
          } : null
        };
        
        return new Response(
          JSON.stringify(safeResponse),
          {
            status: result.success ? 200 : 500,
            headers: {
              "Content-Type": "application/json",
              ...corsHeaders,
            },
          }
        );
      } catch (jsonError) {
        console.error("Error serializing batch result:", jsonError);
        return new Response(
          JSON.stringify({
            success: false,
            message: "Error serializing response: " + (jsonError instanceof Error ? jsonError.message : String(jsonError))
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
    } else if (spotifyId) {
      // Process a single artist (maintaining backward compatibility)
      const result = await processArtist(spotifyId);
      
      // Ensure we return a valid JSON response with a reduced size
      try {
        // Create a minimal response object to avoid serialization issues
        const safeResponse = {
          success: result.success,
          message: result.message,
          data: result.data ? {
            artist: result.data.artist,
            artistId: result.data.artistId,
            tracksProcessed: result.data.tracksProcessed,
            tracksStored: result.data.tracksStored,
            // Include only summary info for track stats, not the full objects
            trackStats: {
              primary: result.data.trackStats?.primary || 0,
              features: result.data.trackStats?.features || 0,
              total: result.data.trackStats?.total || 0,
              new: result.data.trackStats?.new || 0,
              duplicates: result.data.trackStats?.duplicates || 0,
              byAlbumType: result.data.trackStats?.byAlbumType || {}
            },
            albumsProcessed: {
              total: result.data.albumsProcessed?.total || 0,
              successful: result.data.albumsProcessed?.successful || 0,
              failed: result.data.albumsProcessed?.failed || 0
            }
          } : null
        };
        
        return new Response(
          JSON.stringify(safeResponse),
          {
            status: result.success ? 200 : 500,
            headers: {
              "Content-Type": "application/json",
              ...corsHeaders,
            },
          }
        );
      } catch (jsonError) {
        console.error("Error serializing artist result:", jsonError);
        // Fall back to an even more minimal response if serialization fails
        return new Response(
          JSON.stringify({
            success: true,
            message: "Processing completed but response was too large to serialize. Check logs for details.",
            error: String(jsonError)
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "application/json",
              ...corsHeaders,
            },
          }
        );
      }
    } else {
      return new Response(
        JSON.stringify({
          success: false,
          error: "Either spotifyId or batchId is required",
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
  } catch (error) {
    console.error("Error handling process-artist request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "process_artist",
        p_message: "Error handling process-artist request",
        p_stack_trace: error instanceof Error ? error.stack : "",
        p_context: { error: error instanceof Error ? error.message : String(error) },
      });
    } catch (logError) {
      console.error("Error logging to database:", logError);
    }

    // Return a minimal error response that's guaranteed to serialize properly
    return new Response(
      JSON.stringify({
        success: false,
        error: error instanceof Error ? error.message : "Unknown error occurred",
        errorType: error instanceof Error ? error.constructor.name : typeof error
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
