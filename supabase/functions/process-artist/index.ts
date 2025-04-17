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
 * Safe serialization helper function to handle circular references and complex objects
 */
function safeSerialize(obj: any): any {
  if (obj === null || obj === undefined) {
    return null;
  }
  
  // Handle Error objects
  if (obj instanceof Error) {
    return {
      message: obj.message,
      name: obj.name,
      stack: obj.stack
    };
  }
  
  // Handle arrays
  if (Array.isArray(obj)) {
    return obj.map(item => safeSerialize(item));
  }
  
  // Handle dates by converting to ISO strings
  if (obj instanceof Date) {
    return obj.toISOString();
  }
  
  // Handle objects
  if (typeof obj === 'object') {
    const result: Record<string, any> = {};
    
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        try {
          // Skip deeply nested or circular references
          if (key === 'parent' || key === 'children' || key === '_reactInternals') {
            continue;
          }
          
          // Serialize each property safely
          result[key] = safeSerialize(obj[key]);
        } catch (e) {
          result[key] = `[Unable to serialize: ${e.message}]`;
        }
      }
    }
    
    return result;
  }
  
  // Return primitives as is
  return obj;
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
 * Format partial release dates from Spotify to valid PostgreSQL date format
 * Handles cases where Spotify returns only year or year-month
 */
function formatReleaseDate(releaseDate: string | null): string | null {
  if (!releaseDate) return null;
  
  // Check release date format - Spotify uses three different precisions:
  // year: "2012", year-month: "2012-03", full-date: "2012-03-25"
  const dateParts = releaseDate.split('-');
  
  if (dateParts.length === 1 && dateParts[0].length === 4) {
    // Year only format (e.g., "2012") - append "-01-01" to make it January 1st
    console.log(`Converting year-only date "${releaseDate}" to "${releaseDate}-01-01"`);
    return `${releaseDate}-01-01`;
  } else if (dateParts.length === 2) {
    // Year-month format (e.g., "2012-03") - append "-01" to make it the 1st day of the month
    console.log(`Converting year-month date "${releaseDate}" to "${releaseDate}-01"`);
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

/**
 * Create or get album record with the new database structure
 * Now includes handling for partial release dates
 */
async function getOrCreateAlbum(album: any, artistRecord: any): Promise<any> {
  try {
    console.log(`Checking if album ${album.name} (${album.id}) exists in database`);
    
    // First check if album exists in our database
    const { data: existingAlbum, error: checkError } = await supabase
      .from("albums")
      .select("*")
      .eq("spotify_id", album.id)
      .single();

    if (!checkError) {
      console.log(`Album ${existingAlbum.name} already exists in the database with ID: ${existingAlbum.id}`);
      return existingAlbum;
    }

    if (checkError.code === "PGRST116") { // Not found
      console.log(`Album ${album.name} not found, inserting new record`);
      
      // Format the release date properly to handle Spotify's various date formats
      const formattedReleaseDate = formatReleaseDate(album.release_date);
      
      // Insert the album into our database
      const { data: insertedAlbum, error: insertError } = await supabase
        .from("albums")
        .insert({
          name: album.name,
          spotify_id: album.id,
          release_date: formattedReleaseDate, // Use the formatted date
          album_type: album.album_type,
          total_tracks: album.total_tracks,
          image_url: album.images?.[0]?.url,
          spotify_url: album.external_urls?.spotify,
          artist_id: artistRecord.id,
          is_primary_artist_album: album.is_primary_artist_album || false,
          popularity: album.popularity,
          metadata: {
            album_group: album.album_group,
            available_markets: album.available_markets,
            external_urls: album.external_urls,
            href: album.href,
            images: album.images,
            release_date_precision: album.release_date_precision,
            restrictions: album.restrictions,
            uri: album.uri
          }
        })
        .select("*")
        .single();

      if (insertError) {
        console.error(`Failed to insert album: ${insertError.message}`, insertError);
        throw new Error(`Failed to insert album: ${insertError.message}`);
      }
      
      console.log(`Inserted new album: ${album.name} with database ID: ${insertedAlbum.id}`);
      return insertedAlbum;
    } else {
      console.error(`Failed to check for existing album: ${checkError.message}`, checkError);
      throw new Error(`Failed to check for existing album: ${checkError.message}`);
    }
  } catch (error) {
    console.error(`Error in getOrCreateAlbum for ${album.name} (${album.id}):`, error);
    throw error;
  }
}

/**
 * Process a single album with its tracks atomically
 * This isolates the processing of each album to avoid issues with long-running operations
 */
async function processAlbumWithTracks(
  spotify: SpotifyClient,
  albumData: any,
  artistRecord: any,
  artistCache: Map<string, string>,
  processedTrackIds: Set<string> = new Set()
): Promise<{
  success: boolean;
  tracksProcessed: number;
  message?: string;
  error?: any;
}> {
  try {
    console.log(`Processing album "${albumData.name}" (ID: ${albumData.id})`);
    
    // Store the album in database
    const albumRecord = await getOrCreateAlbum(albumData, artistRecord);
    
    // Get all tracks for this album
    const albumTracks = await fetchAlbumTracks(spotify, albumData.id);
    
    if (albumTracks.length === 0) {
      console.log(`No tracks found for album "${albumData.name}"`);
      return { success: true, tracksProcessed: 0 };
    }
    
    // Filter tracks to only include new ones we haven't processed yet
    const newTracks = albumTracks.filter(track => !processedTrackIds.has(track.id));
    console.log(`Found ${newTracks.length} new tracks and ${albumTracks.length - newTracks.length} duplicates in album "${albumData.name}"`);
    
    // Mark all tracks as processed regardless of whether we actually insert them
    // This prevents duplicate processing
    albumTracks.forEach(track => {
      if (track && track.id) {
        processedTrackIds.add(track.id);
      }
    });
    
    // If this is not the artist's primary album, only include tracks where the artist appears
    const relevantTracks = albumData.is_primary_artist_album ? 
      newTracks : 
      newTracks.filter(track => {
        return track.artists && 
               Array.isArray(track.artists) && 
               track.artists.some(artist => artist.id === artistRecord.spotify_id);
      });
      
    if (relevantTracks.length === 0) {
      console.log(`No relevant tracks in album "${albumData.name}"`);
      return { success: true, tracksProcessed: 0 };
    }
    
    console.log(`Processing ${relevantTracks.length} relevant tracks for album "${albumData.name}"`);
    
    // Process tracks in smaller batches
    const trackChunks = chunkArray(relevantTracks, 20);
    let totalProcessed = 0;
    
    for (const [chunkIndex, trackChunk] of trackChunks.entries()) {
      console.log(`Processing track chunk ${chunkIndex + 1}/${trackChunks.length} for album "${albumData.name}"`);
      
      // Create track records and collect their IDs for batch association
      const trackRecordsToInsert = [];
      
      for (const track of trackChunk) {
        if (!track || !track.id) {
          console.warn(`Invalid track data in album ${albumData.name}`);
          continue;
        }
        
        trackRecordsToInsert.push({
          name: track.name,
          spotify_id: track.id,
          artist_id: artistRecord.id,
          spotify_url: track.external_urls?.spotify,
          preview_url: track.preview_url,
          release_date: albumData.release_date,
          metadata: {
            disc_number: track.disc_number,
            track_number: track.track_number,
            duration_ms: track.duration_ms,
            explicit: track.explicit,
            external_urls: track.external_urls
          }
        });
      }
      
      if (trackRecordsToInsert.length === 0) {
        console.warn(`No valid track records to insert for chunk in album "${albumData.name}"`);
        continue;
      }
      
      // Insert tracks with ON CONFLICT DO UPDATE
      const { data: insertedTracks, error: insertTracksError } = await supabase
        .from("tracks")
        .upsert(trackRecordsToInsert, {
          onConflict: 'spotify_id',
          returning: 'id,spotify_id,name'
        });

      if (insertTracksError) {
        console.error(`Error upserting tracks:`, insertTracksError);
        throw insertTracksError;
      }
      
      if (!insertedTracks || insertedTracks.length === 0) {
        console.warn(`No tracks were inserted/updated for chunk in album "${albumData.name}"`);
        continue;
      }
      
      console.log(`Upserted ${insertedTracks.length} tracks from album "${albumData.name}"`);
      
      // Create track-album relationships
      console.log(`Creating track-album relationships for album ID: ${albumRecord.id}`);
      
      // Prepare track-album relationships for batch insertion
      const trackAlbumRecords = [];
      
      for (let i = 0; i < trackChunk.length; i++) {
        const track = trackChunk[i];
        // Find the corresponding inserted track
        const insertedTrack = insertedTracks.find(t => t.spotify_id === track.id);
        
        if (!insertedTrack || !insertedTrack.id) {
          console.warn(`Could not find inserted track record for Spotify ID: ${track.id}`);
          continue;
        }
        
        trackAlbumRecords.push({
          track_id: insertedTrack.id,
          album_id: albumRecord.id,
          track_number: track.track_number || null,
          disc_number: track.disc_number || null
        });
        
        console.log(`Prepared track-album relationship: Track "${track.name}" -> Album "${albumData.name}"`);
      }
      
      if (trackAlbumRecords.length === 0) {
        console.warn(`No track-album relationships to create for album "${albumData.name}"`);
        continue;
      }
      
      // Insert track-album relationships in smaller batches
      const relationshipBatches = chunkArray(trackAlbumRecords, 25);
      
      for (const [batchIndex, relationshipBatch] of relationshipBatches.entries()) {
        console.log(`Inserting batch ${batchIndex + 1}/${relationshipBatches.length} of track-album relationships`);
        
        const { data: insertedRelationships, error: relationshipError } = await supabase
          .from("track_albums")
          .upsert(relationshipBatch, {
            onConflict: 'track_id,album_id',
            returning: 'id'
          });
          
        if (relationshipError) {
          console.error(`Error creating track-album relationships (batch ${batchIndex + 1}):`, relationshipError);
          throw relationshipError;
        }
        
        console.log(`Successfully created ${insertedRelationships?.length || 0} track-album relationships`);
      }
      
      // Process artist relationships for each track
      for (const track of trackChunk) {
        if (!track || !Array.isArray(track.artists)) {
          continue;
        }
        
        // Find the inserted track ID
        const insertedTrack = insertedTracks.find(t => t.spotify_id === track.id);
        if (!insertedTrack) continue;
        
        // Process each artist
        for (const [index, trackArtist] of track.artists.entries()) {
          if (!trackArtist || !trackArtist.id) continue;
          
          try {
            let dbArtistId;
            
            // Check artist cache first
            if (artistCache.has(trackArtist.id)) {
              dbArtistId = artistCache.get(trackArtist.id);
            } else {
              // Get or create artist
              const { data: existingArtist, error: artistError } = await supabase
                .from("artists")
                .select("id")
                .eq("spotify_id", trackArtist.id)
                .single();
              
              if (artistError && artistError.code === "PGRST116") {
                // Create new artist with basic details
                const { data: newArtist, error: insertError } = await supabase
                  .from("artists")
                  .insert({
                    name: trackArtist.name,
                    spotify_id: trackArtist.id,
                    spotify_url: trackArtist.external_urls?.spotify,
                    metadata: {
                      external_urls: trackArtist.external_urls,
                      needs_full_processing: true
                    }
                  })
                  .select("id")
                  .single();
                
                if (insertError) {
                  console.error(`Error inserting artist ${trackArtist.name}:`, insertError);
                  continue;
                }
                
                dbArtistId = newArtist.id;
              } else if (artistError) {
                console.error(`Error checking for artist ${trackArtist.name}:`, artistError);
                continue;
              } else {
                dbArtistId = existingArtist.id;
              }
              
              // Cache the artist ID
              artistCache.set(trackArtist.id, dbArtistId);
            }
            
            // Create track-artist relationship
            const isPrimary = trackArtist.id === artistRecord.spotify_id || index === 0;
            await supabase
              .from("track_artists")
              .upsert({
                track_id: insertedTrack.id,
                artist_id: dbArtistId,
                is_primary: isPrimary
              }, {
                onConflict: 'track_id,artist_id'
              });
          } catch (artistError) {
            console.error(`Error processing artist relationship:`, artistError);
            // Continue with next artist
          }
        }
      }
      
      totalProcessed += insertedTracks.length;
    }
    
    // Verification query
    try {
      const { count, error: countError } = await supabase
        .from("track_albums")
        .select("*", { count: "exact", head: true })
        .eq("album_id", albumRecord.id);
      
      if (countError) {
        console.error(`Error verifying track-album relationships:`, countError);
      } else {
        console.log(`Verification: Found ${count} track-album relationships for album "${albumData.name}"`);
        
        if (count === 0 && totalProcessed > 0) {
          console.error(`WARNING: No track-album relationships were created despite processing ${totalProcessed} tracks!`);
        }
      }
    } catch (verificationError) {
      console.error(`Error during verification:`, verificationError);
    }
    
    return {
      success: true,
      tracksProcessed: totalProcessed,
      message: `Successfully processed ${totalProcessed} tracks for album "${albumData.name}"`
    };
  } catch (error) {
    console.error(`Error processing album ${albumData?.name || albumData?.id}:`, error);
    return {
      success: false,
      tracksProcessed: 0,
      message: `Error processing album: ${error.message}`,
      error
    };
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
 * Background processing function that processes albums in a batch
 * This function is designed to run in the background using EdgeRuntime.waitUntil
 */
async function processBatchInBackground(
  batchId: string, 
  artistRecord: any, 
  processedTrackIds: Set<string> = new Set()
): Promise<void> {
  try {
    console.log(`Starting background processing for batch ${batchId}`);
    
    const spotify = new SpotifyClient();
    
    // Artist cache to avoid duplicate lookups
    const artistCache = new Map<string, string>();
    artistCache.set(artistRecord.spotify_id, artistRecord.id);
    
    // Get pending items from the batch
    let hasMoreItems = true;
    let processedItems = 0;
    let failedItems = 0;
    
    while (hasMoreItems) {
      // Get a small batch of pending items
      const { data: pendingItems, error: itemsError } = await supabase
        .from("processing_items")
        .select("*")
        .eq("batch_id", batchId)
        .eq("status", "pending")
        .order("priority", { ascending: false })
        .limit(5); // Process 5 albums at a time to avoid timeouts
        
      if (itemsError) {
        console.error(`Error fetching pending items:`, itemsError);
        break;
      }
      
      if (!pendingItems || pendingItems.length === 0) {
        console.log(`No more pending items to process for batch ${batchId}`);
        hasMoreItems = false;
        break;
      }
      
      console.log(`Processing batch of ${pendingItems.length} albums in background`);
      
      // Process each item sequentially
      for (const item of pendingItems) {
        try {
          console.log(`Processing item ${item.id}: Album ID ${item.item_id}`);
          
          // Get the album details from Spotify
          const albumData = await spotify.getAlbum(item.item_id);
          
          // Add the is_primary_artist_album flag
          albumData.is_primary_artist_album = item.metadata.is_primary_artist_album || false;
          
          // Process the album
          const result = await processAlbumWithTracks(
            spotify,
            albumData,
            artistRecord,
            artistCache,
            processedTrackIds
          );
          
          if (result.success) {
            // Mark item as processed
            await supabase
              .from("processing_items")
              .update({
                status: "completed",
                metadata: {
                  ...item.metadata,
                  completed_at: new Date().toISOString(),
                  tracks_processed: result.tracksProcessed
                }
              })
              .eq("id", item.id);
              
            processedItems++;
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
              
            failedItems++;
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
            
          failedItems++;
        }
        
        // Update batch progress
        await updateBatchProgress(batchId);
        
        // Add a small delay between items to avoid API rate limits
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    // Check if all items are processed
    const { count: pendingCount } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "pending");
      
    if (pendingCount === 0) {
      // Mark batch as completed
      await supabase
        .from("processing_batches")
        .update({
          status: "completed",
          completed_at: new Date().toISOString()
        })
        .eq("id", batchId);
        
      console.log(`Background processing completed for batch ${batchId}`);
    } else {
      console.log(`Background processing stopped with ${pendingCount} items still pending for batch ${batchId}`);
    }
  } catch (error) {
    console.error(`Error in background processing for batch ${batchId}:`, error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "processing",
        p_source: "process_artist_background",
        p_message: `Error in background processing`,
        p_stack_trace: error.stack || "",
        p_context: { batchId },
        p_item_id: batchId,
        p_item_type: "batch"
      });
    } catch (logError) {
      console.error("Failed to log error to database:", logError);
    }
  }
}

/**
 * Helper function to update batch progress
 */
async function updateBatchProgress(batchId: string): Promise<void> {
  try {
    // Get accurate counts from the database
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
    
    // Get total tracks processed
    const { data: completedItemsData } = await supabase
      .from("processing_items")
      .select("metadata")
      .eq("batch_id", batchId)
      .eq("status", "completed");
      
    let totalTracksProcessed = 0;
    if (completedItemsData) {
      completedItemsData.forEach(item => {
        if (item.metadata && typeof item.metadata.tracks_processed === 'number') {
          totalTracksProcessed += item.metadata.tracks_processed;
        }
      });
    }
    
    // Update the batch with accurate progress
    await supabase
      .from("processing_batches")
      .update({
        items_total: totalItems || 0,
        items_processed: completedItems || 0,
        items_failed: errorItems || 0,
        metadata: {
          tracks_processed: totalTracksProcessed
        }
      })
      .eq("id", batchId);
  } catch (error) {
    console.error(`Error updating batch progress for ${batchId}:`, error);
  }
}

/**
 * Enhanced process tracks from an album function
 * Redesigned to use job-based architecture with background processing
 */
async function processArtist(spotifyId: string): Promise<{success: boolean, message: string, data?: any}> {
  try {
    console.log(`Processing artist with Spotify ID: ${spotifyId}`);
    
    const spotify = new SpotifyClient();
    
    // Get or create the artist record
    const artistRecord = await getOrCreateArtist(spotify, spotifyId);
    
    // Create a processing batch for this artist
    const { data: newBatch, error: batchError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "process_artist",
        status: "processing",
        metadata: {
          artist_id: artistRecord.id,
          artist_name: artistRecord.name,
          spotify_id: spotifyId,
          started_at: new Date().toISOString()
        }
      })
      .select("id")
      .single();
      
    if (batchError) {
      throw new Error(`Failed to create processing batch: ${batchError.message}`);
    }
    
    const batchId = newBatch.id;
    console.log(`Created processing batch ${batchId} for artist ${artistRecord.name}`);
    
    // Cache for artists we encounter to avoid duplicate lookups
    const artistCache = new Map<string, string>();
    artistCache.set(spotifyId, artistRecord.id);

    // Get artist albums from Spotify
    const allAlbums = await fetchArtistAlbums(spotify, spotifyId);
    
    console.log(`Found ${allAlbums.length} total albums for artist ${artistRecord.name}`);
    
    // Track which tracks we've already processed
    const processedTrackIds = new Set<string>();
    
    // Update batch with total albums
    await supabase
      .from("processing_batches")
      .update({
        items_total: allAlbums.length,
        metadata: {
          ...newBatch.metadata,
          total_albums: allAlbums.length
        }
      })
      .eq("id", batchId);
    
    // Split albums into initial batch and remainder for background processing
    const initialBatchSize = 3; // Process 3 albums immediately for responsive feedback
    const initialBatch = allAlbums.slice(0, initialBatchSize);
    const remainingAlbums = allAlbums.slice(initialBatchSize);
    
    console.log(`Processing initial batch of ${initialBatch.length} albums immediately`);
    
    // Process initial batch albums immediately (for API responsiveness)
    let initialTracksProcessed = 0;
    let initialAlbumsProcessed = 0;
    
    for (const album of initialBatch) {
      try {
        const result = await processAlbumWithTracks(
          spotify, 
          album, 
          artistRecord, 
          artistCache,
          processedTrackIds
        );
        
        if (result.success) {
          initialTracksProcessed += result.tracksProcessed;
          initialAlbumsProcessed++;
          
          // Update batch progress
          await supabase
            .from("processing_batches")
            .update({
              items_processed: initialAlbumsProcessed,
              metadata: {
                ...newBatch.metadata,
                tracks_processed: initialTracksProcessed,
                albums_processed: initialAlbumsProcessed
              }
            })
            .eq("id", batchId);
        }
      } catch (albumError) {
        console.error(`Error processing album ${album.name} in initial batch:`, albumError);
        // Continue with next album
      }
    }
    
    // Store remaining albums in processing queue for background processing
    if (remainingAlbums.length > 0) {
      console.log(`Queueing ${remainingAlbums.length} albums for background processing`);
      
      // Create processing items for remaining albums
      const processingItems = remainingAlbums.map((album, index) => ({
        batch_id: batchId,
        item_type: "album",
        item_id: album.id,
        status: "pending",
        priority: album.is_primary_artist_album ? 10 : 5, // Prioritize primary albums
        metadata: {
          name: album.name,
          is_primary_artist_album: album.is_primary_artist_album || false,
          album_type: album.album_type || "unknown",
          position: index
        }
      }));
      
      // Insert in smaller batches to avoid request size limits
      const itemBatches = chunkArray(processingItems, 50);
      
      for (const [batchIndex, itemBatch] of itemBatches.entries()) {
        console.log(`Inserting processing items batch ${batchIndex + 1}/${itemBatches.length}`);
        
        const { error: insertError } = await supabase
          .from("processing_items")
          .insert(itemBatch);
          
        if (insertError) {
          console.error(`Error inserting processing items batch ${batchIndex + 1}:`, insertError);
        }
      }
      
      // Start background processing
      console.log(`Starting background processing for remaining ${remainingAlbums.length} albums`);
      
      // Use EdgeRuntime.waitUntil to continue processing in the background
      EdgeRuntime.waitUntil(
        processBatchInBackground(batchId, artistRecord, processedTrackIds)
      );
    } else {
      // Mark the batch as completed since we've processed all albums
      await supabase
        .from("processing_batches")
        .update({
          status: "completed",
          completed_at: new Date().toISOString()
        })
        .eq("id", batchId);
    }
    
    // Update the artist's last_processed_at timestamp
    await supabase
      .from("artists")
      .update({
        last_processed_at: new Date().toISOString()
      })
      .eq("spotify_id", spotifyId);
    
    // Return a response with minimal data to avoid serialization issues
    return {
      success: true,
      message: `Processing artist ${artistRecord.name}: ${initialAlbumsProcessed} albums processed immediately, ${remainingAlbums.length} queued for background processing`,
      data: {
        artist: artistRecord.name,
        artistId: artistRecord.id,
        totalAlbums: allAlbums.length,
        processedImmediately: initialAlbumsProcessed,
        tracksProcessedImmediately: initialTracksProcessed,
        albumsQueuedForBackground: remainingAlbums.length,
        batchId: batchId
      }
    };
  } catch (error) {
    console.error(`Error in processArtist for ${spotifyId}:`, error);
    
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

// Process a batch of artists
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
                  result: {
                    success: result.success,
                    message: result.message,
                    summary: {
                      tracksProcessed: result.data?.tracksProcessed || 0,
                      albumsProcessed: result.data?.albumsProcessed?.total || 0
                    }
                  }
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
        processedItems: processed.length,
        failedItems: failed.length
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
      
      // Use safe serialization to avoid JSON issues
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
          JSON.stringify(safeSerialize(safeResponse)),
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
      // Process a single artist
      const result = await processArtist(spotifyId);
      
      // Use safe serialization to avoid JSON issues
      try {
        // Create a minimal response that's guaranteed to serialize properly
        const safeResponse = {
          success: result.success,
          message: result.message,
          data: result.data ? safeSerialize({
            artist: result.data.artist || "",
            artistId: result.data.artistId || "",
            totalAlbums: result.data.totalAlbums || 0,
            processedImmediately: result.data.processedImmediately || 0,
            tracksProcessedImmediately: result.data.tracksProcessedImmediately || 0,
            albumsQueuedForBackground: result.data.albumsQueuedForBackground || 0,
            batchId: result.data.batchId || null
          }) : null
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
        
        // Fall back to an even more minimal response
        return new Response(
          JSON.stringify({
            success: true,
            message: "Processing started but response was too large to serialize. Check logs for details.",
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

    // Return a minimal error response
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
