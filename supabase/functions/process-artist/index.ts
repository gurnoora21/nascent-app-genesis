
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
 * Fetch just a small initial batch of albums for immediate processing
 */
async function fetchInitialArtistAlbums(
  spotify: SpotifyClient, 
  spotifyId: string, 
  limit: number = 3
): Promise<any[]> {
  try {
    console.log(`Fetching initial ${limit} albums for artist ${spotifyId}`);
    
    const albumsResponse = await RetryHelper.retryOperation(
      async () => spotify.getArtistAlbums(
        spotifyId, 
        limit, 
        0, 
        "album,single" // Primary albums first
      ),
      3,
      2000,
      `Fetch initial albums`
    );
    
    if (!albumsResponse || !Array.isArray(albumsResponse?.items)) {
      console.log(`No albums found for artist ${spotifyId}`);
      return [];
    }
    
    // Filter valid albums and mark as primary
    const validAlbums = albumsResponse.items.filter(item => {
      return item && item.id;
    }).map(album => {
      album.is_primary_artist_album = true;
      return album;
    });
    
    console.log(`Found ${validAlbums.length} initial albums for artist ${spotifyId}`);
    return validAlbums;
  } catch (error) {
    console.error(`Error fetching initial albums for ${spotifyId}:`, error);
    return [];
  }
}

/**
 * Enhanced process artist albums helper function with improved pagination
 * Modified to better handle appears_on and compilation albums
 * IMPROVED: Added global deduplication across offset pages
 */
async function fetchRemainingArtistAlbums(spotify: SpotifyClient, spotifyId: string, offset: number = 0): Promise<any[]> {
  let allAlbums = [];
  const limit = 50; // Maximum allowed by Spotify API
  let hasMore = true;
  let totalAlbums = 0;
  let retryCount = 0;
  const maxRetries = 3;
  let failedBatches = 0;
  
  // Use a Set for global deduplication across all pagination requests
  const processedAlbumIds = new Set<string>();
  
  // Process albums - artist's own albums
  while (hasMore && failedBatches < 5) {
    try {
      console.log(`Fetching album batch at offset ${offset} for artist ${spotifyId}`);
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
      
      // Global deduplication improvement
      const newAlbums = validAlbums.filter(album => {
        // Skip already processed album IDs
        if (processedAlbumIds.has(album.id)) {
          return false;
        }
        // Add to processed set
        processedAlbumIds.add(album.id);
        return true;
      });
      
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
      
      // Improved check for pagination completion
      hasMore = validAlbums.length > 0 && 
               (albumsResponse.next || offset < totalAlbums) &&
               // Add a check to make sure we're not hitting a loop
               newAlbums.length > 0;
      
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
  
  // Reset for appears_on albums
  offset = 0;
  hasMore = true;
  let totalAppearances = 0;
  retryCount = 0;
  failedBatches = 0;
  
  // Process albums - appears_on and compilations
  console.log(`Fetching appears_on and compilation albums for artist ${spotifyId}`);
  
  while (hasMore && failedBatches < 5) {
    try {
      console.log(`Fetching appears_on albums batch at offset ${offset}`);
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
        totalAppearances = albumsResponse.total;
        console.log(`Artist appears on ${totalAppearances} total albums to process`);
      }
      
      const validAlbums = albumsResponse.items.filter(item => {
        if (!item || !item.id) {
          console.warn(`Invalid album entry found in response`);
          return false;
        }
        return true;
      });
      
      // Global deduplication improvement
      const newAlbums = validAlbums.filter(album => {
        // Skip already processed album IDs
        if (processedAlbumIds.has(album.id)) {
          return false;
        }
        // Add to processed set
        processedAlbumIds.add(album.id);
        return true;
      });
      
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
      console.log(`Fetched ${newAlbums.length} new appears_on albums (total: ${allAlbums.length})`);
      
      // Improved check for pagination completion
      hasMore = validAlbums.length > 0 && 
               (albumsResponse.next || offset < totalAppearances) &&
               // Add a check to make sure we're not hitting a loop
               newAlbums.length > 0;
      
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

  console.log(`Found ${allAlbums.length} unique albums for artist with ${processedAlbumIds.size} unique IDs`);
  
  return allAlbums;
}

/**
 * Enhanced process album tracks function with better pagination and deduplication
 */
async function fetchAlbumTracks(spotify: SpotifyClient, albumId: string): Promise<any[]> {
  let albumTracks = [];
  let trackOffset = 0;
  const trackLimit = 50; // Increased to Spotify's maximum
  let hasMoreTracks = true;
  let totalTracks = 0;
  let retryCount = 0;
  const maxRetries = 3;
  // Global set for track deduplication across pagination
  const processedTrackIds = new Set<string>();

  try {
    console.log(`Starting to fetch tracks for album ${albumId}`);
    
    while (hasMoreTracks) {
      try {
        console.log(`Fetching tracks batch at offset ${trackOffset} for album ${albumId}`);
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
        
        // Improved global deduplication across all pagination requests
        const newTracks = validTracks.filter(track => {
          // Skip if already processed
          if (processedTrackIds.has(track.id)) {
            return false;
          }
          // Add to processed set
          processedTrackIds.add(track.id);
          return true;
        });
        
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
        // Added check to avoid infinite loops by ensuring we're getting new tracks
        hasMoreTracks = validTracks.length > 0 && 
                       (tracksResponse.next || trackOffset < totalTracks) &&
                       newTracks.length > 0;
        
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
    
    console.log(`Found ${albumTracks.length} tracks for album ${albumId} with ${processedTrackIds.size} unique track IDs`);
    return albumTracks;
    
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
 * Queue a single album for processing
 */
async function queueAlbumForProcessing(
  album: any, 
  batchId: string, 
  artistRecord: any, 
  priority: number = 5
): Promise<void> {
  try {
    console.log(`Queueing album ${album.name} (${album.id}) for processing`);
    
    await supabase
      .from("processing_items")
      .insert({
        batch_id: batchId,
        item_type: "album",
        item_id: album.id,
        status: "pending",
        priority: album.is_primary_artist_album ? priority + 5 : priority, // Prioritize primary albums
        metadata: {
          name: album.name,
          is_primary_artist_album: album.is_primary_artist_album || false,
          album_type: album.album_type || "unknown",
          artist_id: artistRecord.id,
          artist_name: artistRecord.name
        }
      });
      
    console.log(`Successfully queued album ${album.name} (${album.id}) for processing`);
  } catch (error) {
    console.error(`Error queueing album ${album?.name || album?.id}:`, error);
  }
}

/**
 * Process a single album with its tracks atomically
 * Enhanced with more robust error handling and improved deduplication
 */
async function processAlbumWithTracks(
  spotify: SpotifyClient,
  albumItem: any,
  artistRecord: any,
  artistCache: Map<string, string> = new Map(),
  processedTrackIds: Set<string> = new Set() // Track IDs that have already been processed
): Promise<{
  success: boolean;
  processed: number;
  failed: number;
  message?: string;
  error?: any;
}> {
  try {
    console.log(`Processing album item ${albumItem.id}: Spotify Album ID ${albumItem.item_id}`);
    
    // Get album details from Spotify
    const albumData = await spotify.getAlbum(albumItem.item_id);
    
    // Set the primary artist flag from metadata
    albumData.is_primary_artist_album = albumItem.metadata?.is_primary_artist_album || false;
    
    // Store the album in database
    const albumRecord = await getOrCreateAlbum(albumData, artistRecord);
    
    // Get all tracks for this album
    const albumTracks = await fetchAlbumTracks(spotify, albumItem.item_id);
    
    if (albumTracks.length === 0) {
      console.log(`No tracks found for album "${albumData.name}"`);
      return { 
        success: true, 
        processed: 0,
        failed: 0,
        message: `No tracks found for album "${albumData.name}"`
      };
    }
    
    // If this is not the artist's primary album, only include tracks where the artist appears
    const relevantTracks = albumData.is_primary_artist_album ? 
      albumTracks : 
      albumTracks.filter(track => {
        return track.artists && 
               Array.isArray(track.artists) && 
               track.artists.some(artist => artist.id === artistRecord.spotify_id);
      });
      
    if (relevantTracks.length === 0) {
      console.log(`No relevant tracks in album "${albumData.name}"`);
      return { 
        success: true, 
        processed: 0,
        failed: 0,
        message: `No relevant tracks in album "${albumData.name}"`
      };
    }
    
    console.log(`Processing ${relevantTracks.length} relevant tracks for album "${albumData.name}"`);
    
    // Improved: filter out tracks that have already been processed globally
    const newRelevantTracks = relevantTracks.filter(track => !processedTrackIds.has(track.id));
    
    if (newRelevantTracks.length < relevantTracks.length) {
      console.log(`Filtered out ${relevantTracks.length - newRelevantTracks.length} already processed tracks from album "${albumData.name}"`);
    }
    
    if (newRelevantTracks.length === 0) {
      console.log(`All tracks from album "${albumData.name}" have already been processed`);
      return { 
        success: true, 
        processed: 0,
        failed: 0,
        message: `All tracks from album "${albumData.name}" have already been processed`
      };
    }
    
    // Process tracks in smaller batches
    const trackChunks = chunkArray(newRelevantTracks, 20);
    let processedTracks = 0;
    let failedTracks = 0;
    
    for (const [chunkIndex, trackChunk] of trackChunks.entries()) {
      console.log(`Processing track chunk ${chunkIndex + 1}/${trackChunks.length} for album "${albumData.name}"`);
      
      try {
        // Create track records and collect their IDs for batch association
        const trackRecordsToInsert = [];
        
        for (const track of trackChunk) {
          if (!track || !track.id) {
            console.warn(`Invalid track data in album ${albumData.name}`);
            failedTracks++;
            continue;
          }
          
          // Add to global processed set
          processedTrackIds.add(track.id);
          
          // Format the release date properly
          const formattedReleaseDate = formatReleaseDate(albumData.release_date);
          
          trackRecordsToInsert.push({
            name: track.name,
            spotify_id: track.id,
            artist_id: artistRecord.id,
            spotify_url: track.external_urls?.spotify,
            preview_url: track.preview_url,
            release_date: formattedReleaseDate,
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
          failedTracks += trackRecordsToInsert.length;
          continue;
        }
        
        console.log(`Upserted ${insertedTracks.length} tracks from album "${albumData.name}"`);
        processedTracks += insertedTracks.length;
        
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
            failedTracks++;
            continue;
          }
          
          trackAlbumRecords.push({
            track_id: insertedTrack.id,
            album_id: albumRecord.id,
            track_number: track.track_number || null,
            disc_number: track.disc_number || null
          });
        }
        
        if (trackAlbumRecords.length === 0) {
          console.warn(`No track-album relationships to create for album "${albumData.name}"`);
          continue;
        }
        
        // Insert track-album relationships
        const { data: insertedRelationships, error: relationshipError } = await supabase
          .from("track_albums")
          .upsert(trackAlbumRecords, {
            onConflict: 'track_id,album_id',
            returning: 'id'
          });
          
        if (relationshipError) {
          console.error(`Error creating track-album relationships:`, relationshipError);
          // We don't throw here because we want to continue with other tracks
          failedTracks += trackAlbumRecords.length;
        } else {
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
      } catch (chunkError) {
        console.error(`Error processing track chunk ${chunkIndex} for album "${albumData.name}":`, chunkError);
        failedTracks += trackChunk.length;
      }
      
      // Add a small delay between chunks to avoid rate limits
      if (chunkIndex < trackChunks.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
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
        
        if (count === 0 && processedTracks > 0) {
          console.error(`WARNING: No track-album relationships were created despite processing ${processedTracks} tracks!`);
        }
      }
    } catch (verificationError) {
      console.error(`Error during verification:`, verificationError);
    }
    
    return {
      success: true,
      processed: processedTracks,
      failed: failedTracks,
      message: `Successfully processed ${processedTracks} tracks for album "${albumData.name}" with ${failedTracks} failures`
    };
  } catch (error) {
    console.error(`Error processing album ${albumItem?.metadata?.name || albumItem?.item_id}:`, error);
    return {
      success: false,
      processed: 0,
      failed: 1,
      message: `Error processing album: ${error.message}`,
      error
    };
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
      
    console.log(`Updated batch ${batchId} progress: ${completedItems}/${totalItems} items, ${totalTracksProcessed} tracks`);
  } catch (error) {
    console.error(`Error updating batch progress for ${batchId}:`, error);
  }
}

/**
 * Enhanced background processing function with improved error handling
 * and track deduplication across albums
 */
async function processBatchInBackground(
  batchId: string, 
  artistRecord: any = null
): Promise<void> {
  try {
    console.log(`Starting background processing for batch ${batchId}`);
    
    // If artist record is not provided, try to get it from the batch metadata
    if (!artistRecord) {
      const { data: batchData, error: batchError } = await supabase
        .from("processing_batches")
        .select("metadata")
        .eq("id", batchId)
        .single();
        
      if (batchError) {
        console.error(`Error fetching batch data for ${batchId}:`, batchError);
        throw new Error(`Failed to get batch data: ${batchError.message}`);
      }
      
      if (batchData.metadata?.artist_id) {
        const { data: artist, error: artistError } = await supabase
          .from("artists")
          .select("*")
          .eq("id", batchData.metadata.artist_id)
          .single();
          
        if (artistError) {
          console.error(`Error fetching artist data for batch ${batchId}:`, artistError);
          throw new Error(`Failed to get artist data: ${artistError.message}`);
        }
        
        artistRecord = artist;
      } else {
        console.error(`Batch ${batchId} does not have artist_id in metadata`);
        throw new Error(`Missing artist_id in batch metadata`);
      }
    }
    
    const spotify = new SpotifyClient();
    
    // Artist cache to avoid duplicate lookups
    const artistCache = new Map<string, string>();
    artistCache.set(artistRecord.spotify_id, artistRecord.id);
    
    // Global track ID set for deduplication across albums
    const processedTrackIds = new Set<string>();
    
    // Load already processed tracks for this artist to avoid duplicates
    try {
      const { data: existingTracks } = await supabase
        .from("tracks")
        .select("spotify_id")
        .eq("artist_id", artistRecord.id);
        
      if (existingTracks && existingTracks.length > 0) {
        existingTracks.forEach(track => {
          if (track.spotify_id) {
            processedTrackIds.add(track.spotify_id);
          }
        });
        console.log(`Loaded ${processedTrackIds.size} existing track IDs for global deduplication`);
      }
    } catch (error) {
      console.error(`Error loading existing tracks for deduplication:`, error);
      // Continue without preloaded tracks, will deduplicate on insert
    }
    
    // Get pending items from the batch
    let hasMoreItems = true;
    let processedItems = 0;
    let failedItems = 0;
    let batchSize = 5;
    let consecutiveEmptyBatches = 0;
    const maxConsecutiveEmptyBatches = 3;
    
    // Store last processed state for recovery
    let lastProcessedId = null;
    
    while (hasMoreItems && consecutiveEmptyBatches < maxConsecutiveEmptyBatches) {
      // Get a small batch of pending items with recovery logic
      let pendingItemsQuery = supabase
        .from("processing_items")
        .select("*")
        .eq("batch_id", batchId)
        .eq("status", "pending")
        .order("priority", { ascending: false });
        
      // Add recovery point if we have one
      if (lastProcessedId) {
        pendingItemsQuery = pendingItemsQuery.filter('id', 'gt', lastProcessedId);
      }
      
      const { data: pendingItems, error: itemsError } = await pendingItemsQuery
        .limit(batchSize);
        
      if (itemsError) {
        console.error(`Error fetching pending items:`, itemsError);
        // Retry with a delay
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }
      
      if (!pendingItems || pendingItems.length === 0) {
        consecutiveEmptyBatches++;
        
        if (consecutiveEmptyBatches >= maxConsecutiveEmptyBatches) {
          console.log(`No more pending items found after ${maxConsecutiveEmptyBatches} attempts, ending processing for batch ${batchId}`);
          hasMoreItems = false;
          break;
        }
        
        console.log(`No pending items found (attempt ${consecutiveEmptyBatches}/${maxConsecutiveEmptyBatches}), checking for rate limiting or pagination issues`);
        
        // Check if there are still pending items in the database
        const { count: remainingCount, error: countError } = await supabase
          .from("processing_items")
          .select("*", { count: "exact", head: true })
          .eq("batch_id", batchId)
          .eq("status", "pending");
          
        if (countError) {
          console.error(`Error checking for remaining items:`, countError);
        } else if (remainingCount && remainingCount > 0) {
          console.log(`Still have ${remainingCount} pending items, but couldn't fetch them. Resetting pagination state.`);
          // Reset pagination state
          lastProcessedId = null;
        } else {
          console.log(`No remaining pending items confirmed by count query`);
          hasMoreItems = false;
          break;
        }
        
        // Add a longer delay before retrying
        await new Promise(resolve => setTimeout(resolve, 10000));
        continue;
      }
      
      // Reset consecutive empty batches counter
      consecutiveEmptyBatches = 0;
      
      console.log(`Processing batch of ${pendingItems.length} albums in background`);
      
      // Process each item sequentially
      for (const item of pendingItems) {
        try {
          console.log(`Processing item ${item.id}: Album ID ${item.item_id}`);
          
          // Store last processed ID for recovery
          lastProcessedId = item.id;
          
          // Process the album with global track deduplication
          const result = await processAlbumWithTracks(
            spotify,
            item,
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
                  tracks_processed: result.processed,
                  tracks_failed: result.failed
                }
              })
              .eq("id", item.id);
              
            processedItems++;
            console.log(`Successfully processed album ${item.metadata?.name || item.item_id} with ${result.processed} tracks`);
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
            console.log(`Failed to process album ${item.metadata?.name || item.item_id}: ${result.message}`);
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
      
      // Update progress one more time after the batch
      await updateBatchProgress(batchId);
      
      console.log(`Processed ${processedItems} albums, ${failedItems} failures, ${processedTrackIds.size} unique tracks so far`);
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
        
      console.log(`Background processing completed for batch ${batchId} with ${processedTrackIds.size} total unique tracks`);
    } else {
      console.log(`Background processing stopped with ${pendingCount} items still pending for batch ${batchId}`);
      
      // Log warning about incomplete processing
      await supabase.rpc("log_error", {
        p_error_type: "warning",
        p_source: "process_artist_background",
        p_message: `Background processing stopped with items still pending`,
        p_stack_trace: "",
        p_context: { 
          batchId, 
          pendingItems: pendingCount, 
          processedItems, 
          failedItems,
          uniqueTrackIds: processedTrackIds.size
        },
        p_item_id: batchId,
        p_item_type: "batch"
      });
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
 * Background function to fetch remaining albums and process them
 * Enhanced with better error handling and retry mechanisms
 */
async function fetchRemainingAlbumsAndProcess(batchId: string, spotifyId: string, artistRecord: any): Promise<void> {
  try {
    console.log(`Starting background fetch of remaining albums for artist ${artistRecord.name}`);
    
    // Get the fetch job
    const { data: fetchJob, error: fetchJobError } = await supabase
      .from("processing_items")
      .select("*")
      .eq("batch_id", batchId)
      .eq("item_type", "fetch_remaining_albums")
      .eq("item_id", spotifyId)
      .single();
      
    if (fetchJobError) {
      console.error(`Error fetching job:`, fetchJobError);
      return;
    }
    
    // Mark job as processing
    await supabase
      .from("processing_items")
      .update({
        status: "processing",
        metadata: {
          ...fetchJob.metadata,
          started_at: new Date().toISOString()
        }
      })
      .eq("id", fetchJob.id);
    
    const spotify = new SpotifyClient();
    
    // Fetch remaining albums
    console.log(`Fetching remaining albums starting from offset ${fetchJob.metadata.offset}`);
    const remainingAlbums = await fetchRemainingArtistAlbums(spotify, spotifyId, fetchJob.metadata.offset);
    
    if (remainingAlbums.length === 0) {
      console.log(`No remaining albums found for artist ${artistRecord.name}`);
      
      // Mark job as completed
      await supabase
        .from("processing_items")
        .update({
          status: "completed",
          metadata: {
            ...fetchJob.metadata,
            completed_at: new Date().toISOString(),
            albums_found: 0
          }
        })
        .eq("id", fetchJob.id);
        
      return;
    }
    
    console.log(`Found ${remainingAlbums.length} remaining albums for artist ${artistRecord.name}`);
    
    // Queue all remaining albums for processing
    const albumItems = [];
    for (const [index, album] of remainingAlbums.entries()) {
      albumItems.push({
        batch_id: batchId,
        item_type: "album",
        item_id: album.id,
        status: "pending",
        priority: album.is_primary_artist_album ? 7 : 5, // Prioritize primary albums but lower than initial batch
        metadata: {
          name: album.name,
          is_primary_artist_album: album.is_primary_artist_album || false,
          album_type: album.album_type || "unknown",
          artist_id: artistRecord.id,
          artist_name: artistRecord.name,
          position: index + fetchJob.metadata.offset
        }
      });
    }
    
    // Insert in smaller chunks to avoid request size limits
    const albumChunks = chunkArray(albumItems, 50);
    for (const [chunkIndex, chunk] of albumChunks.entries()) {
      console.log(`Inserting album chunk ${chunkIndex + 1}/${albumChunks.length}`);
      
      const { error: insertError } = await supabase
        .from("processing_items")
        .insert(chunk);
        
      if (insertError) {
        console.error(`Error inserting album chunk:`, insertError);
      }
      
      // Small delay between chunks
      if (chunkIndex < albumChunks.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    
    // Mark fetch job as completed
    await supabase
      .from("processing_items")
      .update({
        status: "completed",
        metadata: {
          ...fetchJob.metadata,
          completed_at: new Date().toISOString(),
          albums_found: remainingAlbums.length
        }
      })
      .eq("id", fetchJob.id);
    
    // Update batch progress
    await updateBatchProgress(batchId);
    
    // Start processing albums in the background
    console.log(`Starting background processing of albums for batch ${batchId}`);
    await processBatchInBackground(batchId, artistRecord);
    
  } catch (error) {
    console.error(`Error in fetchRemainingAlbumsAndProcess:`, error);
    
    // Log error
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "fetch_remaining_albums",
      p_message: `Error fetching remaining albums`,
      p_stack_trace: error.stack || "",
      p_context: { batchId, spotifyId },
      p_item_id: spotifyId,
      p_item_type: "artist"
    });
  }
}

/**
 * Process a batch of artists
 */
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
    const artistChunkSize = 1; // Process just 1 artist at a time to avoid timeouts
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
          
          // Process just the first artist synchronously for immediate feedback
          if (i === 0 && itemChunk.indexOf(item) === 0) {
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
                        tracksProcessed: result.data?.tracksProcessedImmediately || 0,
                        albumsProcessed: result.data?.initialAlbumsProcessed || 0
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
          } else {
            // Queue all other artists for background processing
            // Create a separate background job for each artist
            const jobId = crypto.randomUUID();
            
            // Create a sub-batch for this artist
            const { data: artistBatch, error: artistBatchError } = await supabase
              .from("processing_batches")
              .insert({
                batch_type: "process_artist",
                status: "pending",
                metadata: {
                  parent_batch_id: batchId,
                  artist_spotify_id: item.item_id,
                  original_item_id: item.id
                }
              })
              .select("id")
              .single();
              
            if (artistBatchError) {
              console.error(`Error creating artist batch:`, artistBatchError);
              
              failed.push(item.id);
              
              await supabase
                .from("processing_items")
                .update({
                  status: "error",
                  retry_count: (item.retry_count || 0) + 1,
                  last_error: `Failed to create artist batch: ${artistBatchError.message}`
                })
                .eq("id", item.id);
                
              continue;
            }
            
            // Launch background processing for this artist
            EdgeRuntime.waitUntil((async () => {
              try {
                console.log(`Starting background processing for artist ${item.item_id}`);
                
                // Process the artist
                const result = await processArtist(item.item_id);
                
                // Update the original item status
                if (result.success) {
                  await supabase
                    .from("processing_items")
                    .update({
                      status: "completed",
                      metadata: {
                        ...item.metadata,
                        result: {
                          success: result.success,
                          message: result.message,
                          artistBatchId: artistBatch.id,
                          jobId: jobId
                        }
                      }
                    })
                    .eq("id", item.id);
                } else {
                  await supabase
                    .from("processing_items")
                    .update({
                      status: "error",
                      retry_count: (item.retry_count || 0) + 1,
                      last_error: result.message
                    })
                    .eq("id", item.id);
                }
              } catch (error) {
                console.error(`Error in background processing for artist ${item.item_id}:`, error);
                
                // Update the original item as error
                await supabase
                  .from("processing_items")
                  .update({
                    status: "error",
                    retry_count: (item.retry_count || 0) + 1,
                    last_error: error.message
                  })
                  .eq("id", item.id);
              }
            })());
            
            // Mark item as in progress for now
            await supabase
              .from("processing_items")
              .update({
                status: "processing",
                metadata: {
                  ...item.metadata,
                  jobId: jobId,
                  artistBatchId: artistBatch.id,
                  backgroundProcessingStarted: new Date().toISOString()
                }
              })
              .eq("id", item.id);
              
            processed.push(item.id);
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
    
    return {
      success: true,
      message: `Processed ${processed.length} artists synchronously, failed ${failed.length} artists, remaining artists queued for background processing`,
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

/**
 * Enhanced process artist function with improved deduplication and error handling
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

    // Fetch a small initial batch of albums for immediate processing
    const initialAlbums = await fetchInitialArtistAlbums(spotify, spotifyId, 3);
    console.log(`Fetched ${initialAlbums.length} initial albums for immediate processing`);
    
    // Process initial albums immediately
    let initialProcessedTracks = 0;
    let initialFailedTracks = 0;
    let initialProcessedAlbums = 0;
    let initialFailedAlbums = 0;
    const artistCache = new Map<string, string>();
    artistCache.set(spotifyId, artistRecord.id);
    
    // Global track ID set for deduplication across albums
    const processedTrackIds = new Set<string>();
    
    // Load already processed tracks for this artist to avoid duplicates
    try {
      const { data: existingTracks } = await supabase
        .from("tracks")
        .select("spotify_id")
        .eq("artist_id", artistRecord.id);
        
      if (existingTracks && existingTracks.length > 0) {
        existingTracks.forEach(track => {
          if (track.spotify_id) {
            processedTrackIds.add(track.spotify_id);
          }
        });
        console.log(`Loaded ${processedTrackIds.size} existing track IDs for global deduplication`);
      }
    } catch (error) {
      console.error(`Error loading existing tracks for deduplication:`, error);
      // Continue without preloaded tracks, will deduplicate on insert
    }
    
    // Queue initial albums for processing
    for (const album of initialAlbums) {
      await queueAlbumForProcessing(album, batchId, artistRecord, 10); // High priority
    }
    
    // Process first album immediately for responsive feedback
    if (initialAlbums.length > 0) {
      try {
        const firstAlbum = initialAlbums[0];
        console.log(`Processing first album immediately: ${firstAlbum.name}`);
        
        // Create a processing item for this album
        const { data: albumItem, error: itemError } = await supabase
          .from("processing_items")
          .select("*")
          .eq("batch_id", batchId)
          .eq("item_id", firstAlbum.id)
          .single();
          
        if (itemError) {
          console.error(`Error fetching album item:`, itemError);
        } else {
          // Process the album with its tracks
          const result = await processAlbumWithTracks(
            spotify,
            albumItem,
            artistRecord,
            artistCache,
            processedTrackIds
          );
          
          if (result.success) {
            initialProcessedTracks += result.processed;
            initialFailedTracks += result.failed;
            initialProcessedAlbums++;
            
            // Mark as completed
            await supabase
              .from("processing_items")
              .update({
                status: "completed",
                metadata: {
                  ...albumItem.metadata,
                  completed_at: new Date().toISOString(),
                  tracks_processed: result.processed,
                  tracks_failed: result.failed
                }
              })
              .eq("id", albumItem.id);
              
            console.log(`Successfully processed initial album ${firstAlbum.name} with ${result.processed} tracks`);
          } else {
            initialFailedAlbums++;
            
            // Mark as error
            await supabase
              .from("processing_items")
              .update({
                status: "error",
                retry_count: (albumItem.retry_count || 0) + 1,
                last_error: result.message
              })
              .eq("id", albumItem.id);
              
            console.error(`Failed to process initial album ${firstAlbum.name}: ${result.message}`);
          }
        }
      } catch (error) {
        console.error(`Error processing first album:`, error);
        initialFailedAlbums++;
      }
    }
    
    // Create a background fetch job for the remaining albums
    const { data: fetchJob, error: fetchJobError } = await supabase
      .from("processing_items")
      .insert({
        batch_id: batchId,
        item_type: "fetch_remaining_albums",
        item_id: spotifyId,
        status: "pending",
        priority: 10, // High priority
        metadata: {
          artist_id: artistRecord.id,
          artist_name: artistRecord.name,
          offset: initialAlbums.length, // Start after initial batch
        }
      })
      .select("id")
      .single();
      
    if (fetchJobError) {
      console.error(`Error creating fetch job:`, fetchJobError);
    } else {
      console.log(`Created background fetch job ${fetchJob.id} for remaining albums`);
    }
    
    // Update batch metadata
    await supabase
      .from("processing_batches")
      .update({
        metadata: {
          artist_id: artistRecord.id,
          artist_name: artistRecord.name,
          spotify_id: spotifyId,
          initial_albums_processed: initialProcessedAlbums,
          initial_albums_failed: initialFailedAlbums,
          initial_tracks_processed: initialProcessedTracks,
          initial_tracks_failed: initialFailedTracks,
          total_unique_tracks_processed: processedTrackIds.size,
          started_at: new Date().toISOString()
        }
      })
      .eq("id", batchId);
    
    // Start background processing immediately
    EdgeRuntime.waitUntil(fetchRemainingAlbumsAndProcess(batchId, spotifyId, artistRecord));
    
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
      message: `Started processing artist ${artistRecord.name}. Processed ${initialProcessedAlbums} albums immediately with ${initialProcessedTracks} tracks. Remaining albums queued for background processing.`,
      data: {
        artist: artistRecord.name,
        artistId: artistRecord.id,
        batchId: batchId,
        initialAlbumsProcessed: initialProcessedAlbums,
        tracksProcessedImmediately: initialProcessedTracks,
        uniqueTracksCount: processedTrackIds.size
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

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Create a job ID for tracking
    const jobId = crypto.randomUUID();
    
    // Parse request body
    const requestBody = await req.json();
    const { spotifyId, batchId } = requestBody;
    
    // Check if we're processing a batch or a single artist
    if (batchId) {
      // Queue the batch processing as a background job
      EdgeRuntime.waitUntil(processBatch(batchId));
      
      // Return immediate response
      return new Response(
        JSON.stringify({
          success: true,
          jobId,
          message: `Processing batch ${batchId} has been queued successfully.`,
          status: "queued"
        }),
        {
          status: 202, // Accepted
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
    } else if (spotifyId) {
      // Queue the artist processing as a background job
      EdgeRuntime.waitUntil(processArtist(spotifyId));
      
      // Return immediate response
      return new Response(
        JSON.stringify({
          success: true,
          jobId,
          message: `Processing artist ${spotifyId} has been queued successfully.`,
          status: "queued"
        }),
        {
          status: 202, // Accepted
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
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
