import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";
import pLimit from "https://esm.sh/p-limit@4.0.0";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Constants and Configuration
const SUPABASE_URL = "https://nsxxzhhbcwzatvlulfyp.supabase.co";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const DB_BATCH_SIZE = 50; // Number of records to insert in bulk
const DEFAULT_THROTTLE_MS = Number(Deno.env.get("RATE_LIMIT_MS")) || 1000;
const MAX_THROTTLE_MS = 5000;
const THROTTLE_BUMP_MS = 500;
const DEFAULT_CONCURRENCY = Number(Deno.env.get("SPOTIFY_CONCURRENCY")) || 2;

// Initialize Supabase client with service role for admin access
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Adaptive throttling
let throttleMs = DEFAULT_THROTTLE_MS;

// Bump throttle time when we hit rate limits
function bumpThrottle(): void {
  throttleMs = Math.min(throttleMs + THROTTLE_BUMP_MS, MAX_THROTTLE_MS);
  console.warn(`Throttle increased to ${throttleMs}ms`);
}

// Calculate backoff time based on retry count with jitter
function calculateBackoff(retryCount: number, baseDelay = 1000): number {
  // Exponential backoff: 1s, 2s, 4s, 8s, 16s...
  const exponentialDelay = baseDelay * Math.pow(2, retryCount);
  
  // Add random jitter (±25%)
  const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
  
  return Math.min(exponentialDelay + jitter, 30000); // Cap at 30 seconds
}

// Check if API is rate limited
async function isApiRateLimited(apiName: string, endpoint: string): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from("api_rate_limits")
      .select("*")
      .eq("api_name", apiName)
      .eq("endpoint", endpoint)
      .single();
    
    if (error || !data) return false;
    
    // If we're out of requests and reset time is in the future
    if (
      data.requests_remaining !== null && 
      data.requests_remaining <= 0 && 
      data.reset_at && 
      new Date(data.reset_at) > new Date()
    ) {
      return true;
    }
    
    return false;
  } catch (err) {
    console.error("Error checking rate limits:", err);
    return false;
  }
}

// Spotify Client with rate limiting and retry logic
class SpotifyClient {
  private accessToken: string | null = null;
  private tokenExpiry: Date | null = null;
  private clientId: string;
  private clientSecret: string;
  private requestThrottleMs: number;
  private limit: any;

  constructor(throttleMs = 1000, concurrency = 2) {
    this.clientId = Deno.env.get("SPOTIFY_CLIENT_ID") || "";
    this.clientSecret = Deno.env.get("SPOTIFY_CLIENT_SECRET") || "";
    this.requestThrottleMs = throttleMs;
    this.limit = pLimit(concurrency);
  }

  // Set throttle time between requests
  setThrottle(milliseconds: number): void {
    console.log(`Throttle time changed from ${this.requestThrottleMs}ms to ${milliseconds}ms`);
    this.requestThrottleMs = milliseconds;
  }

  // Set concurrency limit
  setConcurrency(limit: number): void {
    console.log(`Updating concurrency limiter to ${limit}`);
    this.limit = pLimit(limit);
  }

  // Get a valid access token
  async getAccessToken(): Promise<string> {
    // Return existing token if valid
    if (this.accessToken && this.tokenExpiry && this.tokenExpiry > new Date()) {
      return this.accessToken;
    }

    try {
      const response = await fetch("https://accounts.spotify.com/api/token", {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Authorization: `Basic ${btoa(`${this.clientId}:${this.clientSecret}`)}`,
        },
        body: "grant_type=client_credentials",
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(`Spotify auth error: ${data.error}`);
      }

      this.accessToken = data.access_token;
      this.tokenExpiry = new Date(Date.now() + data.expires_in * 1000);
      
      return this.accessToken;
    } catch (error) {
      console.error("Error getting Spotify access token:", error);
      await this.logError("auth", "Failed to get Spotify access token", error);
      throw error;
    }
  }

  // Make authenticated request to Spotify API with retry and rate limit handling
  async makeRequest(endpoint: string, method = "GET", body?: any): Promise<any> {
    // Use the concurrency limiter for this request
    return this.limit(async () => {
      // Create a unique key for this endpoint type to use in rate limiting
      const endpointType = endpoint.split('?')[0].split('/').slice(0, 2).join('/');
      const rateLimiter = new ApiRateLimiter("spotify", endpointType);
      
      // Check if we're rate limited for this endpoint type
      const rateLimitStatus = await rateLimiter.isRateLimited();
      if (rateLimitStatus.limited) {
        const retryAfter = rateLimitStatus.retryAfter || 30;
        console.warn(`Rate limited for Spotify API endpoint: ${endpointType}. Waiting ${retryAfter} seconds before retry.`);
        
        // Wait the full retry period
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
        
        // Reset the rate limit entry to avoid getting stuck
        await rateLimiter.resetRateLimit();
        
        // Try again after waiting
        return this.makeRequest(endpoint, method, body);
      }
      
      // Apply throttling to avoid hitting rate limits
      await new Promise(resolve => setTimeout(resolve, throttleMs));
      
      try {
        const token = await this.getAccessToken();
        
        const options: RequestInit = {
          method,
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        };

        if (body) {
          options.body = JSON.stringify(body);
        }

        const response = await fetch(`https://api.spotify.com/v1${endpoint}`, options);
        
        // Handle rate limiting explicitly - honor Spotify's Retry-After header
        if (response.status === 429) {
          // Get retry-after header (in seconds)
          const waitSec = parseInt(response.headers.get("Retry-After") || "1", 10);
          console.warn(`Spotify 429 on ${endpointType}; retry-after=${waitSec}s`);
          
          // Update rate limit tracking
          await rateLimiter.updateRateLimit(response.headers, { error: "Rate limited" });
          
          // Bump our throttle time to avoid future rate limits
          bumpThrottle();
          
          // Wait the full time specified in the Retry-After header
          await new Promise(resolve => setTimeout(resolve, waitSec * 1000));
          
          // Try the request again once
          return this.makeRequest(endpoint, method, body);
        }
        
        let data;
        try {
          data = await response.json();
        } catch (jsonError) {
          console.error(`Error parsing JSON from Spotify API ${endpoint}:`, jsonError);
          throw new Error(`Failed to parse Spotify API response as JSON: ${jsonError.message}`);
        }

        // Update rate limit info
        await rateLimiter.updateRateLimit(response.headers, data);

        if (!response.ok) {
          throw new Error(`Spotify API error: ${JSON.stringify(data)}`);
        }
        
        // Log the successful response
        console.log(`Spotify API ${endpoint} response status: ${response.status}`);
        
        return data;
      } catch (error) {
        console.error(`Error calling Spotify API ${endpoint}:`, error);
        await this.logError("api", `Error calling Spotify API ${endpoint}`, error);
        throw error;
      }
    });
  }

  // Get artist by ID
  async getArtist(id: string): Promise<any> {
    const result = await this.makeRequest(`/artists/${id}`);
    
    // Validate response
    if (!result || !result.id || !result.name) {
      console.warn(`Unexpected artist response structure:`, result);
      throw new Error(`Invalid artist data returned for ID ${id}`);
    }
    
    return result;
  }

  // Get all artist albums with pagination handling
  async getAllArtistAlbums(id: string, include_groups = "album,single,ep"): Promise<any[]> {
    let allAlbums: any[] = [];
    let offset = 0;
    const limit = 50; // Maximum allowed by Spotify API
    let hasMore = true;
    
    console.log(`Fetching all albums for artist ${id}`);
    
    while (hasMore) {
      console.log(`Fetching albums batch at offset ${offset}`);
      
      const result = await this.makeRequest(
        `/artists/${id}/albums?limit=${limit}&offset=${offset}&include_groups=${encodeURIComponent(include_groups)}`
      );
      
      if (!result.items || !result.items.length) {
        hasMore = false;
        break;
      }
      
      allAlbums = [...allAlbums, ...result.items];
      console.log(`Retrieved ${result.items.length} albums, total so far: ${allAlbums.length}`);
      
      if (!result.next) {
        hasMore = false;
      } else {
        offset += limit;
      }
      
      // Add a small delay between paginated requests to avoid rate limits
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    console.log(`Completed fetching all ${allAlbums.length} albums for artist ${id}`);
    return allAlbums;
  }

  // Log error to database
  private async logError(errorType: string, message: string, error: any): Promise<void> {
    try {
      await supabase.rpc("log_error", {
        p_error_type: errorType,
        p_source: "spotify",
        p_message: message,
        p_stack_trace: error.stack || "",
        p_context: { error: error.message },
      });
    } catch (err) {
      console.error("Error logging to database:", err);
    }
  }
}

// Rate limiting and caching utility
class ApiRateLimiter {
  private apiName: string;
  private endpoint: string;

  constructor(apiName: string, endpoint: string) {
    this.apiName = apiName;
    this.endpoint = endpoint;
  }

  // Check if endpoint is rate limited
  async isRateLimited(): Promise<{limited: boolean, retryAfter?: number}> {
    try {
      const { data } = await supabase
        .from("api_rate_limits")
        .select("*")
        .eq("api_name", this.apiName)
        .eq("endpoint", this.endpoint)
        .maybeSingle();
      
      if (!data) return { limited: false };

      // If reset time is in the future and no requests remaining
      if (
        data.reset_at && 
        data.requests_remaining !== null && 
        data.requests_remaining <= 0 &&
        new Date(data.reset_at) > new Date()
      ) {
        // Calculate seconds until reset
        const secondsUntilReset = Math.ceil(
          (new Date(data.reset_at).getTime() - Date.now()) / 1000
        );
        
        // Check if it's been at least 5 minutes since our last check
        // This helps avoid being stuck in a permanent rate-limited state due to stale data
        const lastUpdatedTime = new Date(data.updated_at).getTime();
        const fiveMinutesAgo = Date.now() - (5 * 60 * 1000);
        
        if (lastUpdatedTime < fiveMinutesAgo) {
          // It's been more than 5 minutes, let's try again
          await this.resetRateLimit();
          return { limited: false };
        }
        
        return { 
          limited: true, 
          retryAfter: secondsUntilReset
        };
      }

      return { limited: false };
    } catch (err) {
      console.error("Error checking rate limits:", err);
      return { limited: false };
    }
  }

  // Update rate limit info after API call
  async updateRateLimit(headers: Headers, response: any): Promise<void> {
    try {
      // Parse rate limit headers from different APIs
      let limit = null;
      let remaining = null;
      let resetAt = null;

      // Spotify format
      if (headers.get("x-ratelimit-limit")) {
        limit = parseInt(headers.get("x-ratelimit-limit") || "0");
        remaining = parseInt(headers.get("x-ratelimit-remaining") || "0");
        const resetSeconds = parseInt(headers.get("x-ratelimit-reset") || "0");
        resetAt = new Date(Date.now() + resetSeconds * 1000).toISOString();
      }
      // Retry-After header (Spotify uses this for 429)
      else if (headers.get("retry-after")) {
        remaining = 0; // We're rate limited
        const retryAfterSeconds = parseInt(headers.get("retry-after") || "1");
        resetAt = new Date(Date.now() + retryAfterSeconds * 1000).toISOString();
        
        console.warn(`Rate limited by ${this.apiName} for ${retryAfterSeconds} seconds until ${resetAt}`);
      }

      // If we have rate limit info, update our tracking
      if (limit !== null || remaining !== null || resetAt !== null) {
        const { data, error } = await supabase
          .from("api_rate_limits")
          .upsert({
            api_name: this.apiName,
            endpoint: this.endpoint,
            requests_limit: limit,
            requests_remaining: remaining,
            reset_at: resetAt,
            last_response: response,
            updated_at: new Date().toISOString(),
          }, {
            onConflict: "api_name,endpoint"
          });

        if (error) {
          console.error("Error updating rate limit:", error);
        }
      }
    } catch (err) {
      console.error("Error in updateRateLimit:", err);
    }
  }

  // Reset rate limit info
  async resetRateLimit(): Promise<void> {
    try {
      const { error } = await supabase
        .from("api_rate_limits")
        .update({
          requests_remaining: null,
          reset_at: null,
          updated_at: new Date().toISOString(),
        })
        .eq("api_name", this.apiName)
        .eq("endpoint", this.endpoint);

      if (error) {
        console.error("Error resetting rate limit:", error);
      } else {
        console.log(`Successfully reset rate limits for ${this.apiName}:${this.endpoint}`);
      }
    } catch (err) {
      console.error("Error in resetRateLimit:", err);
    }
  }
}

// Validate batch integrity before processing
async function validateBatchIntegrity(batchId: string): Promise<{valid: boolean, reason?: string}> {
  try {
    // Check if batch exists
    const { data: batch, error: batchError } = await supabase
      .from("processing_batches")
      .select("*")
      .eq("id", batchId)
      .single();
    
    if (batchError || !batch) {
      return { valid: false, reason: `Batch ${batchId} not found` };
    }
    
    // Check if batch is in a valid state
    if (batch.status !== 'pending' && batch.status !== 'processing') {
      return { valid: false, reason: `Batch ${batchId} is in ${batch.status} state, not processable` };
    }
    
    // Check if there are pending items
    const { count, error: countError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "pending");
    
    if (countError) {
      return { valid: false, reason: `Error checking pending items: ${countError.message}` };
    }
    
    if (count === 0) {
      return { valid: false, reason: `No pending items in batch ${batchId}` };
    }
    
    return { valid: true };
  } catch (err) {
    console.error("Error validating batch integrity:", err);
    return { valid: false, reason: `Exception during validation: ${err.message}` };
  }
}

// Main function to process an artist
async function processArtist(
  artistIdentifier: string, 
  isTestMode = false
): Promise<{
  success: boolean;
  message: string;
  processedAlbums?: number;
}> {
  const startTime = Date.now();
  try {
    // Create Spotify client with appropriate throttle and concurrency settings
    // Use higher throttle in production mode, lower concurrency to avoid rate limits
    const spotifyThrottle = isTestMode ? 1000 : throttleMs;
    const concurrencyLimit = isTestMode ? 2 : DEFAULT_CONCURRENCY;
    const spotify = new SpotifyClient(spotifyThrottle, concurrencyLimit);
    
    console.log(`Processing artist ${artistIdentifier} (Test mode: ${isTestMode})`);
    console.log(`Using throttle: ${spotifyThrottle}ms, concurrency: ${concurrencyLimit}`);
    
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
    
    // Get all albums using our improved method with proper rate limit handling
    try {
      console.log(`Fetching all albums for ${artist.name} using sequential processing`);
      
      // Get all albums in one call that handles pagination internally
      const albums = await spotify.getAllArtistAlbums(
        artist.spotify_id,
        "album,single,ep"
      );
      
      if (!albums || !albums.length) {
        console.log(`No albums found for ${artist.name}`);
      } else {
        console.log(`Processing ${albums.length} albums for ${artist.name}`);
        
        // Filter for primary artist albums
        const primaryArtistAlbums = albums.filter(album => 
          album.artists[0].id === artist.spotify_id
        );
        
        console.log(`Found ${primaryArtistAlbums.length} primary artist albums for ${artist.name}`);
        
        // Process albums - prepare batch inserts
        const newAlbums = [];
        
        for (const album of primaryArtistAlbums) {
          // Check if we already have this album
          const { data: existingAlbum } = await supabase
            .from("albums")
            .select("id")
            .eq("spotify_id", album.id)
            .maybeSingle();
          
          if (existingAlbum) {
            processedAlbumIds.push(existingAlbum.id);
            console.log(`Album ${album.name} already exists, skipping`);
            continue;
          }
          
          console.log(`Adding new album: ${album.name}`);
          
          // Prepare album for batch insert
          newAlbums.push({
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
          });
          
          // Process in batches of DB_BATCH_SIZE
          if (newAlbums.length >= DB_BATCH_SIZE) {
            const { data: insertedAlbums, error: albumsError } = await supabase
              .from("albums")
              .insert(newAlbums)
              .select("id");
            
            if (albumsError) {
              console.error(`Error storing album batch:`, albumsError);
            } else {
              processedAlbumIds = [...processedAlbumIds, ...insertedAlbums.map(a => a.id)];
              console.log(`Stored batch of ${insertedAlbums.length} albums`);
              
              // Update data quality scores for each album
              for (let i = 0; i < insertedAlbums.length; i++) {
                const albumData = newAlbums[i];
                await updateDataQualityScore(
                  "album",
                  insertedAlbums[i].id,
                  "spotify",
                  albumData.popularity ? albumData.popularity / 100 : 0.6,
                  albumData.total_tracks && albumData.release_date ? 0.8 : 0.6,
                  0.9
                );
              }
            }
            
            // Clear the batch
            newAlbums.length = 0;
          }
        }
        
        // Insert any remaining albums
        if (newAlbums.length > 0) {
          const { data: insertedAlbums, error: albumsError } = await supabase
            .from("albums")
            .insert(newAlbums)
            .select("id");
          
          if (albumsError) {
            console.error(`Error storing remaining albums:`, albumsError);
          } else {
            processedAlbumIds = [...processedAlbumIds, ...insertedAlbums.map(a => a.id)];
            console.log(`Stored remaining ${insertedAlbums.length} albums`);
            
            // Update data quality scores for each album
            for (let i = 0; i < insertedAlbums.length; i++) {
              const albumData = newAlbums[i];
              await updateDataQualityScore(
                "album",
                insertedAlbums[i].id,
                "spotify",
                albumData.popularity ? albumData.popularity / 100 : 0.6,
                albumData.total_tracks && albumData.release_date ? 0.8 : 0.6,
                0.9
              );
            }
          }
        }
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
      // Create processing items in chunks of DB_BATCH_SIZE
      for (let i = 0; i < processedAlbumIds.length; i += DB_BATCH_SIZE) {
        const chunk = processedAlbumIds.slice(i, i + DB_BATCH_SIZE);
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
          console.error(`Error adding batch chunk ${i / DB_BATCH_SIZE + 1}:`, itemsError);
          continue;
        }
      }
      
      console.log(`Added ${processedAlbumIds.length} albums to batch ${batch.id}`);
    }
    
    const duration = Date.now() - startTime;
    const message = `Processed ${processedAlbumIds.length} albums for artist ${artist.name}${isTestMode ? " (Test Mode)" : ""} in ${duration}ms`;
    console.log(message);
    
    return {
      success: true,
      message,
      processedAlbums: processedAlbumIds.length
    };
  } catch (error) {
    const duration = Date.now() - startTime;
    console.error(`Error processing artist ${artistIdentifier} (took ${duration}ms):`, error);
    
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

// Process a batch of artist items
async function processArtistItems(
  batchId: string, 
  workerId: string,
  items: any[]
): Promise<{
  success: boolean;
  data: {
    processedItems: string[];
    failedItems: string[];
  }
}> {
  console.log(`Processing ${items.length} artists in batch ${batchId}`);
  
  const processedItems: string[] = [];
  const failedItems: string[] = [];
  const startTime = Date.now();
  
  // Process items sequentially to avoid overwhelming APIs
  for (const item of items) {
    try {
      // Check if this is a test run
      const isTestMode = !!(item.metadata?.is_test_mode === true);
      console.log(`Processing artist item ${item.item_id} (test: ${isTestMode})`);
      
      // Process the artist
      const result = await processArtist(item.item_id, isTestMode);
      
      if (result.success) {
        // Update item as completed
        await supabase
          .from("processing_items")
          .update({
            status: "completed",
            updated_at: new Date().toISOString(),
            metadata: {
              ...item.metadata,
              processed_albums: result.processedAlbums,
              processing_time: Date.now() - startTime
            }
          })
          .eq("id", item.id);
        
        processedItems.push(item.id);
        console.log(`Successfully processed artist ${item.item_id}`);
      } else {
        // If retry count < 5, increment and keep as pending
        const shouldRetry = (item.retry_count || 0) < 5;
        
        await supabase
          .from("processing_items")
          .update({
            status: shouldRetry ? "pending" : "error",
            retry_count: (item.retry_count || 0) + 1,
            last_error: result.message,
            metadata: {
              ...item.metadata,
              last_error: result.message,
              retry_after: shouldRetry ? new Date(Date.now() + calculateBackoff(item.retry_count || 0)).toISOString() : null
            }
          })
          .eq("id", item.id);
        
        // Only count as failed if we've given up retrying
        if (!shouldRetry) {
          failedItems.push(item.id);
        }
        
        console.log(`Failed to process artist ${item.item_id}: ${result.message}`);
      }
    } catch (error) {
      console.error(`Exception processing artist item ${item.item_id}:`, error);
      
      // If retry count < 5, increment and keep as pending
      const shouldRetry = (item.retry_count || 0) < 5;
      
      await supabase
        .from("processing_items")
        .update({
          status: shouldRetry ? "pending" : "error",
          retry_count: (item.retry_count || 0) + 1,
          last_error: error.message,
          metadata: {
            ...item.metadata,
            last_error: error.message,
            retry_after: shouldRetry ? new Date(Date.now() + calculateBackoff(item.retry_count || 0)).toISOString() : null
          }
        })
        .eq("id", item.id);
      
      // Only count as failed if we've given up retrying
      if (!shouldRetry) {
        failedItems.push(item.id);
      }
    }
    
    // Brief pause between items
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  
  const duration = Date.now() - startTime;
  console.log(`Batch processing complete in ${duration}ms. Processed: ${processedItems.length}, Failed: ${failedItems.length}`);
  
  return {
    success: true,
    data: {
      processedItems,
      failedItems
    }
  };
}

// Create the next batch in the pipeline (albums batch)
async function createAlbumsBatch(artistsBatchId: string): Promise<string | null> {
  try {
    // Create a new batch for processing albums
    const { data: newBatch, error: createError } = await supabase
      .from("processing_batches")
      .insert({
        batch_type: "process_albums",
        status: "pending",
        metadata: {
          parent_batch_id: artistsBatchId,
          parent_batch_type: "process_artists",
          processing_pipeline: true
        }
      })
      .select("id")
      .single();
    
    if (createError) {
      console.error(`Error creating albums batch:`, createError);
      return null;
    }
    
    console.log(`Created albums batch with ID ${newBatch.id}`);
    
    // Get completed artist items from the artists batch
    const { data: completedItems, error: itemsError } = await supabase
      .from("processing_items")
      .select("item_id, priority")
      .eq("batch_id", artistsBatchId)
      .eq("item_type", "artist")
      .eq("status", "completed");
    
    if (itemsError) {
      console.error(`Error getting completed artist items:`, itemsError);
      return newBatch.id;
    }
    
    if (!completedItems || completedItems.length === 0) {
      console.log(`No completed artists found in batch ${artistsBatchId}`);
      return newBatch.id;
    }
    
    // Get albums for these artists
    const artistIds = completedItems.map(item => item.item_id);
    
    // Fetch albums to process
    const { data: albums, error: albumsError } = await supabase
      .from("albums")
      .select("id, artist_id")
      .in("artist_id", artistIds);
    
    if (albumsError) {
      console.error(`Error fetching albums for artists:`, albumsError);
      return newBatch.id;
    }
    
    if (!albums || albums.length === 0) {
      console.log(`No albums found for artists in batch ${artistsBatchId}`);
      return newBatch.id;
    }
    
    // Create processing items for each album in batches
    const batchSize = DB_BATCH_SIZE;
    let totalAdded = 0;
    
    for (let i = 0; i < albums.length; i += batchSize) {
      const albumBatch = albums.slice(i, i + batchSize);
      const albumItems = albumBatch.map(album => {
        // Find the priority of the parent artist
        const artistItem = completedItems.find(item => item.item_id === album.artist_id);
        const priority = artistItem ? artistItem.priority : 5; // Default priority
        
        return {
          batch_id: newBatch.id,
          item_type: "album",
          item_id: album.id,
          status: "pending",
          priority,
          metadata: {
            source_batch_id: artistsBatchId,
            artist_id: album.artist_id,
            pipeline_stage: "process_albums"
          }
        };
      });
      
      if (albumItems.length > 0) {
        const { error: insertError } = await supabase
          .from("processing_items")
          .insert(albumItems);
        
        if (insertError) {
          console.error(`Error creating album batch ${i / batchSize + 1}:`, insertError);
        } else {
          totalAdded += albumItems.length;
          console.log(`Added batch ${i / batchSize + 1} with ${albumItems.length} albums`);
        }
      }
    }
    
    console.log(`Added total of ${totalAdded} albums to process_albums batch ${newBatch.id}`);
    
    // Update the batch with the accurate total number of items
    await supabase
      .from("processing_batches")
      .update({
        items_total: totalAdded
      })
      .eq("id", newBatch.id);
    
    return newBatch.id;
  } catch (error) {
    console.error(`Error creating albums batch:`, error);
    return null;
  }
}

// Helper function for logging system events
async function logSystemEvent(
  logLevel: string,
  component: string,
  message: string,
  context: any = {}
): Promise<void> {
  try {
    await supabase.from("system_logs").insert({
      log_level: logLevel,
      component,
      message,
      context
    });
  } catch (error) {
    console.error("Error logging system event:", error);
  }
}

// Helper function for logging processing errors
async function logProcessingError(
  errorType: string,
  source: string,
  message: string,
  error: any,
  context: any = {},
  itemId?: string,
  itemType?: string
): Promise<void> {
  try {
    await supabase.rpc("log_error", {
      p_error_type: errorType,
      p_source: source,
      p_message: message,
      p_stack_trace: error.stack || "",
      p_context: context,
      p_item_id: itemId,
      p_item_type: itemType
    });
  } catch (err) {
    console.error("Error logging to database:", err);
  }
}

// Helper function for updating data quality scores
async function updateDataQualityScore(
  entityType: string,
  entityId: string,
  source: string,
  accuracyScore?: number,
  completenessScore?: number,
  qualityScore?: number
): Promise<void> {
  try {
    // Calculate overall quality if not provided
    const calculatedQuality = qualityScore ?? 
      ((accuracyScore || 0.5) * 0.4 + (completenessScore || 0.5) * 0.6);
    
    await supabase.from("data_quality_scores").insert({
      entity_type: entityType,
      entity_id: entityId,
      source: source,
      accuracy_score: accuracyScore,
      completeness_score: completenessScore,
      quality_score: calculatedQuality,
      last_verified_at: new Date().toISOString()
    });
  } catch (error) {
    console.error(`Error updating data quality score for ${entityType} ${entityId}:`, error);
  }
}

// Process a pending artists batch
async function processArtistsBatch(): Promise<{
  success: boolean;
  message: string;
  batchId?: string;
  processed?: number;
  failed?: number;
  status?: string;
}> {
  const startTime = Date.now();
  try {
    // Generate a unique worker ID
    const workerId = crypto.randomUUID();
    
    console.log(`INFO: Starting process-artists-batch worker=${workerId}`);
    
    // Pre-flight check: Check if API is rate limited
    const apiLimited = await isApiRateLimited("spotify", "/artists");
    if (apiLimited) {
      return {
        success: true,
        message: "API is currently rate limited, skipping batch claim",
        status: "delayed"
      };
    }
    
    // Claim a batch to process with soft locking mechanism
    const { data: batchId, error: claimError } = await supabase.rpc(
      "claim_processing_batch",
      {
        p_batch_type: "process_artists",
        p_worker_id: workerId,
        p_claim_ttl_seconds: 3600 // 1 hour claim time
      }
    );
    
    if (claimError) {
      console.error("Error claiming batch:", claimError);
      throw claimError;
    }
    
    if (!batchId) {
      return {
        success: true,
        message: "No pending artists batches found to process",
        status: "idle"
      };
    }
    
    console.log(`INFO: Starting process-artists-batch batchId=${batchId}`);
    
    // Validate batch integrity
    const batchValidation = await validateBatchIntegrity(batchId);
    if (!batchValidation.valid) {
      console.warn(`Batch integrity check failed: ${batchValidation.reason}`);
      
      // Release the batch as it's not valid
      await supabase.rpc(
        "release_processing_batch",
        {
          p_batch_id: batchId,
          p_worker_id: workerId,
          p_status: "error"
        }
      );
      
      // Update batch with error message
      await supabase
        .from("processing_batches")
        .update({
          error_message: batchValidation.reason
        })
        .eq("id", batchId);
      
      return {
        success: false,
        message: `Batch integrity validation failed: ${batchValidation.reason}`,
        batchId,
        status: "error"
      };
    }
    
    // Define batch-specific parameters
    const batchSize = 10; // Conservative batch size
    
    // Get items to process
    const { data: items, error: itemsError } = await supabase
      .from("processing_items")
      .select("*")
      .eq("batch_id", batchId)
      .eq("status", "pending")
      .order("priority", { ascending: false })
      .order("created_at", { ascending: true })
      .limit(batchSize);
    
    if (itemsError) {
      console.error("Error getting batch items:", itemsError);
      throw itemsError;
    }
    
    if (!items || items.length === 0) {
      // No items to process, release the batch as completed
      await supabase.rpc(
        "release_processing_batch",
        {
          p_batch_id: batchId,
          p_worker_id: workerId,
          p_status: "completed"
        }
      );
      
      // For completed batches, create the next batch in the pipeline
      await createAlbumsBatch(batchId);
      
      return {
        success: true,
        message: "Artists batch claimed but no items to process",
        batchId,
        processed: 0,
        failed: 0,
        status: "completed"
      };
    }
    
    console.log(`Processing ${items.length} artists in batch ${batchId}`);
    
    // Process the artist items directly (no HTTP fetch)
    const result = await processArtistItems(batchId, workerId, items);
    
    // Get accurate counts from the database for this batch
    const { count: totalItems, error: countError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId);
    
    const { count: pendingItems, error: pendingError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "pending");
    
    const { count: completedItems, error: completedError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "completed");
    
    const { count: errorItems, error: errorCountError } = await supabase
      .from("processing_items")
      .select("*", { count: "exact", head: true })
      .eq("batch_id", batchId)
      .eq("status", "error");
    
    // Calculate if all items are processed
    const allDone = !pendingError && pendingItems === 0;
    
    // Release the batch with appropriate status
    await supabase.rpc(
      "release_processing_batch",
      {
        p_batch_id: batchId,
        p_worker_id: workerId,
        p_status: allDone ? "completed" : "processing"
      }
    );
    
    // If batch is completed, create next batch in pipeline
    if (allDone) {
      await createAlbumsBatch(batchId);
    }
    
    // Update batch with accurate progress
    if (!countError && !completedError && !errorCountError) {
      await supabase
        .from("processing_batches")
        .update({
          items_total: totalItems || 0,
          items_processed: completedItems || 0,
          items_failed: errorItems || 0,
        })
        .eq("id", batchId);
    }
    
    const duration = Date.now() - startTime;
    const message = `Processed ${result.data.processedItems.length} artists, failed ${result.data.failedItems.length} artists, ${pendingItems || 'unknown'} items remaining`;
    
    console.log(`INFO: Finished process-artists-batch batchId=${batchId} duration=${duration}ms`);
    
    return {
      success: true,
      message,
      batchId,
      processed: result.data.processedItems.length,
      failed: result.data.failedItems.length,
      status: allDone ? "completed" : "in_progress"
    };
  } catch (error) {
    const duration = Date.now() - startTime;
    console.error(`Error processing artists batch (took ${duration}ms):`, error);
    
    // Log error to our database
    await logProcessingError(
      "processing",
      "process_artists_batch",
      `Error processing artists batch`,
      error,
      { batchType: "process_artists" }
    );
    
    return {
      success: false,
      message: `Error processing artists batch: ${error.message}`,
      status: "error"
    };
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const result = await processArtistsBatch();
    
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
    console.error("Error handling process-artists-batch request:", error);
    
    // Log error to our database
    try {
      await logProcessingError(
        "endpoint",
        "process_artists_batch",
        "Error handling process-artists-batch request",
        error
      );
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
