
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const SUPABASE_URL = "https://nsxxzhhbcwzatvlulfyp.supabase.co";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";

// Initialize Supabase client with service role for admin access to DB
export const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

/**
 * Rate limiting and caching utility
 */
export class ApiRateLimiter {
  private apiName: string;
  private endpoint: string;
  private cacheTimeMs: number;
  private cache: Map<string, {data: any, timestamp: number}> = new Map();

  constructor(apiName: string, endpoint: string, cacheTimeMs = 3600000) { // Default cache: 1 hour
    this.apiName = apiName;
    this.endpoint = endpoint;
    this.cacheTimeMs = cacheTimeMs;
  }

  // Get cached data if available
  getCachedData(cacheKey: string): any | null {
    if (!this.cache.has(cacheKey)) return null;
    
    const cachedItem = this.cache.get(cacheKey);
    if (!cachedItem) return null;
    
    // Check if cache is still valid
    if (Date.now() - cachedItem.timestamp < this.cacheTimeMs) {
      return cachedItem.data;
    }
    
    // Cache expired
    this.cache.delete(cacheKey);
    return null;
  }

  // Store data in cache
  setCachedData(cacheKey: string, data: any): void {
    this.cache.set(cacheKey, {
      data,
      timestamp: Date.now()
    });
  }

  // Check if endpoint is rate limited
  async isRateLimited(): Promise<{limited: boolean, retryAfter?: number}> {
    try {
      const { data, error } = await supabase
        .from("api_rate_limits")
        .select("*")
        .eq("api_name", this.apiName)
        .eq("endpoint", this.endpoint)
        .single();

      if (error) {
        console.error("Error checking rate limit:", error);
        return { limited: false };
      }

      if (!data) return { limited: false };

      // If reset time is in the future and no requests remaining
      if (
        data.reset_at && 
        data.requests_remaining !== null && 
        data.requests_remaining <= 0 &&
        new Date(data.reset_at) > new Date()
      ) {
        // Calculate seconds remaining until reset
        const secondsUntilReset = Math.ceil((new Date(data.reset_at).getTime() - Date.now()) / 1000);
        return { limited: true, retryAfter: secondsUntilReset };
      }

      // If reset time is in the past, we can reset the counter
      if (data.reset_at && new Date(data.reset_at) <= new Date()) {
        await this.resetRateLimit();
        return { limited: false };
      }

      return { limited: false };
    } catch (err) {
      console.error("Error in isRateLimited:", err);
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
      }
      // Genius format
      else if (headers.get("x-ratelimit-limit")) {
        limit = parseInt(headers.get("x-ratelimit-limit") || "0");
        remaining = parseInt(headers.get("x-ratelimit-remaining") || "0");
        resetAt = headers.get("x-ratelimit-reset")
          ? new Date(parseInt(headers.get("x-ratelimit-reset") || "0") * 1000).toISOString()
          : null;
      }
      // Discogs format
      else if (headers.get("x-discogs-ratelimit")) {
        limit = parseInt(headers.get("x-discogs-ratelimit") || "0");
        remaining = parseInt(headers.get("x-discogs-ratelimit-remaining") || "0");
        // Discogs uses a reset time in seconds
        const resetSeconds = parseInt(headers.get("x-discogs-ratelimit-used") || "0");
        resetAt = new Date(Date.now() + resetSeconds * 1000).toISOString();
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
  private async resetRateLimit(): Promise<void> {
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
      }
    } catch (err) {
      console.error("Error in resetRateLimit:", err);
    }
  }
}

/**
 * Utility for retry logic with exponential backoff
 */
export class RetryHelper {
  static async retryOperation<T>(
    operation: () => Promise<T>, 
    maxRetries = 3, 
    initialDelay = 1000, 
    operationName = "operation",
    isRetryable?: (error: Error) => boolean
  ): Promise<T> {
    let attempt = 0;
    
    while (true) {
      try {
        return await operation();
      } catch (error) {
        attempt++;
        
        // Check if we should retry based on error type
        if (isRetryable && !isRetryable(error)) {
          console.error(`${operationName} failed with non-retryable error:`, error);
          throw error;
        }
        
        if (attempt > maxRetries) {
          console.error(`${operationName} failed after ${maxRetries} retries:`, error);
          throw error;
        }
        
        // Check for retry-after in headers if it's a response error
        let retryAfter = 0;
        if (error.headers && error.headers.get) {
          const retryAfterHeader = error.headers.get('retry-after');
          if (retryAfterHeader) {
            retryAfter = parseInt(retryAfterHeader) * 1000;
          }
        }
        
        // Use retry-after from header or exponential backoff with jitter
        const delay = retryAfter || initialDelay * Math.pow(2, attempt - 1) * (0.5 + Math.random() * 0.5);
        
        console.warn(`${operationName} attempt ${attempt} failed, retrying in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
}

/**
 * Spotify API Client with improved rate limiting and batching
 */
export class SpotifyClient {
  private accessToken: string | null = null;
  private tokenExpiry: Date | null = null;
  private clientId: string;
  private clientSecret: string;
  private requestThrottleMs: number = 200; // Default throttle between requests
  private cacheEnabled: boolean = true;

  constructor(throttleMs = 200, enableCache = true) {
    this.clientId = Deno.env.get("SPOTIFY_CLIENT_ID") || "";
    this.clientSecret = Deno.env.get("SPOTIFY_CLIENT_SECRET") || "";
    this.requestThrottleMs = throttleMs;
    this.cacheEnabled = enableCache;
  }

  // Set throttle time between requests
  setThrottle(milliseconds: number): void {
    this.requestThrottleMs = milliseconds;
  }

  // Enable or disable caching
  setCache(enabled: boolean): void {
    this.cacheEnabled = enabled;
  }

  // Get a valid access token
  async getAccessToken(): Promise<string> {
    // Return existing token if valid
    if (this.accessToken && this.tokenExpiry && this.tokenExpiry > new Date()) {
      return this.accessToken;
    }

    try {
      return await RetryHelper.retryOperation(async () => {
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
      }, 3, 2000, "Spotify authentication");
    } catch (error) {
      console.error("Error getting Spotify access token:", error);
      await this.logError("auth", "Failed to get Spotify access token", error);
      throw error;
    }
  }

  // Make authenticated request to Spotify API with retry and rate limit handling
  async makeRequest(endpoint: string, method = "GET", body?: any, cacheKey?: string): Promise<any> {
    const rateLimiter = new ApiRateLimiter("spotify", endpoint);
    
    // Try to get from cache first if enabled and a cache key is provided
    if (this.cacheEnabled && cacheKey) {
      const cachedData = rateLimiter.getCachedData(cacheKey);
      if (cachedData) {
        console.log(`Using cached data for ${endpoint} with key ${cacheKey}`);
        return cachedData;
      }
    }
    
    // Check if we're rate limited
    const rateLimitStatus = await rateLimiter.isRateLimited();
    if (rateLimitStatus.limited) {
      console.warn(`Rate limited for Spotify API endpoint: ${endpoint}. Retry after ${rateLimitStatus.retryAfter || 0} seconds.`);
      throw new Error(`Rate limited for Spotify API endpoint: ${endpoint}. Retry after ${rateLimitStatus.retryAfter || 0} seconds.`);
    }

    // Throttle requests to avoid hitting rate limits
    await new Promise(resolve => setTimeout(resolve, this.requestThrottleMs));

    try {
      const isRetryable = (error: any) => {
        // Retry on network errors and 5xx server errors
        if (error.message && (
            error.message.includes('network') || 
            error.message.includes('timeout') ||
            error.message.includes('500') || 
            error.message.includes('503')
        )) {
          return true;
        }
        
        // Don't retry on 4xx client errors except 429 (rate limit)
        if (error.status && error.status !== 429 && error.status < 500 && error.status >= 400) {
          return false;
        }
        
        return true;
      };
      
      return await RetryHelper.retryOperation(async () => {
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
        
        // Handle rate limiting before we parse the response
        if (response.status === 429) {
          // Get retry-after header
          const retryAfter = parseInt(response.headers.get("Retry-After") || "1");
          const error: any = new Error(`Spotify rate limit exceeded. Retry after ${retryAfter} seconds.`);
          error.status = 429;
          error.headers = response.headers;
          
          // Update rate limit tracking
          await rateLimiter.updateRateLimit(response.headers, { error: "Rate limited" });
          
          throw error;
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
          const error: any = new Error(`Spotify API error: ${JSON.stringify(data)}`);
          error.status = response.status;
          error.headers = response.headers;
          throw error;
        }

        // Store in cache if caching is enabled and we have a cache key
        if (this.cacheEnabled && cacheKey) {
          rateLimiter.setCachedData(cacheKey, data);
        }
        
        // Log the successful response for debugging
        console.log(`Spotify API ${endpoint} response status: ${response.status}`);
        
        return data;
      }, 3, 2000, `Spotify API call to ${endpoint}`, isRetryable);
    } catch (error) {
      console.error(`Error calling Spotify API ${endpoint}:`, error);
      await this.logError("api", `Error calling Spotify API ${endpoint}`, error);
      throw error;
    }
  }

  // Search for artists by name or genre with improved caching
  async searchArtists(query: string, limit = 20, offset = 0): Promise<any> {
    const cacheKey = `search_artists_${query}_${limit}_${offset}`;
    
    const result = await this.makeRequest(
      `/search?type=artist&q=${encodeURIComponent(query)}&limit=${limit}&offset=${offset}`,
      "GET",
      undefined,
      cacheKey
    );
    
    // Add validation to ensure expected structure
    if (!result || !result.artists || !Array.isArray(result.artists?.items)) {
      console.warn(`Unexpected search artists response structure:`, result);
      return { artists: { items: [], total: 0 } };
    }
    
    return result;
  }

  // Get artist by ID
  async getArtist(id: string): Promise<any> {
    const cacheKey = `artist_${id}`;
    
    const result = await this.makeRequest(`/artists/${id}`, "GET", undefined, cacheKey);
    
    // Validate response
    if (!result || !result.id || !result.name) {
      console.warn(`Unexpected artist response structure:`, result);
      throw new Error(`Invalid artist data returned for ID ${id}`);
    }
    
    return result;
  }

  // Get artist's albums with efficient batching and caching
  async getArtistAlbums(
    id: string, 
    limit = 50, 
    offset = 0,
    include_groups = "album,single,compilation,appears_on"
  ): Promise<any> {
    try {
      const cacheKey = `artist_albums_${id}_${limit}_${offset}_${include_groups}`;
      
      const result = await this.makeRequest(
        `/artists/${id}/albums?limit=${limit}&offset=${offset}&include_groups=${encodeURIComponent(include_groups)}`,
        "GET",
        undefined,
        cacheKey
      );
      
      // Validate and normalize response 
      if (!result) {
        console.warn(`Empty response for artist albums ${id}`);
        return { items: [], total: 0, next: null };
      }
      
      // Ensure items is always an array
      if (!result.items || !Array.isArray(result.items)) {
        console.warn(`Missing or invalid items array for artist albums ${id}:`, result);
        result.items = [];
      }
      
      // Ensure total exists
      if (typeof result.total !== 'number') {
        console.warn(`Missing or invalid total for artist albums ${id}:`, result);
        result.total = result.items.length;
      }
      
      return result;
    } catch (error) {
      console.error(`Error fetching albums for artist ${id}:`, error);
      // Return a valid but empty response structure on error
      return { items: [], total: 0, next: null };
    }
  }

  // Get album details
  async getAlbum(id: string): Promise<any> {
    const cacheKey = `album_${id}`;
    
    const result = await this.makeRequest(`/albums/${id}`, "GET", undefined, cacheKey);
    
    // Validate response
    if (!result || !result.id || !result.name) {
      console.warn(`Unexpected album response structure:`, result);
      throw new Error(`Invalid album data returned for ID ${id}`);
    }
    
    return result;
  }

  // Get album tracks with caching
  async getAlbumTracks(id: string, limit = 50, offset = 0): Promise<any> {
    try {
      const cacheKey = `album_tracks_${id}_${limit}_${offset}`;
      
      const result = await this.makeRequest(
        `/albums/${id}/tracks?limit=${limit}&offset=${offset}`,
        "GET",
        undefined,
        cacheKey
      );
      
      // Validate and normalize response
      if (!result) {
        console.warn(`Empty response for album tracks ${id}`);
        return { items: [], total: 0, next: null };
      }
      
      // Ensure items is always an array
      if (!result.items || !Array.isArray(result.items)) {
        console.warn(`Missing or invalid items array for album tracks ${id}:`, result);
        result.items = [];
      }
      
      // Ensure total exists
      if (typeof result.total !== 'number') {
        console.warn(`Missing or invalid total for album tracks ${id}:`, result);
        result.total = result.items.length;
      }
      
      return result;
    } catch (error) {
      console.error(`Error fetching tracks for album ${id}:`, error);
      // Return a valid but empty response structure on error
      return { items: [], total: 0, next: null };
    }
  }

  // Get multiple tracks in a single batch request (up to 50)
  async getTracks(ids: string[]): Promise<any> {
    if (!ids.length) return { tracks: [] };
    
    // Spotify allows max 50 IDs per request
    const batchSize = 50;
    const batches = [];
    
    for (let i = 0; i < ids.length; i += batchSize) {
      const batchIds = ids.slice(i, i + batchSize);
      const cacheKey = `tracks_batch_${batchIds.join('_')}`;
      
      batches.push(
        this.makeRequest(
          `/tracks?ids=${encodeURIComponent(batchIds.join(','))}`,
          "GET",
          undefined,
          cacheKey
        )
      );
    }
    
    const results = await Promise.all(batches);
    
    // Combine all batch results
    const tracks = results.flatMap(result => result.tracks || []);
    
    return { tracks };
  }

  // Get track details
  async getTrack(id: string): Promise<any> {
    const cacheKey = `track_${id}`;
    
    const result = await this.makeRequest(`/tracks/${id}`, "GET", undefined, cacheKey);
    
    // Validate response
    if (!result || !result.id || !result.name) {
      console.warn(`Unexpected track response structure:`, result);
      throw new Error(`Invalid track data returned for ID ${id}`);
    }
    
    return result;
  }

  // Log error to our database
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

/**
 * Genius API Client
 */
export class GeniusClient {
  private accessToken: string;

  constructor() {
    this.accessToken = Deno.env.get("GENIUS_ACCESS_TOKEN") || "";
  }

  // Make request to Genius API
  async makeRequest(endpoint: string, method = "GET", body?: any): Promise<any> {
    const rateLimiter = new ApiRateLimiter("genius", endpoint);
    
    // Check if we're rate limited
    if (await rateLimiter.isRateLimited()) {
      throw new Error(`Rate limited for Genius API endpoint: ${endpoint}`);
    }

    try {
      const options: RequestInit = {
        method,
        headers: {
          Authorization: `Bearer ${this.accessToken}`,
          "Content-Type": "application/json",
        },
      };

      if (body) {
        options.body = JSON.stringify(body);
      }

      const response = await fetch(`https://api.genius.com${endpoint}`, options);
      const data = await response.json();

      // Update rate limit info
      await rateLimiter.updateRateLimit(response.headers, data);

      if (!response.ok) {
        throw new Error(`Genius API error: ${JSON.stringify(data)}`);
      }

      return data;
    } catch (error) {
      console.error(`Error calling Genius API ${endpoint}:`, error);
      await this.logError("api", `Error calling Genius API ${endpoint}`, error);
      throw error;
    }
  }

  // Search for songs
  async searchSongs(query: string): Promise<any> {
    return this.makeRequest(`/search?q=${encodeURIComponent(query)}`);
  }

  // Get song details
  async getSong(id: string): Promise<any> {
    return this.makeRequest(`/songs/${id}`);
  }

  // Get artist details
  async getArtist(id: string): Promise<any> {
    return this.makeRequest(`/artists/${id}`);
  }

  // Log error to our database
  private async logError(errorType: string, message: string, error: any): Promise<void> {
    try {
      await supabase.rpc("log_error", {
        p_error_type: errorType,
        p_source: "genius",
        p_message: message,
        p_stack_trace: error.stack || "",
        p_context: { error: error.message },
      });
    } catch (err) {
      console.error("Error logging to database:", err);
    }
  }
}

/**
 * Discogs API Client
 */
export class DiscogsClient {
  private personalToken: string;

  constructor() {
    this.personalToken = Deno.env.get("DISCOGS_PERSONAL_TOKEN") || "";
  }

  // Make request to Discogs API
  async makeRequest(endpoint: string, method = "GET", body?: any): Promise<any> {
    const rateLimiter = new ApiRateLimiter("discogs", endpoint);
    
    // Check if we're rate limited
    if (await rateLimiter.isRateLimited()) {
      throw new Error(`Rate limited for Discogs API endpoint: ${endpoint}`);
    }

    try {
      const options: RequestInit = {
        method,
        headers: {
          "Authorization": `Discogs token=${this.personalToken}`,
          "User-Agent": "MusicProducerDirectory/1.0",
          "Content-Type": "application/json",
        },
      };

      if (body) {
        options.body = JSON.stringify(body);
      }

      const response = await fetch(`https://api.discogs.com${endpoint}`, options);
      const data = await response.json();

      // Update rate limit info
      await rateLimiter.updateRateLimit(response.headers, data);

      if (!response.ok) {
        throw new Error(`Discogs API error: ${JSON.stringify(data)}`);
      }

      return data;
    } catch (error) {
      console.error(`Error calling Discogs API ${endpoint}:`, error);
      await this.logError("api", `Error calling Discogs API ${endpoint}`, error);
      throw error;
    }
  }

  // Search for releases
  async searchReleases(query: string, type = "release"): Promise<any> {
    return this.makeRequest(
      `/database/search?q=${encodeURIComponent(query)}&type=${type}`
    );
  }

  // Get release details
  async getRelease(id: string): Promise<any> {
    return this.makeRequest(`/releases/${id}`);
  }

  // Get master release details
  async getMasterRelease(id: string): Promise<any> {
    return this.makeRequest(`/masters/${id}`);
  }

  // Log error to our database
  private async logError(errorType: string, message: string, error: any): Promise<void> {
    try {
      await supabase.rpc("log_error", {
        p_error_type: errorType,
        p_source: "discogs",
        p_message: message,
        p_stack_trace: error.stack || "",
        p_context: { error: error.message },
      });
    } catch (err) {
      console.error("Error logging to database:", err);
    }
  }
}
