
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

// Create Supabase client
export const supabase = createClient(
  Deno.env.get("SUPABASE_URL") || "https://nsxxzhhbcwzatvlulfyp.supabase.co",
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "",
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  }
);

// Spotify client class with error handling and rate limit tracking
export class SpotifyClient {
  private accessToken: string | null = null;
  private tokenExpiry: number | null = null;
  
  constructor() {}
  
  /**
   * Get an artist's albums from Spotify with better rate limit handling
   */
  async getArtistAlbums(
    artistId: string,
    options: {
      limit?: number;
      offset?: number;
      include_groups?: string;
      market?: string;
    } = {}
  ) {
    try {
      // Default parameters
      const limit = options.limit || 20;
      const offset = options.offset || 0;
      const includeGroups = options.include_groups || "album,single,compilation";
      const market = options.market || "US";

      // Build query parameters
      const queryParams = new URLSearchParams({
        limit: limit.toString(),
        offset: offset.toString(),
        include_groups: includeGroups,
        market: market,
      });

      // Get response with rate limit handling
      const response = await this.makeApiRequest(
        `artists/${artistId}/albums?${queryParams.toString()}`
      );
      
      return response;
    } catch (error) {
      console.error("Error fetching artist albums:", error);
      return {
        error,
        data: null,
        status: 500,
      };
    }
  }

  /**
   * Get an album's tracks from Spotify with better rate limit handling
   */
  async getAlbumTracks(
    albumId: string,
    options: {
      limit?: number;
      offset?: number;
      market?: string;
    } = {}
  ) {
    try {
      // Default parameters
      const limit = options.limit || 50;
      const offset = options.offset || 0;
      const market = options.market || "US";

      // Build query parameters
      const queryParams = new URLSearchParams({
        limit: limit.toString(),
        offset: offset.toString(),
        market: market,
      });

      // Get response with rate limit handling
      const response = await this.makeApiRequest(
        `albums/${albumId}/tracks?${queryParams.toString()}`
      );
      
      return response;
    } catch (error) {
      console.error("Error fetching album tracks:", error);
      return {
        error,
        data: null,
        status: 500,
      };
    }
  }
  
  /**
   * Search for artists by query
   */
  async searchArtists(
    query: string,
    limit: number = 20,
    options: {
      offset?: number;
      market?: string;
    } = {}
  ) {
    try {
      // Default parameters
      const offset = options.offset || 0;
      const market = options.market || "US";

      // Build query parameters
      const queryParams = new URLSearchParams({
        q: query,
        type: "artist",
        limit: limit.toString(),
        offset: offset.toString(),
        market: market,
      });

      // Make API request with rate limit handling
      const response = await this.makeApiRequest(
        `search?${queryParams.toString()}`
      );
      
      if (response.error) {
        throw response.error;
      }
      
      return response.data;
    } catch (error) {
      console.error("Error searching for artists:", error);
      throw error;
    }
  }
  
  /**
   * Get an artist by ID
   */
  async getArtist(artistId: string) {
    try {
      // Make API request with rate limit handling
      const response = await this.makeApiRequest(`artists/${artistId}`);
      
      if (response.error) {
        throw response.error;
      }
      
      return response.data;
    } catch (error) {
      console.error(`Error getting artist ${artistId}:`, error);
      throw error;
    }
  }
  
  /**
   * Get an album by ID
   */
  async getAlbum(albumId: string) {
    try {
      // Make API request with rate limit handling
      const response = await this.makeApiRequest(`albums/${albumId}`);
      
      if (response.error) {
        throw response.error;
      }
      
      return response.data;
    } catch (error) {
      console.error(`Error getting album ${albumId}:`, error);
      throw error;
    }
  }
  
  /**
   * Make an API request to Spotify with rate limit handling
   */
  private async makeApiRequest(endpoint: string): Promise<{
    data: any;
    headers: Headers;
    status: number;
    error?: any;
  }> {
    try {
      // Get access token
      const accessToken = await this.getAccessToken();

      // Make API request
      const response = await fetch(
        `https://api.spotify.com/v1/${endpoint}`,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json",
          },
        }
      );

      // Process response headers for rate limiting
      this.processRateLimitHeaders(response.headers, endpoint);

      // Process response
      if (!response.ok) {
        // Handle rate limiting specifically
        if (response.status === 429) {
          const retryAfter = response.headers.get("Retry-After");
          throw new Error(
            `Rate limited by Spotify. Retry after ${retryAfter} seconds.`
          );
        }

        throw new Error(
          `Spotify API error: ${response.status} ${response.statusText}`
        );
      }

      // Parse response body
      const data = await response.json();

      return {
        data,
        headers: response.headers,
        status: response.status,
      };
    } catch (error) {
      return {
        error,
        data: null,
        status: error.status || 500,
        headers: new Headers(),
      };
    }
  }
  
  /**
   * Process rate limit headers from Spotify response
   */
  private async processRateLimitHeaders(headers: Headers, endpoint: string): Promise<void> {
    try {
      // Extract rate limit headers
      const requestsRemaining = parseInt(
        headers.get('X-RateLimit-Remaining') || 
        headers.get('x-ratelimit-remaining') || 
        '-1'
      );

      const requestsLimit = parseInt(
        headers.get('X-RateLimit-Limit') || 
        headers.get('x-ratelimit-limit') || 
        '-1'
      );
      
      const retryAfter = headers.get('Retry-After');
      
      // Store the rate limit info
      if (requestsLimit > 0 || requestsRemaining >= 0 || retryAfter) {
        let resetAt: Date | null = null;
        
        if (retryAfter) {
          // Retry-After is in seconds
          resetAt = new Date(Date.now() + parseInt(retryAfter) * 1000);
        }
        
        await supabase
          .from('api_rate_limits')
          .upsert({
            api_name: 'spotify',
            endpoint: endpoint.split('?')[0], // Store without query params
            requests_limit: requestsLimit > 0 ? requestsLimit : undefined,
            requests_remaining: requestsRemaining >= 0 ? requestsRemaining : undefined,
            reset_at: resetAt ? resetAt.toISOString() : undefined,
            last_response: {
              updated_at: new Date().toISOString()
            }
          }, {
            onConflict: 'api_name,endpoint'
          });
      }
    } catch (error) {
      console.error('Error processing rate limit headers:', error);
    }
  }

  /**
   * Get Spotify access token with caching
   */
  private async getAccessToken(): Promise<string> {
    // Check if we have a valid cached token
    if (
      this.accessToken &&
      this.tokenExpiry &&
      Date.now() < this.tokenExpiry - 60000 // Refresh 1 minute before expiry
    ) {
      return this.accessToken;
    }

    // Get credentials from env
    const clientId = Deno.env.get("SPOTIFY_CLIENT_ID");
    const clientSecret = Deno.env.get("SPOTIFY_CLIENT_SECRET");

    if (!clientId || !clientSecret) {
      throw new Error("Spotify credentials not configured");
    }

    // Prepare request for client credentials flow
    const authString = `${clientId}:${clientSecret}`;
    const base64Auth = btoa(authString);

    const response = await fetch("https://accounts.spotify.com/api/token", {
      method: "POST",
      headers: {
        Authorization: `Basic ${base64Auth}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: "grant_type=client_credentials",
    });

    if (!response.ok) {
      throw new Error(
        `Failed to get Spotify access token: ${response.status} ${response.statusText}`
      );
    }

    const data = await response.json();
    
    // Cache the token and calculate expiry time
    this.accessToken = data.access_token;
    this.tokenExpiry = Date.now() + (data.expires_in * 1000);

    return data.access_token;
  }
}

// For backwards compatibility
export const spotify = {
  getArtistAlbums: async (artistId: string, options = {}) => {
    const client = new SpotifyClient();
    return client.getArtistAlbums(artistId, options);
  },
  getAlbumTracks: async (albumId: string, options = {}) => {
    const client = new SpotifyClient();
    return client.getAlbumTracks(albumId, options);
  }
};
