
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

// Spotify client with error handling and rate limit tracking
export const spotify = {
  /**
   * Get an artist's albums from Spotify
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

      // Get access token
      const accessToken = await getSpotifyAccessToken();

      // Build query parameters
      const queryParams = new URLSearchParams({
        limit: limit.toString(),
        offset: offset.toString(),
        include_groups: includeGroups,
        market: market,
      });

      // Make API request
      const response = await fetch(
        `https://api.spotify.com/v1/artists/${artistId}/albums?${queryParams.toString()}`,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json",
          },
        }
      );

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
      console.error("Error fetching artist albums:", error);
      return {
        error,
        data: null,
        status: 500,
      };
    }
  },

  /**
   * Get an album's tracks from Spotify
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

      // Get access token
      const accessToken = await getSpotifyAccessToken();

      // Build query parameters
      const queryParams = new URLSearchParams({
        limit: limit.toString(),
        offset: offset.toString(),
        market: market,
      });

      // Make API request
      const response = await fetch(
        `https://api.spotify.com/v1/albums/${albumId}/tracks?${queryParams.toString()}`,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json",
          },
        }
      );

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
      console.error("Error fetching album tracks:", error);
      return {
        error,
        data: null,
        status: 500,
      };
    }
  },
};

// Access token cache
let spotifyAccessToken: string | null = null;
let spotifyTokenExpiry: number | null = null;

/**
 * Get Spotify access token with caching
 */
async function getSpotifyAccessToken(): Promise<string> {
  // Check if we have a valid cached token
  if (
    spotifyAccessToken &&
    spotifyTokenExpiry &&
    Date.now() < spotifyTokenExpiry - 60000 // Refresh 1 minute before expiry
  ) {
    return spotifyAccessToken;
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
  spotifyAccessToken = data.access_token;
  spotifyTokenExpiry = Date.now() + (data.expires_in * 1000);

  return data.access_token;
}
