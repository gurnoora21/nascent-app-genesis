
import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { GeniusClient, DiscogsClient, supabase } from "../lib/api-clients.ts";

// CORS headers for browser access
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Find producer information for a track
async function identifyProducers(trackId: string): Promise<{
  success: boolean;
  message: string;
  producers?: any[];
}> {
  try {
    const genius = new GeniusClient();
    const discogs = new DiscogsClient();
    
    // Get track details from our database
    const { data: track, error: trackError } = await supabase
      .from("tracks")
      .select(`
        id,
        name,
        spotify_id,
        album_name,
        artists:artists!inner(
          id,
          name,
          spotify_id
        )
      `)
      .eq("id", trackId)
      .single();
    
    if (trackError) {
      console.error(`Error getting track ${trackId}:`, trackError);
      throw trackError;
    }
    
    console.log(`Identifying producers for track: ${track.name} by ${track.artists.name}`);
    
    const producersFound = [];
    let confidence = 'low';
    
    // First try Genius API
    try {
      // Search for the song
      const searchQuery = `${track.name} ${track.artists.name}`;
      const searchResult = await genius.searchSongs(searchQuery);
      
      if (searchResult?.response?.hits && searchResult.response.hits.length > 0) {
        // Get the first hit
        const song = searchResult.response.hits[0].result;
        
        // Get song details
        const songDetails = await genius.getSong(song.id);
        
        if (songDetails?.response?.song?.producer_artists) {
          confidence = 'medium';
          
          // Process producer artists
          for (const producer of songDetails.response.song.producer_artists) {
            // Check if producer exists in our database
            const { data: existingProducer, error: producerError } = await supabase
              .from("producers")
              .select("id")
              .eq("genius_id", producer.id.toString())
              .single();
            
            let producerId;
            
            if (producerError && producerError.code === "PGRST116") {
              // Insert new producer
              const { data: newProducer, error: insertError } = await supabase
                .from("producers")
                .insert({
                  name: producer.name,
                  genius_id: producer.id.toString(),
                  image_url: producer.image_url,
                  metadata: {
                    genius: producer
                  }
                })
                .select("id")
                .single();
              
              if (insertError) {
                console.error(`Error inserting producer ${producer.name}:`, insertError);
                continue;
              }
              
              producerId = newProducer.id;
            } else if (producerError) {
              console.error(`Error checking for producer ${producer.name}:`, producerError);
              continue;
            } else {
              producerId = existingProducer.id;
            }
            
            // Create track-producer relationship
            const { error: relError } = await supabase
              .from("track_producers")
              .upsert({
                track_id: trackId,
                producer_id: producerId,
                source: "genius",
                confidence: confidence as any,
                metadata: {
                  genius_song_id: song.id
                }
              }, {
                onConflict: "track_id,producer_id,source"
              });
            
            if (relError) {
              console.error(`Error creating track-producer relationship:`, relError);
              continue;
            }
            
            producersFound.push({
              name: producer.name,
              id: producerId,
              source: "genius",
              confidence
            });
          }
        }
      }
    } catch (geniusError) {
      console.error(`Error using Genius API:`, geniusError);
      // Continue to try other sources
    }
    
    // Try Discogs API if no producers found or confidence is low
    if (producersFound.length === 0 || confidence === 'low') {
      try {
        const searchQuery = `${track.name} ${track.artists.name}`;
        const searchResult = await discogs.searchReleases(searchQuery);
        
        if (searchResult?.results && searchResult.results.length > 0) {
          // Get the first result
          const release = searchResult.results[0];
          
          // Get release details
          const releaseDetails = await discogs.getRelease(release.id);
          
          if (releaseDetails?.extraartists) {
            // Find producers in the extra artists
            const producers = releaseDetails.extraartists.filter(
              (artist: any) => artist.role.toLowerCase().includes("producer")
            );
            
            if (producers.length > 0) {
              confidence = confidence === 'low' ? 'medium' : 'high';
              
              for (const producer of producers) {
                // Check if producer exists in our database
                const { data: existingProducer, error: producerError } = await supabase
                  .from("producers")
                  .select("id")
                  .eq("discogs_id", producer.id.toString())
                  .single();
                
                let producerId;
                
                if (producerError && producerError.code === "PGRST116") {
                  // Insert new producer
                  const { data: newProducer, error: insertError } = await supabase
                    .from("producers")
                    .insert({
                      name: producer.name,
                      discogs_id: producer.id.toString(),
                      metadata: {
                        discogs: producer
                      }
                    })
                    .select("id")
                    .single();
                  
                  if (insertError) {
                    console.error(`Error inserting producer ${producer.name}:`, insertError);
                    continue;
                  }
                  
                  producerId = newProducer.id;
                } else if (producerError) {
                  console.error(`Error checking for producer ${producer.name}:`, producerError);
                  continue;
                } else {
                  producerId = existingProducer.id;
                }
                
                // Create track-producer relationship
                const { error: relError } = await supabase
                  .from("track_producers")
                  .upsert({
                    track_id: trackId,
                    producer_id: producerId,
                    source: "discogs",
                    confidence: confidence as any,
                    metadata: {
                      discogs_release_id: release.id,
                      producer_role: producer.role
                    }
                  }, {
                    onConflict: "track_id,producer_id,source"
                  });
                
                if (relError) {
                  console.error(`Error creating track-producer relationship:`, relError);
                  continue;
                }
                
                producersFound.push({
                  name: producer.name,
                  id: producerId,
                  source: "discogs",
                  confidence
                });
              }
            }
          }
        }
      } catch (discogsError) {
        console.error(`Error using Discogs API:`, discogsError);
        // Last source tried, so we'll just return what we have
      }
    }
    
    return {
      success: true,
      message: `Found ${producersFound.length} producers for track`,
      producers: producersFound
    };
  } catch (error) {
    console.error(`Error identifying producers for track ${trackId}:`, error);
    
    // Log error to our database
    await supabase.rpc("log_error", {
      p_error_type: "processing",
      p_source: "identify_producers",
      p_message: `Error identifying producers`,
      p_stack_trace: error.stack || "",
      p_context: { trackId },
      p_item_id: trackId,
      p_item_type: "track"
    });
    
    return {
      success: false,
      message: `Error identifying producers: ${error.message}`
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
    const { trackId } = await req.json();
    
    if (!trackId) {
      return new Response(
        JSON.stringify({
          success: false,
          error: "trackId is required",
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

    const result = await identifyProducers(trackId);
    
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
    console.error("Error handling identify-producers request:", error);
    
    // Log error to our database
    try {
      await supabase.rpc("log_error", {
        p_error_type: "endpoint",
        p_source: "identify_producers",
        p_message: "Error handling identify-producers request",
        p_stack_trace: error.stack || "",
        p_context: { error: error.message },
      });
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
