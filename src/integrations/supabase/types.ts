export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  public: {
    Tables: {
      api_rate_limits: {
        Row: {
          api_name: string
          endpoint: string
          id: string
          last_response: Json | null
          requests_limit: number | null
          requests_remaining: number | null
          reset_at: string | null
          updated_at: string
        }
        Insert: {
          api_name: string
          endpoint: string
          id?: string
          last_response?: Json | null
          requests_limit?: number | null
          requests_remaining?: number | null
          reset_at?: string | null
          updated_at?: string
        }
        Update: {
          api_name?: string
          endpoint?: string
          id?: string
          last_response?: Json | null
          requests_limit?: number | null
          requests_remaining?: number | null
          reset_at?: string | null
          updated_at?: string
        }
        Relationships: []
      }
      artists: {
        Row: {
          discovered_at: string
          genres: string[] | null
          id: string
          image_url: string | null
          last_processed_at: string | null
          metadata: Json | null
          name: string
          popularity: number | null
          spotify_id: string | null
          spotify_url: string | null
          updated_at: string
        }
        Insert: {
          discovered_at?: string
          genres?: string[] | null
          id?: string
          image_url?: string | null
          last_processed_at?: string | null
          metadata?: Json | null
          name: string
          popularity?: number | null
          spotify_id?: string | null
          spotify_url?: string | null
          updated_at?: string
        }
        Update: {
          discovered_at?: string
          genres?: string[] | null
          id?: string
          image_url?: string | null
          last_processed_at?: string | null
          metadata?: Json | null
          name?: string
          popularity?: number | null
          spotify_id?: string | null
          spotify_url?: string | null
          updated_at?: string
        }
        Relationships: []
      }
      error_logs: {
        Row: {
          context: Json | null
          created_at: string
          error_type: string
          id: string
          item_id: string | null
          item_type: string | null
          message: string
          resolved: boolean | null
          source: string
          stack_trace: string | null
        }
        Insert: {
          context?: Json | null
          created_at?: string
          error_type: string
          id?: string
          item_id?: string | null
          item_type?: string | null
          message: string
          resolved?: boolean | null
          source: string
          stack_trace?: string | null
        }
        Update: {
          context?: Json | null
          created_at?: string
          error_type?: string
          id?: string
          item_id?: string | null
          item_type?: string | null
          message?: string
          resolved?: boolean | null
          source?: string
          stack_trace?: string | null
        }
        Relationships: []
      }
      processing_batches: {
        Row: {
          batch_type: string
          claim_expires_at: string | null
          claimed_by: string | null
          completed_at: string | null
          created_at: string
          error_message: string | null
          id: string
          items_failed: number | null
          items_processed: number | null
          items_total: number | null
          metadata: Json | null
          started_at: string | null
          status: Database["public"]["Enums"]["processing_status"]
          updated_at: string
        }
        Insert: {
          batch_type: string
          claim_expires_at?: string | null
          claimed_by?: string | null
          completed_at?: string | null
          created_at?: string
          error_message?: string | null
          id?: string
          items_failed?: number | null
          items_processed?: number | null
          items_total?: number | null
          metadata?: Json | null
          started_at?: string | null
          status?: Database["public"]["Enums"]["processing_status"]
          updated_at?: string
        }
        Update: {
          batch_type?: string
          claim_expires_at?: string | null
          claimed_by?: string | null
          completed_at?: string | null
          created_at?: string
          error_message?: string | null
          id?: string
          items_failed?: number | null
          items_processed?: number | null
          items_total?: number | null
          metadata?: Json | null
          started_at?: string | null
          status?: Database["public"]["Enums"]["processing_status"]
          updated_at?: string
        }
        Relationships: []
      }
      processing_items: {
        Row: {
          batch_id: string
          created_at: string
          id: string
          item_id: string
          item_type: string
          last_error: string | null
          metadata: Json | null
          priority: number | null
          retry_count: number | null
          status: Database["public"]["Enums"]["processing_status"]
          updated_at: string
        }
        Insert: {
          batch_id: string
          created_at?: string
          id?: string
          item_id: string
          item_type: string
          last_error?: string | null
          metadata?: Json | null
          priority?: number | null
          retry_count?: number | null
          status?: Database["public"]["Enums"]["processing_status"]
          updated_at?: string
        }
        Update: {
          batch_id?: string
          created_at?: string
          id?: string
          item_id?: string
          item_type?: string
          last_error?: string | null
          metadata?: Json | null
          priority?: number | null
          retry_count?: number | null
          status?: Database["public"]["Enums"]["processing_status"]
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "processing_items_batch_id_fkey"
            columns: ["batch_id"]
            isOneToOne: false
            referencedRelation: "processing_batches"
            referencedColumns: ["id"]
          },
        ]
      }
      producers: {
        Row: {
          aliases: string[] | null
          discogs_id: string | null
          discovered_at: string
          genius_id: string | null
          id: string
          image_url: string | null
          metadata: Json | null
          name: string
          spotify_id: string | null
          updated_at: string
        }
        Insert: {
          aliases?: string[] | null
          discogs_id?: string | null
          discovered_at?: string
          genius_id?: string | null
          id?: string
          image_url?: string | null
          metadata?: Json | null
          name: string
          spotify_id?: string | null
          updated_at?: string
        }
        Update: {
          aliases?: string[] | null
          discogs_id?: string | null
          discovered_at?: string
          genius_id?: string | null
          id?: string
          image_url?: string | null
          metadata?: Json | null
          name?: string
          spotify_id?: string | null
          updated_at?: string
        }
        Relationships: []
      }
      track_producers: {
        Row: {
          confidence: Database["public"]["Enums"]["confidence_level"]
          created_at: string
          id: string
          metadata: Json | null
          producer_id: string
          source: Database["public"]["Enums"]["source_type"]
          track_id: string
          updated_at: string
        }
        Insert: {
          confidence?: Database["public"]["Enums"]["confidence_level"]
          created_at?: string
          id?: string
          metadata?: Json | null
          producer_id: string
          source: Database["public"]["Enums"]["source_type"]
          track_id: string
          updated_at?: string
        }
        Update: {
          confidence?: Database["public"]["Enums"]["confidence_level"]
          created_at?: string
          id?: string
          metadata?: Json | null
          producer_id?: string
          source?: Database["public"]["Enums"]["source_type"]
          track_id?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "track_producers_producer_id_fkey"
            columns: ["producer_id"]
            isOneToOne: false
            referencedRelation: "producers"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "track_producers_track_id_fkey"
            columns: ["track_id"]
            isOneToOne: false
            referencedRelation: "tracks"
            referencedColumns: ["id"]
          },
        ]
      }
      tracks: {
        Row: {
          album_name: string | null
          artist_id: string
          discovered_at: string
          id: string
          metadata: Json | null
          name: string
          popularity: number | null
          preview_url: string | null
          release_date: string | null
          spotify_id: string | null
          spotify_url: string | null
          updated_at: string
        }
        Insert: {
          album_name?: string | null
          artist_id: string
          discovered_at?: string
          id?: string
          metadata?: Json | null
          name: string
          popularity?: number | null
          preview_url?: string | null
          release_date?: string | null
          spotify_id?: string | null
          spotify_url?: string | null
          updated_at?: string
        }
        Update: {
          album_name?: string | null
          artist_id?: string
          discovered_at?: string
          id?: string
          metadata?: Json | null
          name?: string
          popularity?: number | null
          preview_url?: string | null
          release_date?: string | null
          spotify_id?: string | null
          spotify_url?: string | null
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "tracks_artist_id_fkey"
            columns: ["artist_id"]
            isOneToOne: false
            referencedRelation: "artists"
            referencedColumns: ["id"]
          },
        ]
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      claim_processing_batch: {
        Args: {
          p_batch_type: string
          p_worker_id: string
          p_claim_ttl_seconds?: number
        }
        Returns: string
      }
      clone_batch: {
        Args: { p_batch_id: string; p_include_only_failed?: boolean }
        Returns: string
      }
      find_producer_duplicates: {
        Args: { threshold?: number }
        Returns: {
          id1: string
          id2: string
          name1: string
          name2: string
          similarity: number
        }[]
      }
      gtrgm_compress: {
        Args: { "": unknown }
        Returns: unknown
      }
      gtrgm_decompress: {
        Args: { "": unknown }
        Returns: unknown
      }
      gtrgm_in: {
        Args: { "": unknown }
        Returns: unknown
      }
      gtrgm_options: {
        Args: { "": unknown }
        Returns: undefined
      }
      gtrgm_out: {
        Args: { "": unknown }
        Returns: unknown
      }
      log_error: {
        Args: {
          p_error_type: string
          p_source: string
          p_message: string
          p_stack_trace?: string
          p_context?: Json
          p_item_id?: string
          p_item_type?: string
        }
        Returns: string
      }
      normalize_producer_name: {
        Args: { name: string }
        Returns: string
      }
      release_processing_batch: {
        Args: {
          p_batch_id: string
          p_worker_id: string
          p_status: Database["public"]["Enums"]["processing_status"]
        }
        Returns: boolean
      }
      reset_batch: {
        Args: { p_batch_id: string }
        Returns: boolean
      }
      reset_failed_items: {
        Args: { p_batch_id: string }
        Returns: number
      }
      set_limit: {
        Args: { "": number }
        Returns: number
      }
      show_limit: {
        Args: Record<PropertyKey, never>
        Returns: number
      }
      show_trgm: {
        Args: { "": string }
        Returns: string[]
      }
    }
    Enums: {
      confidence_level: "low" | "medium" | "high" | "verified"
      processing_status: "pending" | "processing" | "completed" | "error"
      source_type: "spotify" | "genius" | "discogs"
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DefaultSchema = Database[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof (Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        Database[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof Database }
  ? (Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      Database[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof Database }
  ? Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof Database }
  ? Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof Database },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends { schema: keyof Database }
  ? Database[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof Database },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends { schema: keyof Database }
  ? Database[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {
      confidence_level: ["low", "medium", "high", "verified"],
      processing_status: ["pending", "processing", "completed", "error"],
      source_type: ["spotify", "genius", "discogs"],
    },
  },
} as const
