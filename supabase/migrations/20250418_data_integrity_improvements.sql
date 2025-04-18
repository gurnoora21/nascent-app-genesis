
-- 1. Add Foreign Key Constraints
ALTER TABLE track_albums 
  ADD CONSTRAINT fk_track_albums_track 
  FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE;

ALTER TABLE track_albums 
  ADD CONSTRAINT fk_track_albums_album 
  FOREIGN KEY (album_id) REFERENCES albums(id) ON DELETE CASCADE;

ALTER TABLE track_artists 
  ADD CONSTRAINT fk_track_artists_track 
  FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE;

ALTER TABLE track_artists 
  ADD CONSTRAINT fk_track_artists_artist 
  FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE;

ALTER TABLE artist_genres 
  ADD CONSTRAINT fk_artist_genres_artist 
  FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE;

ALTER TABLE artist_genres 
  ADD CONSTRAINT fk_artist_genres_genre 
  FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE;

-- 2. Add Unique Constraints
ALTER TABLE artists 
  ADD CONSTRAINT uq_artists_spotify_id 
  UNIQUE (spotify_id) 
  WHERE spotify_id IS NOT NULL;

ALTER TABLE albums 
  ADD CONSTRAINT uq_albums_spotify_id 
  UNIQUE (spotify_id) 
  WHERE spotify_id IS NOT NULL;

ALTER TABLE tracks 
  ADD CONSTRAINT uq_tracks_spotify_id 
  UNIQUE (spotify_id) 
  WHERE spotify_id IS NOT NULL;

-- Unique constraints for join tables
ALTER TABLE track_artists 
  ADD CONSTRAINT uq_track_artists 
  UNIQUE (track_id, artist_id);

ALTER TABLE track_albums 
  ADD CONSTRAINT uq_track_albums 
  UNIQUE (track_id, album_id);

ALTER TABLE artist_genres 
  ADD CONSTRAINT uq_artist_genres 
  UNIQUE (artist_id, genre_id, source);

-- 3. Add Partial Indexes for performance
CREATE INDEX idx_processing_items_pending 
  ON processing_items(batch_id, priority DESC, created_at) 
  WHERE status = 'pending';

-- 4. Create dead letter queue table for robustness
CREATE TABLE public.dead_letter_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  original_item_id UUID NOT NULL,
  original_batch_id UUID NOT NULL,
  item_type TEXT NOT NULL,
  item_id TEXT NOT NULL,
  error_message TEXT,
  metadata JSONB,
  retry_count INTEGER,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- 5. Add function for reconciliation of failed batches
CREATE OR REPLACE FUNCTION public.reset_expired_batches()
RETURNS INTEGER AS $$
DECLARE
  v_count INTEGER;
BEGIN
  UPDATE processing_batches
  SET 
    status = 'pending',
    claim_expires_at = NULL,
    claimed_by = NULL,
    updated_at = now()
  WHERE 
    status = 'processing' AND
    claim_expires_at < now();
  
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 6. Create function to move failed items to dead letter queue
CREATE OR REPLACE FUNCTION public.move_to_dead_letter_queue(p_item_id UUID)
RETURNS BOOLEAN AS $$
DECLARE
  v_item processing_items;
BEGIN
  -- Get the item details
  SELECT * INTO v_item 
  FROM processing_items 
  WHERE id = p_item_id AND status = 'error';
  
  IF NOT FOUND THEN
    RETURN FALSE;
  END IF;
  
  -- Insert into dead letter queue
  INSERT INTO dead_letter_items (
    original_item_id, original_batch_id, item_type, item_id,
    error_message, metadata, retry_count, created_at, updated_at
  ) VALUES (
    v_item.id, v_item.batch_id, v_item.item_type, v_item.item_id,
    v_item.last_error, v_item.metadata, v_item.retry_count, now(), now()
  );
  
  -- Update the original item status
  UPDATE processing_items
  SET status = 'completed',
      metadata = jsonb_set(
        COALESCE(metadata, '{}'::jsonb),
        '{moved_to_dead_letter}',
        'true'::jsonb
      ),
      updated_at = now()
  WHERE id = p_item_id;
  
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 7. Function to requeue items from dead letter queue
CREATE OR REPLACE FUNCTION public.requeue_dead_letter_item(p_dead_letter_id UUID, p_batch_id UUID DEFAULT NULL)
RETURNS UUID AS $$
DECLARE
  v_dead_letter dead_letter_items;
  v_batch_id UUID;
  v_new_item_id UUID;
BEGIN
  -- Get the dead letter item
  SELECT * INTO v_dead_letter 
  FROM dead_letter_items 
  WHERE id = p_dead_letter_id;
  
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Dead letter item not found';
  END IF;
  
  -- Determine batch ID - use provided one or create a new batch
  IF p_batch_id IS NULL THEN
    INSERT INTO processing_batches (
      batch_type, status, metadata
    ) VALUES (
      'requeue_dead_letter', 'pending', 
      jsonb_build_object('source_dead_letter_id', p_dead_letter_id)
    ) RETURNING id INTO v_batch_id;
  ELSE
    v_batch_id := p_batch_id;
  END IF;
  
  -- Create new processing item
  INSERT INTO processing_items (
    batch_id, item_type, item_id, status, priority, metadata
  ) VALUES (
    v_batch_id, v_dead_letter.item_type, v_dead_letter.item_id, 
    'pending', 10, -- High priority
    jsonb_build_object(
      'original_dead_letter_id', p_dead_letter_id,
      'requeued_at', now()
    )
  ) RETURNING id INTO v_new_item_id;
  
  -- Update the dead letter item
  UPDATE dead_letter_items
  SET metadata = jsonb_set(
    COALESCE(metadata, '{}'::jsonb),
    '{requeued}',
    jsonb_build_object(
      'batch_id', v_batch_id,
      'new_item_id', v_new_item_id,
      'requeued_at', now()
    )
  ),
  updated_at = now()
  WHERE id = p_dead_letter_id;
  
  RETURN v_new_item_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 8. Trigger for automatic track batch creation when albums are completed
CREATE OR REPLACE FUNCTION public.enqueue_tracks_for_completed_album()
RETURNS TRIGGER AS $$
DECLARE
  v_batch_id UUID;
BEGIN
  -- Only proceed if this is a newly completed album
  IF (TG_OP = 'UPDATE' AND NEW.status = 'completed' AND OLD.status != 'completed') THEN
    -- Create a new batch for processing tracks
    INSERT INTO processing_batches (
      batch_type, status, metadata
    ) VALUES (
      'process_tracks', 'pending', 
      jsonb_build_object(
        'source', 'trigger',
        'source_album_id', NEW.item_id
      )
    ) RETURNING id INTO v_batch_id;
    
    -- Enqueue the album for track processing
    INSERT INTO processing_items (
      batch_id, item_type, item_id, status, priority, metadata
    ) VALUES (
      v_batch_id, 'album', NEW.item_id, 'pending', 5,
      jsonb_build_object(
        'source', 'trigger',
        'created_from', 'album_completion_trigger',
        'original_album_item_id', NEW.id
      )
    );
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger on processing_items
CREATE TRIGGER trg_enqueue_tracks_after_album_completion
AFTER UPDATE ON processing_items
FOR EACH ROW
WHEN (NEW.item_type = 'album' AND NEW.status = 'completed' AND OLD.status != 'completed')
EXECUTE FUNCTION public.enqueue_tracks_for_completed_album();

-- 9. Add metrics table for observability
CREATE TABLE public.pipeline_metrics (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_name TEXT NOT NULL,
  metric_value BIGINT NOT NULL,
  dimensions JSONB,
  recorded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX idx_pipeline_metrics_name_time 
  ON pipeline_metrics(metric_name, recorded_at DESC);

-- 10. Function to increment metrics
CREATE OR REPLACE FUNCTION public.increment_metric(
  p_metric_name TEXT,
  p_increment INTEGER DEFAULT 1,
  p_dimensions JSONB DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
  INSERT INTO pipeline_metrics (metric_name, metric_value, dimensions, recorded_at)
  VALUES (p_metric_name, p_increment, p_dimensions, now());
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
