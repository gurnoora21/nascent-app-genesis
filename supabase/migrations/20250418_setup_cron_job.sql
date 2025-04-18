
-- Enable required extensions if not already enabled
CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS pg_net;

-- Set up a cron job to run every hour to reset expired batches
SELECT cron.schedule(
  'reset-expired-batches-hourly',
  '0 * * * *',  -- Run at the start of every hour
  $$
  SELECT
    net.http_post(
        url:='https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/reset-expired-batches',
        headers:='{"Content-Type": "application/json", "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5zeHh6aGhiY3d6YXR2bHVsZnlwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ4NDQ4NDYsImV4cCI6MjA2MDQyMDg0Nn0.CR3TFPYipFCs6sL_51rJ3kOKR3iQGr8tJgZJ2GLlrDk"}'::jsonb,
        body:='{}'::jsonb
    ) as request_id;
  $$
);

-- Set up a metrics cleanup job to prevent metrics table from growing too large
-- This job runs daily and removes metrics older than 30 days
SELECT cron.schedule(
  'cleanup-old-metrics',
  '0 0 * * *',  -- Run at midnight every day
  $$
  DELETE FROM public.pipeline_metrics
  WHERE recorded_at < NOW() - INTERVAL '30 days';
  $$
);
