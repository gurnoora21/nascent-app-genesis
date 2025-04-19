
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

-- Add a more frequent pipeline monitoring job
SELECT cron.schedule(
  'monitor-pipeline-every-5-minutes',
  '*/5 * * * *',  -- Run every 5 minutes
  $$
  SELECT
    net.http_post(
        url:='https://nsxxzhhbcwzatvlulfyp.supabase.co/functions/v1/monitor-pipeline',
        headers:='{"Content-Type": "application/json", "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5zeHh6aGhiY3d6YXR2bHVsZnlwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ4NDQ4NDYsImV4cCI6MjA2MDQyMDg0Nn0.CR3TFPYipFCs6sL_51rJ3kOKR3iQGr8tJgZJ2GLlrDk"}'::jsonb,
        body:='{"checkFinalizedBatches": true, "checkForStuckBatches": true, "checkDLQThreshold": true, "checkErrorRates": true}'::jsonb
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

-- Add a job to check for circuit breaker status and potentially reset
SELECT cron.schedule(
  'circuit-breaker-check',
  '*/15 * * * *',  -- Run every 15 minutes
  $$
  -- This determines if any circuit breakers should be reset
  -- based on time passed since last incident
  WITH breakers AS (
    SELECT 
      api_name,
      max(updated_at) as last_issue_time,
      now() - max(updated_at) as time_since_issue
    FROM 
      api_rate_limits
    WHERE 
      requests_remaining <= 1 OR
      (last_response->>'status')::int >= 429
    GROUP BY 
      api_name
  )
  INSERT INTO system_logs (log_level, component, message, context)
  SELECT 
    'info',
    'circuit_breaker_check',
    'Checking circuit breaker status for ' || api_name,
    jsonb_build_object(
      'api_name', api_name,
      'last_issue_time', last_issue_time,
      'minutes_since_issue', EXTRACT(EPOCH FROM time_since_issue)/60
    )
  FROM 
    breakers
  WHERE 
    EXTRACT(EPOCH FROM time_since_issue)/60 > 15; -- If over 15 minutes, log it as recovered
  $$
);
