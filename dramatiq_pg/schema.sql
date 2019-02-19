\set ON_ERROR_STOP on

CREATE SCHEMA dramatiq;

CREATE TYPE dramatiq."state" AS ENUM (
  'queued',
  'consumed',
  'rejected',
  'done'
);

CREATE TABLE dramatiq.queue(
  id SERIAL PRIMARY KEY,
  queue_name TEXT NOT NULL DEFAULT 'default',
  message_id uuid UNIQUE,
  "state" dramatiq."state",
  mtime TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  -- message as encoded by dramatiq.
  message JSONB,
  "result" JSONB
);
