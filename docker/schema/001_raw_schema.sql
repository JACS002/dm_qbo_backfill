CREATE SCHEMA IF NOT EXISTS raw;

-- Customers
CREATE TABLE IF NOT EXISTS raw.qb_customers (
  id TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_start_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_end_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  page_number INTEGER,
  page_size INTEGER,
  request_payload JSONB
);

-- Items
CREATE TABLE IF NOT EXISTS raw.qb_items (
  id TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_start_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_end_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  page_number INTEGER,
  page_size INTEGER,
  request_payload JSONB
);

-- Invoices
CREATE TABLE IF NOT EXISTS raw.qb_invoices (
  id TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_start_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  extract_window_end_utc TIMESTAMP WITH TIME ZONE NOT NULL,
  page_number INTEGER,
  page_size INTEGER,
  request_payload JSONB
);
