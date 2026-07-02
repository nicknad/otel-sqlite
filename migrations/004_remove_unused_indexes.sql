-- Migration: Remove unused indexes to improve write performance
-- Date: 2026-07-01
-- Description: Removes indexes that are not used by any queries in the codebase.
-- This should improve write performance by ~33% based on benchmark results.

-- Indexes being KEPT (used by queries):
-- 1. idx_log_attr_event_id - used for attribute lookups by event_id
-- 2. idx_log_event_timestamp - used for time-based retention/purge queries
-- 3. idx_log_event_resource_id - used for DISTINCT resource_id queries

-- Remove unused indexes from log_event table
DROP INDEX IF EXISTS idx_log_event_severity;
DROP INDEX IF EXISTS idx_log_event_trace_id;
DROP INDEX IF EXISTS idx_log_event_severity_text;
DROP INDEX IF EXISTS idx_log_event_resource_timestamp;
DROP INDEX IF EXISTS idx_log_event_trace_timestamp;
DROP INDEX IF EXISTS idx_log_event_event_name;

-- Remove unused indexes from log_attr table
DROP INDEX IF EXISTS idx_log_attr_key;
DROP INDEX IF EXISTS idx_log_attr_value_string;
DROP INDEX IF EXISTS idx_log_attr_value_int;
DROP INDEX IF EXISTS idx_log_attr_value_double;

-- Note: If you need to search by severity, trace_id, or other fields in the future,
-- you can recreate these indexes. For now, write performance is prioritized over
-- read performance on non-critical query paths.
