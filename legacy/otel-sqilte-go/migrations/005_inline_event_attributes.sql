-- Migration 005: Inline event attributes as compact JSON
--
-- This is the auditable DDL portion of the migration. The application applies
-- it transactionally and runs the typed log_attr backfill in Go between adding
-- this column and dropping the legacy table. RunMigrations is the supported
-- complete migration path; executing this file alone does not backfill data.

ALTER TABLE log_event
    ADD COLUMN attributes_json TEXT NOT NULL DEFAULT '{}';
