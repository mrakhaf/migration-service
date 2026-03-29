-- Migration Progress Table
-- This table tracks the progress of data migration to allow resuming after restart

CREATE TABLE IF NOT EXISTS migration_progress (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL UNIQUE, -- ✅ FIX di sini
    total_count INTEGER NOT NULL DEFAULT 0,
    processed_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_processed_id INTEGER NOT NULL DEFAULT 0,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    current_status VARCHAR(50) NOT NULL DEFAULT 'initialized',
    errors TEXT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create index on migration_name for faster lookups
CREATE INDEX IF NOT EXISTS idx_migration_progress_name ON migration_progress(migration_name);

-- Create index on last_updated for tracking recent updates
CREATE INDEX IF NOT EXISTS idx_migration_progress_updated ON migration_progress(updated_at);

-- Create a trigger to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_migration_progress_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Drop trigger if it exists to avoid conflicts
DROP TRIGGER IF EXISTS trigger_migration_progress_updated_at ON migration_progress;

-- Create the trigger
CREATE TRIGGER trigger_migration_progress_updated_at
    BEFORE UPDATE ON migration_progress
    FOR EACH ROW
    EXECUTE FUNCTION update_migration_progress_updated_at();

-- Insert initial progress record if it doesn't exist
INSERT INTO migration_progress (migration_name, total_count, processed_count, success_count, failure_count, last_processed_id, current_status)
VALUES ('patient_migration', 0, 0, 0, 0, 0, 'initialized')
ON CONFLICT (migration_name) DO NOTHING;

-- Add comments for documentation
COMMENT ON TABLE migration_progress IS 'Tracks the progress of data migration to allow resuming after restart';
COMMENT ON COLUMN migration_progress.migration_name IS 'Name of the migration process';
COMMENT ON COLUMN migration_progress.total_count IS 'Total number of records to migrate';
COMMENT ON COLUMN migration_progress.processed_count IS 'Number of records that have been processed';
COMMENT ON COLUMN migration_progress.success_count IS 'Number of records successfully migrated';
COMMENT ON COLUMN migration_progress.failure_count IS 'Number of records that failed to migrate';
COMMENT ON COLUMN migration_progress.last_processed_id IS 'ID of the last successfully processed record';
COMMENT ON COLUMN migration_progress.start_time IS 'When the migration started';
COMMENT ON COLUMN migration_progress.last_updated IS 'When the progress was last updated';
COMMENT ON COLUMN migration_progress.current_status IS 'Current status of the migration (initialized, running, completed, error)';
COMMENT ON COLUMN migration_progress.errors IS 'Array of error messages encountered during migration';