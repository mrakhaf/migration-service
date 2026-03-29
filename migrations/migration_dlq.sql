-- Migration script for Dead Letter Queue (DLQ) table
-- This script creates the migration_dlq table in the target database

-- Create the migration_dlq table
CREATE TABLE IF NOT EXISTS migration_dlq (
    id SERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    error TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create an index on created_at for better query performance
CREATE INDEX IF NOT EXISTS idx_migration_dlq_created_at ON migration_dlq(created_at);

-- Create an index on retry_count for monitoring retry distribution
CREATE INDEX IF NOT EXISTS idx_migration_dlq_retry_count ON migration_dlq(retry_count);

-- Optional: Create a composite index for common queries
CREATE INDEX IF NOT EXISTS idx_migration_dlq_status ON migration_dlq(retry_count, created_at);

-- Add comments for documentation
COMMENT ON TABLE migration_dlq IS 'Dead Letter Queue for storing failed migration records';
COMMENT ON COLUMN migration_dlq.id IS 'Unique identifier for the DLQ record';
COMMENT ON COLUMN migration_dlq.payload IS 'JSON payload of the failed patient data';
COMMENT ON COLUMN migration_dlq.error IS 'Error message from the failed migration attempt';
COMMENT ON COLUMN migration_dlq.retry_count IS 'Number of retry attempts for this record';
COMMENT ON COLUMN migration_dlq.created_at IS 'Timestamp when the record was created';

-- Optional: Create a function to clean up old DLQ records (after 30 days)
CREATE OR REPLACE FUNCTION cleanup_dlq_records(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM migration_dlq 
    WHERE created_at < NOW() - INTERVAL '1 day' * days_to_keep;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$;

-- Optional: Create a view for easier monitoring
CREATE OR REPLACE VIEW dlq_monitoring_view AS
SELECT 
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE retry_count = 0) as retry_0,
    COUNT(*) FILTER (WHERE retry_count = 1) as retry_1,
    COUNT(*) FILTER (WHERE retry_count = 2) as retry_2,
    COUNT(*) FILTER (WHERE retry_count >= 3) as retry_max,
    MIN(created_at) as oldest_record,
    MAX(created_at) as newest_record
FROM migration_dlq;

-- Grant necessary permissions (adjust as needed for your security model)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON migration_dlq TO your_application_user;
-- GRANT USAGE, SELECT ON SEQUENCE migration_dlq_id_seq TO your_application_user;
-- GRANT EXECUTE ON FUNCTION cleanup_dlq_records(INTEGER) TO your_application_user;
-- GRANT SELECT ON dlq_monitoring_view TO your_application_user;

-- Insert sample data for testing (optional)
-- INSERT INTO migration_dlq (payload, error, retry_count) VALUES
-- ('{"pasien_uuid": "test-uuid-1", "nama_lengkap": "Test Patient 1"}', 'Connection timeout', 2),
-- ('{"pasien_uuid": "test-uuid-2", "nama_lengkap": "Test Patient 2"}', 'Validation error', 1);

-- Verify table creation
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'migration_dlq'
ORDER BY ordinal_position;

-- Show indexes
SELECT 
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'migration_dlq';