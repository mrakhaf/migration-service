package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// MigrationProgressRepository handles operations on the migration_progress table
type MigrationProgressRepository struct {
	db     *pgxpool.Pool
	logger *logrus.Logger
}

// NewMigrationProgressRepository creates a new migration progress repository
func NewMigrationProgressRepository(db *pgxpool.Pool, logger *logrus.Logger) *MigrationProgressRepository {
	return &MigrationProgressRepository{
		db:     db,
		logger: logger,
	}
}

// MigrationProgress represents the migration progress record

type MigrationProgress struct {
	ID              int
	MigrationName   string
	TotalCount      int
	ProcessedCount  int
	SuccessCount    int
	FailureCount    int
	LastProcessedID int
	StartTime       time.Time
	LastUpdated     time.Time
	CurrentStatus   string
	Errors          []string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// GetProgress retrieves the current migration progress
func (r *MigrationProgressRepository) GetProgress(ctx context.Context, migrationName string) (*MigrationProgress, error) {
	query := `
		SELECT id, migration_name, total_count, processed_count, success_count, 
		       failure_count, last_processed_id, start_time, last_updated, 
		       current_status, errors, created_at, updated_at
		FROM migration_progress 
		WHERE migration_name = $1
	`

	var progress MigrationProgress
	err := r.db.QueryRow(ctx, query, migrationName).Scan(
		&progress.ID,
		&progress.MigrationName,
		&progress.TotalCount,
		&progress.ProcessedCount,
		&progress.SuccessCount,
		&progress.FailureCount,
		&progress.LastProcessedID,
		&progress.StartTime,
		&progress.LastUpdated,
		&progress.CurrentStatus,
		&progress.Errors,
		&progress.CreatedAt,
		&progress.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get migration progress: %w", err)
	}

	return &progress, nil
}

// UpdateProgress updates the migration progress
func (r *MigrationProgressRepository) UpdateProgress(ctx context.Context, progress *MigrationProgress) error {
	query := `
		UPDATE migration_progress 
		SET total_count = $2, processed_count = $3, success_count = $4, 
		    failure_count = $5, last_processed_id = $6, last_updated = NOW(),
		    current_status = $7, errors = $8
		WHERE migration_name = $1
	`

	_, err := r.db.Exec(ctx, query,
		progress.MigrationName,
		progress.TotalCount,
		progress.ProcessedCount,
		progress.SuccessCount,
		progress.FailureCount,
		progress.LastProcessedID,
		progress.CurrentStatus,
		progress.Errors,
	)

	if err != nil {
		return fmt.Errorf("failed to update migration progress: %w", err)
	}

	r.logger.Infof("Updated migration progress for %s: processed=%d, success=%d, failure=%d, last_id=%d, status=%s",
		progress.MigrationName, progress.ProcessedCount, progress.SuccessCount, progress.FailureCount, progress.LastProcessedID, progress.CurrentStatus)

	return nil
}

// GetLastProcessedID retrieves the last processed ID for resuming migration
func (r *MigrationProgressRepository) GetLastProcessedID(ctx context.Context, migrationName string) (int, error) {
	progress, err := r.GetProgress(ctx, migrationName)
	if err != nil {
		return 0, err
	}
	return progress.LastProcessedID, nil
}

// UpdateLastProcessedID updates only the last processed ID
func (r *MigrationProgressRepository) UpdateLastProcessedID(ctx context.Context, migrationName string, lastID int) error {
	query := `
		UPDATE migration_progress 
		SET last_processed_id = $2, last_updated = NOW()
		WHERE migration_name = $1
	`

	_, err := r.db.Exec(ctx, query, migrationName, lastID)
	if err != nil {
		return fmt.Errorf("failed to update last processed ID: %w", err)
	}

	r.logger.Debugf("Updated last processed ID for %s to %d", migrationName, lastID)
	return nil
}
