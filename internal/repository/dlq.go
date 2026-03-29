package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"migration-service/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// DLQRepository handles operations on the Dead Letter Queue
type DLQRepository struct {
	db     *pgxpool.Pool
	logger *logrus.Logger
}

// NewDLQRepository creates a new DLQ repository
func NewDLQRepository(db *pgxpool.Pool, logger *logrus.Logger) *DLQRepository {
	return &DLQRepository{
		db:     db,
		logger: logger,
	}
}

// InsertDLQ inserts a failed record into the DLQ table
func (r *DLQRepository) InsertDLQ(ctx context.Context, payload interface{}, errorStr string, retryCount int) error {
	// Convert payload to JSON
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload to JSON: %w", err)
	}

	// Insert into DLQ table
	_, err = r.db.Exec(ctx, `
		INSERT INTO migration_dlq (payload, error, retry_count, created_at)
		VALUES ($1, $2, $3, NOW())
	`, payloadJSON, errorStr, retryCount)

	if err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	r.logger.Infof("Inserted failed record into DLQ with retry count: %d", retryCount)
	return nil
}

// FetchDLQ fetches records from the DLQ table
func (r *DLQRepository) FetchDLQ(ctx context.Context, limit int) ([]model.DLQRecord, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id, payload, error, retry_count, created_at
		FROM migration_dlq
		ORDER BY created_at ASC
		LIMIT $1
	`, limit)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch DLQ records: %w", err)
	}
	defer rows.Close()

	var records []model.DLQRecord
	for rows.Next() {
		var record model.DLQRecord
		var payloadJSON []byte

		err := rows.Scan(&record.ID, &payloadJSON, &record.Error, &record.RetryCount, &record.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan DLQ record: %w", err)
		}

		// Parse JSON payload
		var payload interface{}
		err = json.Unmarshal(payloadJSON, &payload)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload JSON: %w", err)
		}

		record.Payload = payload
		records = append(records, record)
	}

	return records, nil
}

// DeleteDLQ deletes a record from the DLQ table by ID
func (r *DLQRepository) DeleteDLQ(ctx context.Context, id int) error {
	result, err := r.db.Exec(ctx, "DELETE FROM migration_dlq WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete DLQ record: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("no DLQ record found with ID: %d", id)
	}

	r.logger.Infof("Deleted DLQ record with ID: %d", id)
	return nil
}

// UpdateRetryCount updates the retry count for a DLQ record
func (r *DLQRepository) UpdateRetryCount(ctx context.Context, id int, retryCount int) error {
	result, err := r.db.Exec(ctx, "UPDATE migration_dlq SET retry_count = $1 WHERE id = $2", retryCount, id)
	if err != nil {
		return fmt.Errorf("failed to update retry count: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("no DLQ record found with ID: %d", id)
	}

	r.logger.Infof("Updated retry count for DLQ record ID: %d to %d", id, retryCount)
	return nil
}

// GetDLQStatus returns the current status of the DLQ
func (r *DLQRepository) GetDLQStatus(ctx context.Context) (*model.DLQStatus, error) {
	// Get total count
	var totalRecords int
	err := r.db.QueryRow(ctx, "SELECT COUNT(*) FROM migration_dlq").Scan(&totalRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to get total DLQ records: %w", err)
	}

	// Get retry count distribution
	rows, err := r.db.Query(ctx, `
		SELECT retry_count, COUNT(*) as count
		FROM migration_dlq
		GROUP BY retry_count
		ORDER BY retry_count
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get retry count distribution: %w", err)
	}
	defer rows.Close()

	retryCounts := make(map[int]int)
	for rows.Next() {
		var retryCount, count int
		err := rows.Scan(&retryCount, &count)
		if err != nil {
			return nil, fmt.Errorf("failed to scan retry count: %w", err)
		}
		retryCounts[retryCount] = count
	}

	// Get recent records (limit to 100 for performance)
	records, err := r.FetchDLQ(ctx, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch DLQ records: %w", err)
	}

	status := &model.DLQStatus{
		TotalRecords: totalRecords,
		RetryCounts:  retryCounts,
		Records:      records,
	}

	return status, nil
}

// GetDB returns the database connection (for internal use)
func (r *DLQRepository) GetDB() *pgxpool.Pool {
	return r.db
}
