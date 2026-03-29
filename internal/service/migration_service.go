package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"migration-service/internal/config"
	"migration-service/internal/database"
	"migration-service/internal/model"
	"migration-service/internal/repository"
	"migration-service/internal/transformer"
	"migration-service/internal/worker"

	"github.com/sirupsen/logrus"
)

// MigrationService orchestrates the entire migration process
type MigrationService struct {
	config              *config.Config
	dbManager           *database.DatabaseManager
	sourceRepo          *repository.SourceRepository
	targetRepo          *repository.TargetRepository
	dlqRepo             *repository.DLQRepository
	progressRepo        *repository.MigrationProgressRepository
	transformer         *transformer.Transformer
	workerPool          *worker.WorkerPool
	dlqService          *DLQService
	logger              *logrus.Logger
	progress            *MigrationProgress
	mu                  sync.RWMutex
	shutdownChan        chan struct{}
	isRunning           bool
	isDLQRunning        bool
	usePostgresProgress bool
}

// MigrationProgress tracks the migration progress
type MigrationProgress struct {
	TotalCount      int       `json:"total_count"`
	ProcessedCount  int       `json:"processed_count"`
	SuccessCount    int       `json:"success_count"`
	FailureCount    int       `json:"failure_count"`
	LastProcessedID int       `json:"last_processed_id"`
	StartTime       time.Time `json:"start_time"`
	CurrentStatus   string    `json:"current_status"`
	Errors          []string  `json:"errors"`
}

// NewMigrationService creates a new migration service with DLQ support
func NewMigrationService(cfg *config.Config, logger *logrus.Logger) (*MigrationService, error) {
	// Initialize database connections
	dbManager, err := database.NewDatabaseManager(
		cfg.GetSourceDBConnectionString(),
		cfg.GetTargetDBConnectionString(),
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create database manager: %w", err)
	}

	// Initialize repositories
	sourceRepo := repository.NewSourceRepository(dbManager.SourceDB, logger)
	targetRepo := repository.NewTargetRepository(dbManager.TargetDB, logger)
	dlqRepo := repository.NewDLQRepository(dbManager.TargetDB, logger)

	// Initialize transformer
	transformer := transformer.NewTransformer(logger)

	// Initialize worker pool
	workerPool := worker.NewWorkerPool(
		cfg.Migration.WorkerCount,
		sourceRepo,
		targetRepo,
		transformer,
		logger,
	)

	// Initialize DLQ service
	dlqService := NewDLQService(dlqRepo, targetRepo, transformer, logger)

	// Initialize progress repository
	progressRepo := repository.NewMigrationProgressRepository(dbManager.TargetDB, logger)

	// Initialize progress tracking
	progress := &MigrationProgress{
		TotalCount:      0,
		ProcessedCount:  0,
		SuccessCount:    0,
		FailureCount:    0,
		LastProcessedID: 0,
		StartTime:       time.Now(),
		CurrentStatus:   "initialized",
		Errors:          make([]string, 0),
	}

	// Load existing progress from database
	dbProgress, err := progressRepo.GetProgress(context.Background(), "patient_migration")
	if err != nil {
		logger.Warnf("Failed to load existing progress from database: %v", err)
	} else {
		// Update in-memory progress with database values
		progress.TotalCount = dbProgress.TotalCount
		progress.ProcessedCount = dbProgress.ProcessedCount
		progress.SuccessCount = dbProgress.SuccessCount
		progress.FailureCount = dbProgress.FailureCount
		progress.LastProcessedID = dbProgress.LastProcessedID
		progress.CurrentStatus = dbProgress.CurrentStatus
		progress.Errors = dbProgress.Errors
	}

	return &MigrationService{
		config:              cfg,
		dbManager:           dbManager,
		sourceRepo:          sourceRepo,
		targetRepo:          targetRepo,
		dlqRepo:             dlqRepo,
		progressRepo:        progressRepo,
		transformer:         transformer,
		workerPool:          workerPool,
		dlqService:          dlqService,
		logger:              logger,
		progress:            progress,
		shutdownChan:        make(chan struct{}),
		isRunning:           false,
		isDLQRunning:        false,
		usePostgresProgress: true,
	}, nil
}

// Start begins the migration process
func (ms *MigrationService) Start(ctx context.Context) error {
	ms.mu.Lock()
	if ms.isRunning {
		ms.mu.Unlock()
		return fmt.Errorf("migration is already running")
	}
	ms.isRunning = true
	ms.mu.Unlock()

	ms.logger.Info("Starting migration service")

	// Update progress
	ms.updateProgress(func(p *MigrationProgress) {
		p.CurrentStatus = "starting"
		p.StartTime = time.Now()
	})

	// Get initial counts
	totalCount, err := ms.sourceRepo.GetTotalCount(ctx)
	if err != nil {
		ms.updateProgress(func(p *MigrationProgress) {
			p.CurrentStatus = "error"
			p.Errors = append(p.Errors, err.Error())
		})
		return fmt.Errorf("failed to get total count: %w", err)
	}

	processedCount, err := ms.targetRepo.GetProcessedCount(ctx)
	if err != nil {
		ms.updateProgress(func(p *MigrationProgress) {
			p.CurrentStatus = "error"
			p.Errors = append(p.Errors, err.Error())
		})
		return fmt.Errorf("failed to get processed count: %w", err)
	}

	ms.updateProgress(func(p *MigrationProgress) {
		p.TotalCount = totalCount
		p.ProcessedCount = processedCount
		p.CurrentStatus = "running"
	})

	ms.logger.Infof("Migration started. Total: %d, Already processed: %d", totalCount, processedCount)

	// Check if migration is already complete
	if processedCount >= totalCount {
		ms.logger.Info("Migration already complete")
		ms.updateProgress(func(p *MigrationProgress) {
			p.CurrentStatus = "completed"
		})
		return nil
	}

	// Start periodic progress sync
	progressSyncDone := make(chan struct{})
	go ms.periodicProgressSync(ctx, progressSyncDone)

	// Perform migration with retry mechanism
	err = ms.performMigrationWithRetry(ctx)

	// Stop periodic progress sync
	close(progressSyncDone)

	if err != nil {
		ms.updateProgress(func(p *MigrationProgress) {
			p.CurrentStatus = "error"
			p.Errors = append(p.Errors, err.Error())
		})
		return fmt.Errorf("migration failed after retries: %w", err)
	}

	ms.logger.Info("Migration completed successfully")
	ms.updateProgress(func(p *MigrationProgress) {
		p.CurrentStatus = "completed"
		p.ProcessedCount = p.TotalCount
	})

	return nil
}

// performMigrationWithRetry performs migration with retry mechanism
func (ms *MigrationService) performMigrationWithRetry(ctx context.Context) error {
	var lastError error

	for attempt := 1; attempt <= ms.config.Migration.MaxRetries; attempt++ {
		ms.logger.Infof("Migration attempt %d of %d", attempt, ms.config.Migration.MaxRetries)

		// Check for shutdown signal
		select {
		case <-ms.shutdownChan:
			ms.logger.Info("Migration shutdown requested")
			return fmt.Errorf("migration shutdown requested")
		default:
		}

		// Get the last processed ID for resuming
		lastProcessedID, err := ms.getResumeLastProcessedID(ctx)
		if err != nil {
			ms.logger.Warnf("Failed to get resume ID, starting from beginning: %v", err)
			lastProcessedID = 0
		}

		// Create a progress callback to update progress during processing
		progressCallback := func(successCount, failureCount, lastID int) {
			ms.updateProgress(func(p *MigrationProgress) {
				p.SuccessCount += successCount
				p.FailureCount += failureCount
				p.ProcessedCount = p.SuccessCount + p.FailureCount
				if lastID > p.LastProcessedID {
					p.LastProcessedID = lastID
				}
			})
			// Sync progress to database periodically
			ms.syncProgressToDatabase()
		}

		result, err := ms.workerPool.ProcessBatchesConcurrentFromIDWithProgress(
			ctx,
			ms.config.Migration.BatchSize,
			ms.config.Migration.WorkerCount,
			lastProcessedID,
			progressCallback,
		)

		if err == nil {
			// Final progress update
			ms.updateProgress(func(p *MigrationProgress) {
				p.SuccessCount += result.SuccessCount
				p.FailureCount += result.FailureCount
				p.ProcessedCount = p.SuccessCount + p.FailureCount
				if result.LastID > p.LastProcessedID {
					p.LastProcessedID = result.LastID
				}
			})
			// Final sync to database
			ms.syncProgressToDatabase()
			return nil
		}

		lastError = err
		ms.logger.Errorf("Migration attempt %d failed: %v", attempt, err)

		// Add error to progress
		ms.updateProgress(func(p *MigrationProgress) {
			p.Errors = append(p.Errors, fmt.Sprintf("Attempt %d failed: %v", attempt, err))
		})
		ms.syncProgressToDatabase()

		// Wait before retry (except on last attempt)
		if attempt < ms.config.Migration.MaxRetries {
			ms.logger.Infof("Waiting %v before retry...", ms.config.Migration.RetryDelay)
			select {
			case <-time.After(ms.config.Migration.RetryDelay):
			case <-ms.shutdownChan:
				ms.logger.Info("Migration shutdown requested during retry wait")
				return fmt.Errorf("migration shutdown requested")
			}
		}
	}

	return fmt.Errorf("migration failed after %d attempts: %w", ms.config.Migration.MaxRetries, lastError)
}

// Stop gracefully stops the migration service
func (ms *MigrationService) Stop() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if !ms.isRunning {
		return nil
	}

	ms.logger.Info("Stopping migration service...")
	ms.isRunning = false
	close(ms.shutdownChan)

	// Wait for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), ms.config.Migration.ShutdownTimeout)
	defer cancel()

	// Perform health check during shutdown
	err := ms.dbManager.HealthCheck(ctx)
	if err != nil {
		ms.logger.Warnf("Database health check failed during shutdown: %v", err)
	}

	ms.logger.Info("Migration service stopped")
	return nil
}

// GetProgress returns the current migration progress
func (ms *MigrationService) GetProgress() *MigrationProgress {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Create a copy to avoid race conditions
	progressCopy := *ms.progress
	return &progressCopy
}

// updateProgress safely updates the progress
func (ms *MigrationService) updateProgress(fn func(*MigrationProgress)) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	fn(ms.progress)
}

// IsRunning returns whether the migration is currently running
func (ms *MigrationService) IsRunning() bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.isRunning
}

// GetStatus returns a detailed status of the migration
func (ms *MigrationService) GetStatus() map[string]interface{} {
	progress := ms.GetProgress()
	elapsed := time.Since(progress.StartTime)

	status := map[string]interface{}{
		"is_running":        ms.IsRunning(),
		"total_count":       progress.TotalCount,
		"processed_count":   progress.ProcessedCount,
		"success_count":     progress.SuccessCount,
		"failure_count":     progress.FailureCount,
		"last_processed_id": progress.LastProcessedID,
		"start_time":        progress.StartTime,
		"elapsed_time":      elapsed,
		"current_status":    progress.CurrentStatus,
		"errors":            progress.Errors,
	}

	if progress.TotalCount > 0 {
		status["progress_percentage"] = float64(progress.ProcessedCount) / float64(progress.TotalCount) * 100
	}

	return status
}

// HealthCheck performs a health check on the migration service
func (ms *MigrationService) HealthCheck(ctx context.Context) error {
	// Check database connections
	err := ms.dbManager.HealthCheck(ctx)
	if err != nil {
		return fmt.Errorf("database health check failed: %w", err)
	}

	// Check if service is responsive
	ms.mu.RLock()
	isRunning := ms.isRunning
	ms.mu.RUnlock()

	if !isRunning && ms.GetProgress().CurrentStatus == "error" {
		return fmt.Errorf("service is not running and has errors")
	}

	return nil
}

// ReprocessDLQ reprocesses failed records from the DLQ
func (ms *MigrationService) ReprocessDLQ(ctx context.Context, batchSize int) error {
	ms.mu.Lock()
	if ms.isRunning {
		ms.mu.Unlock()
		return fmt.Errorf("migration is currently running, DLQ reprocessing cannot start")
	}
	if ms.isDLQRunning {
		ms.mu.Unlock()
		return fmt.Errorf("DLQ reprocessing is already running")
	}
	ms.isDLQRunning = true
	ms.mu.Unlock()

	defer func() {
		ms.mu.Lock()
		ms.isDLQRunning = false
		ms.mu.Unlock()
	}()

	ms.logger.Info("Starting DLQ reprocessing")

	status, err := ms.dlqService.ReprocessDLQ(ctx, batchSize)
	if err != nil {
		return fmt.Errorf("DLQ reprocessing failed: %w", err)
	}

	ms.logger.Infof("DLQ reprocessing completed. Total records: %d", status.TotalRecords)
	return nil
}

// GetDLQStatus returns the current DLQ status
func (ms *MigrationService) GetDLQStatus(ctx context.Context) (*model.DLQStatus, error) {
	return ms.dlqService.GetDLQStatus(ctx)
}

// IsDLQRunning returns whether DLQ reprocessing is currently running
func (ms *MigrationService) IsDLQRunning() bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.isDLQRunning
}

// syncProgressToDatabase synchronizes the in-memory progress to the database
func (ms *MigrationService) syncProgressToDatabase() error {
	if !ms.usePostgresProgress {
		return nil
	}

	ms.mu.RLock()
	progress := *ms.progress
	ms.mu.RUnlock()

	// Convert MigrationProgress to repository.MigrationProgress
	dbProgress := &repository.MigrationProgress{
		MigrationName:   "patient_migration",
		TotalCount:      progress.TotalCount,
		ProcessedCount:  progress.ProcessedCount,
		SuccessCount:    progress.SuccessCount,
		FailureCount:    progress.FailureCount,
		LastProcessedID: progress.LastProcessedID,
		CurrentStatus:   progress.CurrentStatus,
		Errors:          progress.Errors,
	}

	err := ms.progressRepo.UpdateProgress(context.Background(), dbProgress)
	if err != nil {
		ms.logger.Errorf("Failed to sync progress to database: %v", err)
		return err
	}

	ms.logger.Debugf("Successfully synced progress to database")
	return nil
}

// getResumeLastProcessedID returns the last processed ID for resuming migration
func (ms *MigrationService) getResumeLastProcessedID(ctx context.Context) (int, error) {
	if ms.usePostgresProgress {
		// Try to get from database first
		lastID, err := ms.progressRepo.GetLastProcessedID(ctx, "patient_migration")
		if err == nil {
			ms.logger.Infof("Resuming migration from last processed ID: %d", lastID)
			return lastID, nil
		}
		ms.logger.Warnf("Failed to get last processed ID from database, falling back to in-memory: %v", err)
	}

	// Fall back to in-memory progress
	ms.mu.RLock()
	lastID := ms.progress.LastProcessedID
	ms.mu.RUnlock()

	ms.logger.Infof("Using in-memory last processed ID: %d", lastID)
	return lastID, nil
}

// updateLastProcessedID updates the last processed ID in both memory and database
func (ms *MigrationService) updateLastProcessedID(lastID int) {
	ms.mu.Lock()
	ms.progress.LastProcessedID = lastID
	ms.mu.Unlock()

	if ms.usePostgresProgress {
		err := ms.progressRepo.UpdateLastProcessedID(context.Background(), "patient_migration", lastID)
		if err != nil {
			ms.logger.Errorf("Failed to update last processed ID in database: %v", err)
		}
	}
}

// periodicProgressSync periodically syncs progress to the database
func (ms *MigrationService) periodicProgressSync(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second) // Sync every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-done:
			ms.logger.Info("Periodic progress sync stopped")
			return
		case <-ctx.Done():
			ms.logger.Info("Context cancelled, stopping periodic progress sync")
			return
		case <-ticker.C:
			// Only sync if migration is running
			ms.mu.RLock()
			isRunning := ms.isRunning
			ms.mu.RUnlock()

			if isRunning {
				err := ms.syncProgressToDatabase()
				if err != nil {
					ms.logger.Warnf("Periodic progress sync failed: %v", err)
				}
			}
		}
	}
}
