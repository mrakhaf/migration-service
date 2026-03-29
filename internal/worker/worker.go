package worker

import (
	"context"
	"fmt"
	"sync"

	"migration-service/internal/model"
	"migration-service/internal/repository"
	"migration-service/internal/transformer"

	"github.com/sirupsen/logrus"
)

// Worker represents a single worker in the pool
type Worker struct {
	ID          int
	source      *repository.SourceRepository
	target      *repository.TargetRepository
	transformer *transformer.Transformer
	logger      *logrus.Logger
}

// NewWorker creates a new worker
func NewWorker(id int, source *repository.SourceRepository, target *repository.TargetRepository, transformer *transformer.Transformer, logger *logrus.Logger) *Worker {
	return &Worker{
		ID:          id,
		source:      source,
		target:      target,
		transformer: transformer,
		logger:      logger,
	}
}

// ProcessBatch processes a single batch of patients
func (w *Worker) ProcessBatch(ctx context.Context, lastID, batchSize int) (*model.BatchResult, error) {
	w.logger.Infof("Worker %d: Processing batch starting from ID %d", w.ID, lastID)

	// Get batch from source
	sourcePatients, err := w.source.GetBatch(ctx, lastID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("worker %d failed to get batch: %w", w.ID, err)
	}

	if len(sourcePatients) == 0 {
		w.logger.Infof("Worker %d: No more patients to process", w.ID)
		return &model.BatchResult{SuccessCount: 0, FailureCount: 0, LastID: lastID}, nil
	}

	// Transform data
	targetPatients, transformErrors := w.transformer.TransformBatch(sourcePatients)
	if len(transformErrors) > 0 {
		w.logger.Warnf("Worker %d: %d transformation errors", w.ID, len(transformErrors))
	}

	// Filter out invalid patients
	validPatients := make([]model.TargetPatient, 0, len(targetPatients))
	for _, patient := range targetPatients {
		if err := w.transformer.ValidateTargetPatient(patient); err != nil {
			w.logger.Warnf("Worker %d: Invalid patient data: %v", w.ID, err)
			continue
		}
		validPatients = append(validPatients, patient)
	}

	if len(validPatients) == 0 {
		w.logger.Warnf("Worker %d: No valid patients to insert", w.ID)
		return &model.BatchResult{
			SuccessCount: 0,
			FailureCount: len(sourcePatients),
			LastID:       sourcePatients[len(sourcePatients)-1].IDPasien,
			Errors:       transformErrors,
		}, nil
	}

	// Insert into target database using optimized method
	err = w.target.BulkInsertOptimized(ctx, validPatients, true) // Check duplicates for safety
	if err != nil {
		return nil, fmt.Errorf("worker %d failed to insert batch: %w", w.ID, err)
	}

	lastID = sourcePatients[len(sourcePatients)-1].IDPasien
	result := &model.BatchResult{
		SuccessCount: len(validPatients),
		FailureCount: len(sourcePatients) - len(validPatients) + len(transformErrors),
		LastID:       lastID,
		Errors:       transformErrors,
	}

	w.logger.Infof("Worker %d: Successfully processed batch, last ID: %d, success: %d, failed: %d",
		w.ID, lastID, result.SuccessCount, result.FailureCount)

	return result, nil
}

// WorkerPool manages a pool of workers
type WorkerPool struct {
	workers     []*Worker
	source      *repository.SourceRepository
	target      *repository.TargetRepository
	transformer *transformer.Transformer
	logger      *logrus.Logger
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workerCount int, source *repository.SourceRepository, target *repository.TargetRepository, transformer *transformer.Transformer, logger *logrus.Logger) *WorkerPool {
	workers := make([]*Worker, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = NewWorker(i+1, source, target, transformer, logger)
	}

	return &WorkerPool{
		workers:     workers,
		source:      source,
		target:      target,
		transformer: transformer,
		logger:      logger,
	}
}

// ProcessBatches processes batches using the worker pool
func (wp *WorkerPool) ProcessBatches(ctx context.Context, batchSize, workerCount int) (*model.BatchResult, error) {
	// Use starting ID of 0 for backward compatibility
	return wp.ProcessBatchesFromID(ctx, batchSize, workerCount, 0)
}

// ProcessBatchesFromID processes batches starting from a specific ID
func (wp *WorkerPool) ProcessBatchesFromID(ctx context.Context, batchSize, workerCount, startID int) (*model.BatchResult, error) {
	wp.logger.Infof("Starting batch processing with %d workers from ID %d", workerCount, startID)

	// Get initial state
	totalCount, err := wp.source.GetTotalCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get total count: %w", err)
	}

	maxID, err := wp.source.GetMaxID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get max ID: %w", err)
	}

	wp.logger.Infof("Total patients to process: %d, Max ID: %d", totalCount, maxID)

	// Process in batches
	var totalSuccess, totalFailure, lastID int
	var allErrors []error
	currentID := startID

	for currentID < maxID {
		// Determine batch size for this iteration
		remaining := maxID - currentID
		actualBatchSize := batchSize
		if remaining < batchSize {
			actualBatchSize = remaining
		}

		// Process batch with first available worker
		result, err := wp.workers[0].ProcessBatch(ctx, currentID, actualBatchSize)
		if err != nil {
			return nil, fmt.Errorf("batch processing failed: %w", err)
		}

		totalSuccess += result.SuccessCount
		totalFailure += result.FailureCount
		lastID = result.LastID
		allErrors = append(allErrors, result.Errors...)

		// Update progress
		progress := float64(lastID) / float64(maxID) * 100
		wp.logger.Infof("Progress: %.2f%% (%d/%d), Success: %d, Failed: %d",
			progress, lastID, maxID, totalSuccess, totalFailure)

		// Move to next batch
		currentID = lastID
	}

	finalResult := &model.BatchResult{
		SuccessCount: totalSuccess,
		FailureCount: totalFailure,
		LastID:       lastID,
		Errors:       allErrors,
	}

	wp.logger.Infof("Batch processing completed. Total success: %d, failed: %d", totalSuccess, totalFailure)
	return finalResult, nil
}

// ProcessBatchesConcurrent processes batches concurrently using all workers
func (wp *WorkerPool) ProcessBatchesConcurrent(ctx context.Context, batchSize, workerCount int) (*model.BatchResult, error) {
	// Use starting ID of 0 for backward compatibility
	return wp.ProcessBatchesConcurrentFromID(ctx, batchSize, workerCount, 0)
}

// ProcessBatchesConcurrentFromID processes batches concurrently starting from a specific ID
func (wp *WorkerPool) ProcessBatchesConcurrentFromID(ctx context.Context, batchSize, workerCount, startID int) (*model.BatchResult, error) {
	wp.logger.Infof("Starting concurrent batch processing with %d workers from ID %d", workerCount, startID)

	// Get initial state
	totalCount, err := wp.source.GetTotalCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get total count: %w", err)
	}

	maxID, err := wp.source.GetMaxID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get max ID: %w", err)
	}

	wp.logger.Infof("Total patients to process: %d, Max ID: %d", totalCount, maxID)

	// Create channels for work distribution
	workChan := make(chan int, 1000) // Channel to distribute work (last IDs)
	resultChan := make(chan *model.BatchResult, workerCount)
	errorChan := make(chan error, workerCount)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wp.processWorker(ctx, workerID, workChan, resultChan, errorChan, batchSize)
		}(i)
	}

	// Distribute work
	go func() {
		defer close(workChan)
		distributeCurrentID := startID
		for distributeCurrentID < maxID {
			workChan <- distributeCurrentID
			// Get next batch to determine the next starting point
			nextBatch, err := wp.workers[0].source.GetBatch(ctx, distributeCurrentID, batchSize)
			if err != nil || len(nextBatch) == 0 {
				break
			}
			distributeCurrentID = nextBatch[len(nextBatch)-1].IDPasien
		}
	}()

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// Collect results
	var totalSuccess, totalFailure, lastID int
	var allErrors []error

	for result := range resultChan {
		totalSuccess += result.SuccessCount
		totalFailure += result.FailureCount
		if result.LastID > lastID {
			lastID = result.LastID
		}
		allErrors = append(allErrors, result.Errors...)
	}

	// Check for errors
	select {
	case err := <-errorChan:
		return nil, fmt.Errorf("worker error: %w", err)
	default:
	}

	finalResult := &model.BatchResult{
		SuccessCount: totalSuccess,
		FailureCount: totalFailure,
		LastID:       lastID,
		Errors:       allErrors,
	}

	wp.logger.Infof("Concurrent batch processing completed. Total success: %d, failed: %d", totalSuccess, totalFailure)
	return finalResult, nil
}

// ProcessBatchesConcurrentFromIDWithProgress processes batches concurrently with progress callback
func (wp *WorkerPool) ProcessBatchesConcurrentFromIDWithProgress(
	ctx context.Context,
	batchSize, workerCount, startID int,
	progressCallback func(successCount, failureCount, lastID int),
) (*model.BatchResult, error) {
	wp.logger.Infof("Starting concurrent batch processing with %d workers from ID %d (with progress)", workerCount, startID)

	// Get initial state
	totalCount, err := wp.source.GetTotalCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get total count: %w", err)
	}

	maxID, err := wp.source.GetMaxID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get max ID: %w", err)
	}

	wp.logger.Infof("Total patients to process: %d, Max ID: %d", totalCount, maxID)

	// Create channels for work distribution
	workChan := make(chan int, 1000) // Channel to distribute work (last IDs)
	resultChan := make(chan *model.BatchResult, workerCount)
	errorChan := make(chan error, workerCount)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wp.processWorkerWithProgress(ctx, workerID, workChan, resultChan, errorChan, batchSize, progressCallback)
		}(i)
	}

	// Distribute work
	go func() {
		defer close(workChan)
		distributeCurrentID := startID
		for distributeCurrentID < maxID {
			workChan <- distributeCurrentID
			// Get next batch to determine the next starting point
			nextBatch, err := wp.workers[0].source.GetBatch(ctx, distributeCurrentID, batchSize)
			if err != nil || len(nextBatch) == 0 {
				break
			}
			distributeCurrentID = nextBatch[len(nextBatch)-1].IDPasien
		}
	}()

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// Collect results
	var totalSuccess, totalFailure, lastID int
	var allErrors []error

	for result := range resultChan {
		totalSuccess += result.SuccessCount
		totalFailure += result.FailureCount
		if result.LastID > lastID {
			lastID = result.LastID
		}
		allErrors = append(allErrors, result.Errors...)
	}

	// Check for errors
	select {
	case err := <-errorChan:
		return nil, fmt.Errorf("worker error: %w", err)
	default:
	}

	finalResult := &model.BatchResult{
		SuccessCount: totalSuccess,
		FailureCount: totalFailure,
		LastID:       lastID,
		Errors:       allErrors,
	}

	wp.logger.Infof("Concurrent batch processing completed. Total success: %d, failed: %d", totalSuccess, totalFailure)
	return finalResult, nil
}

// processWorker processes work from the channel
func (wp *WorkerPool) processWorker(ctx context.Context, workerID int, workChan <-chan int, resultChan chan<- *model.BatchResult, errorChan chan<- error, batchSize int) {
	worker := wp.workers[workerID]

	for lastID := range workChan {
		result, err := worker.ProcessBatch(ctx, lastID, batchSize)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- result
	}
}

// processWorkerWithProgress processes work from the channel with progress callback
func (wp *WorkerPool) processWorkerWithProgress(
	ctx context.Context,
	workerID int,
	workChan <-chan int,
	resultChan chan<- *model.BatchResult,
	errorChan chan<- error,
	batchSize int,
	progressCallback func(successCount, failureCount, lastID int),
) {
	worker := wp.workers[workerID]

	for lastID := range workChan {
		result, err := worker.ProcessBatch(ctx, lastID, batchSize)
		if err != nil {
			errorChan <- err
			return
		}

		// Call progress callback
		if progressCallback != nil {
			progressCallback(result.SuccessCount, result.FailureCount, result.LastID)
		}

		resultChan <- result
	}
}
