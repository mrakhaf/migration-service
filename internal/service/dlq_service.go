package service

import (
	"context"
	"encoding/json"
	"fmt"

	"migration-service/internal/model"
	"migration-service/internal/repository"
	"migration-service/internal/transformer"

	"github.com/sirupsen/logrus"
)

// DLQService handles Dead Letter Queue operations
type DLQService struct {
	dlqRepo     *repository.DLQRepository
	targetRepo  *repository.TargetRepository
	transformer *transformer.Transformer
	logger      *logrus.Logger
}

// NewDLQService creates a new DLQ service
func NewDLQService(
	dlqRepo *repository.DLQRepository,
	targetRepo *repository.TargetRepository,
	transformer *transformer.Transformer,
	logger *logrus.Logger,
) *DLQService {
	return &DLQService{
		dlqRepo:     dlqRepo,
		targetRepo:  targetRepo,
		transformer: transformer,
		logger:      logger,
	}
}

// ReprocessDLQ reprocesses failed records from the DLQ
func (s *DLQService) ReprocessDLQ(ctx context.Context, batchSize int) (*model.DLQStatus, error) {
	s.logger.Info("Starting DLQ reprocessing")

	// Get DLQ status
	status, err := s.dlqRepo.GetDLQStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get DLQ status: %w", err)
	}

	if status.TotalRecords == 0 {
		s.logger.Info("No records in DLQ to reprocess")
		return status, nil
	}

	s.logger.Infof("Found %d records in DLQ to reprocess", status.TotalRecords)

	// Process records in batches
	totalProcessed := 0
	totalSuccess := 0
	totalFailed := 0

	for totalProcessed < status.TotalRecords {
		// Fetch next batch
		records, err := s.dlqRepo.FetchDLQ(ctx, batchSize)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch DLQ batch: %w", err)
		}

		if len(records) == 0 {
			break
		}

		s.logger.Infof("Processing batch of %d records from DLQ", len(records))

		// Process each record
		for _, record := range records {
			success, err := s.reprocessRecord(ctx, record)
			if err != nil {
				s.logger.Errorf("Failed to reprocess DLQ record ID %d: %v", record.ID, err)
				totalFailed++
				continue
			}

			if success {
				totalSuccess++
				s.logger.Infof("Successfully reprocessed DLQ record ID %d", record.ID)
			} else {
				totalFailed++
				s.logger.Warnf("Failed to reprocess DLQ record ID %d after retry", record.ID)
			}

			totalProcessed++
		}

		// Update status after each batch
		status, err = s.dlqRepo.GetDLQStatus(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to update DLQ status: %w", err)
		}
	}

	s.logger.Infof("DLQ reprocessing completed. Processed: %d, Success: %d, Failed: %d",
		totalProcessed, totalSuccess, totalFailed)

	return status, nil
}

// reprocessRecord attempts to reprocess a single DLQ record
func (s *DLQService) reprocessRecord(ctx context.Context, record model.DLQRecord) (bool, error) {
	// Check retry count
	if record.RetryCount >= 3 {
		s.logger.Warnf("DLQ record ID %d has reached maximum retry count (%d)", record.ID, record.RetryCount)
		return false, nil
	}

	// Convert payload back to NewPatient
	var newPatient model.NewPatient
	payloadJSON, err := json.Marshal(record.Payload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal payload: %w", err)
	}

	err = newPatient.FromJSON(payloadJSON)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal payload to NewPatient: %w", err)
	}

	// Convert to TargetPatient
	targetPatient := model.TargetPatient{
		PasienUUID:           newPatient.PasienUUID,
		NamaLengkap:          newPatient.NamaLengkap,
		TanggalLahir:         newPatient.TanggalLahir,
		Gender:               newPatient.Gender,
		Email:                newPatient.Email,
		Telepon:              newPatient.Telepon,
		AlamatLengkap:        newPatient.AlamatLengkap,
		Kota:                 newPatient.Kota,
		Provinsi:             newPatient.Provinsi,
		KodePos:              newPatient.KodePos,
		GolonganDarah:        newPatient.GolonganDarah,
		NamaKontakDarurat:    newPatient.NamaKontakDarurat,
		TeleponKontakDarurat: newPatient.TeleponKontakDarurat,
		TanggalRegistrasi:    newPatient.TanggalRegistrasi,
	}

	// Validate the patient data
	err = s.transformer.ValidateTargetPatient(targetPatient)
	if err != nil {
		return false, fmt.Errorf("invalid patient data: %w", err)
	}

	// Attempt to insert into target database
	err = s.targetRepo.BulkInsert(ctx, []model.TargetPatient{targetPatient})
	if err != nil {
		// Update retry count and error
		newRetryCount := record.RetryCount + 1
		err = s.dlqRepo.UpdateRetryCount(ctx, record.ID, newRetryCount)
		if err != nil {
			return false, fmt.Errorf("failed to update retry count: %w", err)
		}

		s.logger.Warnf("Failed to reprocess DLQ record ID %d, updated retry count to %d", record.ID, newRetryCount)
		return false, nil
	}

	// Success - delete from DLQ
	err = s.dlqRepo.DeleteDLQ(ctx, record.ID)
	if err != nil {
		return false, fmt.Errorf("failed to delete DLQ record after successful reprocessing: %w", err)
	}

	s.logger.Infof("Successfully reprocessed and removed DLQ record ID %d", record.ID)
	return true, nil
}

// GetDLQStatus returns the current DLQ status
func (s *DLQService) GetDLQStatus(ctx context.Context) (*model.DLQStatus, error) {
	return s.dlqRepo.GetDLQStatus(ctx)
}
