package repository

import (
	"context"
	"fmt"

	"migration-service/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// TargetRepository handles operations on the target database
type TargetRepository struct {
	db     *pgxpool.Pool
	logger *logrus.Logger
}

// NewTargetRepository creates a new target repository
func NewTargetRepository(db *pgxpool.Pool, logger *logrus.Logger) *TargetRepository {
	return &TargetRepository{
		db:     db,
		logger: logger,
	}
}

// BulkInsert inserts multiple patients into the target database using COPY with transaction
func (r *TargetRepository) BulkInsert(ctx context.Context, patients []model.TargetPatient) error {
	if len(patients) == 0 {
		return nil
	}

	// Use transaction for better performance and atomicity
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	// Use COPY for efficient bulk insert within transaction
	copyCount, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"pasien"},
		[]string{
			"pasien_uuid", "nama_lengkap", "tanggal_lahir", "gender", "email",
			"telepon", "alamat_lengkap", "kota", "provinsi", "kode_pos",
			"golongan_darah", "nama_kontak_darurat", "telepon_kontak_darurat", "tanggal_registrasi",
		},
		pgx.CopyFromSlice(len(patients), func(i int) ([]interface{}, error) {
			p := patients[i]
			return []interface{}{
				p.PasienUUID,
				p.NamaLengkap,
				p.TanggalLahir,
				p.Gender,
				p.Email,
				p.Telepon,
				p.AlamatLengkap,
				p.Kota,
				p.Provinsi,
				p.KodePos,
				p.GolonganDarah,
				p.NamaKontakDarurat,
				p.TeleponKontakDarurat,
				p.TanggalRegistrasi,
			}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("failed to bulk insert patients: %w", err)
	}

	r.logger.Infof("Successfully inserted %d patients into target database", copyCount)
	return nil
}

// BulkInsertWithDuplicates handles bulk insert with duplicate checking and transaction
func (r *TargetRepository) BulkInsertWithDuplicates(ctx context.Context, patients []model.TargetPatient) error {
	if len(patients) == 0 {
		return nil
	}

	// Use transaction for better performance and atomicity
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	// Check for duplicates first
	duplicateEmails := make(map[string]bool)
	for _, patient := range patients {
		exists, err := r.checkDuplicateInTx(ctx, tx, patient.Email)
		if err != nil {
			return fmt.Errorf("failed to check duplicate: %w", err)
		}
		if exists {
			duplicateEmails[patient.Email] = true
		}
	}

	// Filter out duplicates
	validPatients := make([]model.TargetPatient, 0, len(patients))
	for _, patient := range patients {
		if !duplicateEmails[patient.Email] {
			validPatients = append(validPatients, patient)
		}
	}

	if len(validPatients) == 0 {
		r.logger.Infof("No valid patients to insert (all duplicates)")
		return nil
	}

	// Use COPY for efficient bulk insert within transaction
	copyCount, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"pasien"},
		[]string{
			"pasien_uuid", "nama_lengkap", "tanggal_lahir", "gender", "email",
			"telepon", "alamat_lengkap", "kota", "provinsi", "kode_pos",
			"golongan_darah", "nama_kontak_darurat", "telepon_kontak_darurat", "tanggal_registrasi",
		},
		pgx.CopyFromSlice(len(validPatients), func(i int) ([]interface{}, error) {
			p := validPatients[i]
			return []interface{}{
				p.PasienUUID,
				p.NamaLengkap,
				p.TanggalLahir,
				p.Gender,
				p.Email,
				p.Telepon,
				p.AlamatLengkap,
				p.Kota,
				p.Provinsi,
				p.KodePos,
				p.GolonganDarah,
				p.NamaKontakDarurat,
				p.TeleponKontakDarurat,
				p.TanggalRegistrasi,
			}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("failed to bulk insert patients: %w", err)
	}

	r.logger.Infof("Successfully inserted %d patients into target database (filtered %d duplicates)", copyCount, len(patients)-len(validPatients))
	return nil
}

// checkDuplicateInTx checks for duplicates within a transaction
func (r *TargetRepository) checkDuplicateInTx(ctx context.Context, tx pgx.Tx, email string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pasien WHERE email = $1)", email).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check duplicate: %w", err)
	}
	return exists, nil
}

// CheckDuplicate checks if a patient with the given email already exists
func (r *TargetRepository) CheckDuplicate(ctx context.Context, email string) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pasien WHERE email = $1)", email).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check duplicate: %w", err)
	}
	return exists, nil
}

// GetProcessedCount returns the number of patients already processed
func (r *TargetRepository) GetProcessedCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRow(ctx, "SELECT COUNT(*) FROM pasien").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get processed count: %w", err)
	}
	return count, nil
}

// BulkInsertWithPreparedStmt uses prepared statements for optimal performance
func (r *TargetRepository) BulkInsertWithPreparedStmt(ctx context.Context, patients []model.TargetPatient) error {
	if len(patients) == 0 {
		return nil
	}

	// Use transaction for better performance and atomicity
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	// Prepare the insert statement
	_, err = tx.Prepare(ctx, "insert_patient", `
		INSERT INTO pasien (
			pasien_uuid, nama_lengkap, tanggal_lahir, gender, email,
			telepon, alamat_lengkap, kota, provinsi, kode_pos,
			golongan_darah, nama_kontak_darurat, telepon_kontak_darurat, tanggal_registrasi
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	// Batch insert using prepared statement
	batch := &pgx.Batch{}
	for _, patient := range patients {
		batch.Queue("insert_patient",
			patient.PasienUUID,
			patient.NamaLengkap,
			patient.TanggalLahir,
			patient.Gender,
			patient.Email,
			patient.Telepon,
			patient.AlamatLengkap,
			patient.Kota,
			patient.Provinsi,
			patient.KodePos,
			patient.GolonganDarah,
			patient.NamaKontakDarurat,
			patient.TeleponKontakDarurat,
			patient.TanggalRegistrasi,
		)
	}

	// Execute batch
	batchResults := tx.SendBatch(ctx, batch)
	defer batchResults.Close()

	// Process results
	var successCount int
	for i := 0; i < batch.Len(); i++ {
		_, err := batchResults.Exec()
		if err != nil {
			return fmt.Errorf("batch execution failed at position %d: %w", i, err)
		}
		successCount++
	}

	r.logger.Infof("Successfully inserted %d patients using prepared statements", successCount)
	return nil
}

// BulkInsertOptimized chooses the best insertion method based on data size and requirements
func (r *TargetRepository) BulkInsertOptimized(ctx context.Context, patients []model.TargetPatient, checkDuplicates bool) error {
	if len(patients) == 0 {
		return nil
	}

	// For very large batches, use COPY
	if len(patients) >= 1000 {
		if checkDuplicates {
			return r.BulkInsertWithDuplicates(ctx, patients)
		}
		return r.BulkInsert(ctx, patients)
	}

	// For smaller batches, use prepared statements
	return r.BulkInsertWithPreparedStmt(ctx, patients)
}
