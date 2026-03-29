package repository

import (
	"context"
	"fmt"

	"migration-service/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// SourceRepository handles operations on the source database
type SourceRepository struct {
	db     *pgxpool.Pool
	logger *logrus.Logger
}

// NewSourceRepository creates a new source repository
func NewSourceRepository(db *pgxpool.Pool, logger *logrus.Logger) *SourceRepository {
	return &SourceRepository{
		db:     db,
		logger: logger,
	}
}

// GetBatch retrieves a batch of patients from source database using cursor-based pagination
func (r *SourceRepository) GetBatch(ctx context.Context, lastID, batchSize int) ([]model.SourcePatient, error) {
	query := `
		SELECT id_pasien, nama_depan, nama_belakang, tanggal_lahir, jenis_kelamin, 
		       email, no_telepon, alamat, kota, provinsi, kode_pos, golongan_darah,
		       kontak_darurat, no_kontak_darurat, tanggal_registrasi
		FROM pasien 
		WHERE id_pasien > $1 
		ORDER BY id_pasien ASC 
		LIMIT $2
	`

	rows, err := r.db.Query(ctx, query, lastID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query source database: %w", err)
	}
	defer rows.Close()

	var patients []model.SourcePatient
	for rows.Next() {
		var patient model.SourcePatient
		err := rows.Scan(
			&patient.IDPasien,
			&patient.NamaDepan,
			&patient.NamaBelakang,
			&patient.TanggalLahir,
			&patient.JenisKelamin,
			&patient.Email,
			&patient.NoTelepon,
			&patient.Alamat,
			&patient.Kota,
			&patient.Provinsi,
			&patient.KodePos,
			&patient.GolonganDarah,
			&patient.KontakDarurat,
			&patient.NoKontakDarurat,
			&patient.TanggalRegistrasi,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan patient row: %w", err)
		}
		patients = append(patients, patient)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	r.logger.Infof("Retrieved %d patients from source database", len(patients))
	return patients, nil
}

// GetTotalCount returns the total number of patients in source database
func (r *SourceRepository) GetTotalCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRow(ctx, "SELECT COUNT(*) FROM pasien").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total count: %w", err)
	}
	return count, nil
}

// GetMaxID returns the maximum ID in the source database
func (r *SourceRepository) GetMaxID(ctx context.Context) (int, error) {
	var maxID int
	err := r.db.QueryRow(ctx, "SELECT COALESCE(MAX(id_pasien), 0) FROM pasien").Scan(&maxID)
	if err != nil {
		return 0, fmt.Errorf("failed to get max ID: %w", err)
	}
	return maxID, nil
}
