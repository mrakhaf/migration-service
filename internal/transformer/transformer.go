package transformer

import (
	"fmt"

	"migration-service/internal/model"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// Transformer handles data transformation from source to target schema
type Transformer struct {
	logger *logrus.Logger
}

// NewTransformer creates a new transformer
func NewTransformer(logger *logrus.Logger) *Transformer {
	return &Transformer{
		logger: logger,
	}
}

// TransformBatch transforms a batch of source patients to target patients
func (t *Transformer) TransformBatch(sourcePatients []model.SourcePatient) ([]model.TargetPatient, []error, []model.SourcePatient) {
	if len(sourcePatients) == 0 {
		return []model.TargetPatient{}, []error{}, []model.SourcePatient{}
	}

	targetPatients := make([]model.TargetPatient, 0, len(sourcePatients))
	errors := make([]error, 0)
	errorDatas := make([]model.SourcePatient, 0)

	for _, source := range sourcePatients {
		target, err := t.Transform(source)
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to transform patient ID %d: %w", source.IDPasien, err))
			errorDatas = append(errorDatas, source)
			continue
		}
		targetPatients = append(targetPatients, target)
	}

	t.logger.Infof("Transformed %d patients, %d errors", len(targetPatients), len(errors))
	return targetPatients, errors, errorDatas
}

// Transform transforms a single source patient to target patient
func (t *Transformer) Transform(source model.SourcePatient) (model.TargetPatient, error) {
	// Validate required fields
	if source.NamaDepan == "" && source.NamaBelakang == "" {
		return model.TargetPatient{}, fmt.Errorf("patient ID %d has no name", source.IDPasien)
	}

	// Map gender from source to target
	gender, err := t.mapGender(source.JenisKelamin)
	if err != nil {
		return model.TargetPatient{}, fmt.Errorf("invalid gender for patient ID %d: %w", source.IDPasien, err)
	}

	// Generate UUID for the patient
	pasienUUID := uuid.New().String()

	target := model.TargetPatient{
		PasienUUID:           pasienUUID,
		NamaLengkap:          t.combineNames(source.NamaDepan, source.NamaBelakang),
		TanggalLahir:         source.TanggalLahir,
		Gender:               gender,
		Email:                source.Email,
		Telepon:              source.NoTelepon,
		AlamatLengkap:        source.Alamat,
		Kota:                 source.Kota,
		Provinsi:             source.Provinsi,
		KodePos:              source.KodePos,
		GolonganDarah:        source.GolonganDarah,
		NamaKontakDarurat:    source.KontakDarurat,
		TeleponKontakDarurat: source.NoKontakDarurat,
		TanggalRegistrasi:    source.TanggalRegistrasi,
	}

	return target, nil
}

// mapGender maps source gender to target gender format
func (t *Transformer) mapGender(sourceGender string) (string, error) {
	switch sourceGender {
	case "L", "Laki-laki", "Male", "M":
		return "M", nil
	case "P", "Perempuan", "Female", "F":
		return "F", nil
	case "":
		return "", nil // Allow empty gender
	default:
		return "", fmt.Errorf("unknown gender: %s", sourceGender)
	}
}

// combineNames combines first and last name
func (t *Transformer) combineNames(firstName, lastName string) string {
	if firstName == "" {
		return lastName
	}
	if lastName == "" {
		return firstName
	}
	return firstName + " " + lastName
}

// ValidateTargetPatient validates the target patient data
func (t *Transformer) ValidateTargetPatient(patient model.TargetPatient) error {
	if patient.PasienUUID == "" {
		return fmt.Errorf("pasien_uuid is required")
	}

	if patient.NamaLengkap == "" {
		return fmt.Errorf("nama_lengkap is required")
	}

	if patient.TanggalLahir.IsZero() {
		return fmt.Errorf("tanggal_lahir is required")
	}

	if patient.Gender != "" && patient.Gender != "M" && patient.Gender != "F" {
		return fmt.Errorf("invalid gender: %s", patient.Gender)
	}

	return nil
}
