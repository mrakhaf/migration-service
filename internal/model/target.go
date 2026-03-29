package model

import "time"

// TargetPatient represents the target schema for database_simrs.pasien
type TargetPatient struct {
	PasienUUID           string    `json:"pasien_uuid" db:"pasien_uuid"`
	NamaLengkap          string    `json:"nama_lengkap" db:"nama_lengkap"`
	TanggalLahir         time.Time `json:"tanggal_lahir" db:"tanggal_lahir"`
	Gender               string    `json:"gender" db:"gender"`
	Email                string    `json:"email" db:"email"`
	Telepon              string    `json:"telepon" db:"telepon"`
	AlamatLengkap        string    `json:"alamat_lengkap" db:"alamat_lengkap"`
	Kota                 string    `json:"kota" db:"kota"`
	Provinsi             string    `json:"provinsi" db:"provinsi"`
	KodePos              string    `json:"kode_pos" db:"kode_pos"`
	GolonganDarah        string    `json:"golongan_darah" db:"golongan_darah"`
	NamaKontakDarurat    string    `json:"nama_kontak_darurat" db:"nama_kontak_darurat"`
	TeleponKontakDarurat string    `json:"telepon_kontak_darurat" db:"telepon_kontak_darurat"`
	TanggalRegistrasi    time.Time `json:"tanggal_registrasi" db:"tanggal_registrasi"`
}

// BatchResult represents the result of processing a batch
type BatchResult struct {
	SuccessCount int
	FailureCount int
	LastID       int
	Errors       []error
}
