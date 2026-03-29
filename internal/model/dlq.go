package model

import (
	"encoding/json"
	"time"
)

// DLQRecord represents a record in the Dead Letter Queue
type DLQRecord struct {
	ID         int         `json:"id" db:"id"`
	Payload    interface{} `json:"payload" db:"payload"`
	Error      string      `json:"error" db:"error"`
	RetryCount int         `json:"retry_count" db:"retry_count"`
	CreatedAt  time.Time   `json:"created_at" db:"created_at"`
}

// NewPatient represents the patient data that can be stored in DLQ
type NewPatient struct {
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

// DLQStatus represents the status of the DLQ
type DLQStatus struct {
	TotalRecords int         `json:"total_records"`
	RetryCounts  map[int]int `json:"retry_counts"`
	Records      []DLQRecord `json:"records"`
}

// ToJSON converts NewPatient to JSON for storage in DLQ
func (np *NewPatient) ToJSON() ([]byte, error) {
	return json.Marshal(np)
}

// FromJSON converts JSON back to NewPatient
func (np *NewPatient) FromJSON(data []byte) error {
	return json.Unmarshal(data, np)
}
