package model

import "time"

// SourcePatient represents the source schema from database_emr.pasien
type SourcePatient struct {
	IDPasien          int       `json:"id_pasien" db:"id_pasien"`
	NamaDepan         string    `json:"nama_depan" db:"nama_depan"`
	NamaBelakang      string    `json:"nama_belakang" db:"nama_belakang"`
	TanggalLahir      time.Time `json:"tanggal_lahir" db:"tanggal_lahir"`
	JenisKelamin      string    `json:"jenis_kelamin" db:"jenis_kelamin"`
	Email             string    `json:"email" db:"email"`
	NoTelepon         string    `json:"no_telepon" db:"no_telepon"`
	Alamat            string    `json:"alamat" db:"alamat"`
	Kota              string    `json:"kota" db:"kota"`
	Provinsi          string    `json:"provinsi" db:"provinsi"`
	KodePos           string    `json:"kode_pos" db:"kode_pos"`
	GolonganDarah     string    `json:"golongan_darah" db:"golongan_darah"`
	KontakDarurat     string    `json:"kontak_darurat" db:"kontak_darurat"`
	NoKontakDarurat   string    `json:"no_kontak_darurat" db:"no_kontak_darurat"`
	TanggalRegistrasi time.Time `json:"tanggal_registrasi" db:"tanggal_registrasi"`
}
