CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE pasien (
    pasien_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nama_lengkap VARCHAR(100),
    tanggal_lahir DATE,
    gender VARCHAR(10),
    email VARCHAR(100),
    telepon VARCHAR(20),
    alamat_lengkap VARCHAR(255),
    kota VARCHAR(50),
    provinsi VARCHAR(50),
    kode_pos VARCHAR(10),
    golongan_darah VARCHAR(5),
    nama_kontak_darurat VARCHAR(100),
    telepon_kontak_darurat VARCHAR(20),
    tanggal_registrasi DATE
);