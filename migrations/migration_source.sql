

CREATE TABLE pasien (
    id_pasien INT PRIMARY KEY,
    nama_depan VARCHAR(50),
    nama_belakang VARCHAR(50),
    tanggal_lahir DATE,
    jenis_kelamin VARCHAR(10),
    email VARCHAR(100),
    no_telepon VARCHAR(20),
    alamat VARCHAR(200),
    kota VARCHAR(50),
    provinsi VARCHAR(50),
    kode_pos VARCHAR(10),
    golongan_darah VARCHAR(5),
    kontak_darurat VARCHAR(100),
    no_kontak_darurat VARCHAR(20),
    tanggal_registrasi DATE
);

INSERT INTO pasien (
    id_pasien, nama_depan, nama_belakang, tanggal_lahir, jenis_kelamin,
    email, no_telepon, alamat, kota, provinsi, kode_pos,
    golongan_darah, kontak_darurat, no_kontak_darurat, tanggal_registrasi
)
SELECT 
    n AS id_pasien,
    CASE (n % 40)
        WHEN 0 THEN 'Aditya' WHEN 1 THEN 'Bayu' WHEN 2 THEN 'Cahya' WHEN 3 THEN 'Dian'
        WHEN 4 THEN 'Eko' WHEN 5 THEN 'Fajar' WHEN 6 THEN 'Galih' WHEN 7 THEN 'Hendra'
        WHEN 8 THEN 'Indah' WHEN 9 THEN 'Jaya' WHEN 10 THEN 'Kartika' WHEN 11 THEN 'Lestari'
        WHEN 12 THEN 'Mega' WHEN 13 THEN 'Nanda' WHEN 14 THEN 'Oki' WHEN 15 THEN 'Putri'
        WHEN 16 THEN 'Reza' WHEN 17 THEN 'Sari' WHEN 18 THEN 'Tuti' WHEN 19 THEN 'Umar'
        WHEN 20 THEN 'Vina' WHEN 21 THEN 'Wulan' WHEN 22 THEN 'Yanti' WHEN 23 THEN 'Zahra'
        WHEN 24 THEN 'Andi' WHEN 25 THEN 'Bella' WHEN 26 THEN 'Citra' WHEN 27 THEN 'Dinda'
        WHEN 28 THEN 'Eka' WHEN 29 THEN 'Fina' WHEN 30 THEN 'Gita' WHEN 31 THEN 'Hani'
        WHEN 32 THEN 'Imam' WHEN 33 THEN 'Juni' WHEN 34 THEN 'Kiki' WHEN 35 THEN 'Lili'
        WHEN 36 THEN 'Mira' WHEN 37 THEN 'Nina' WHEN 38 THEN 'Omar' ELSE 'Prita'
    END AS nama_depan,
    CASE (n % 30)
        WHEN 0 THEN 'Santoso' WHEN 1 THEN 'Wijaya' WHEN 2 THEN 'Kusuma' WHEN 3 THEN 'Purnama'
        WHEN 4 THEN 'Pratama' WHEN 5 THEN 'Saputra' WHEN 6 THEN 'Wibowo' WHEN 7 THEN 'Hidayat'
        WHEN 8 THEN 'Setiawan' WHEN 9 THEN 'Firmansyah' WHEN 10 THEN 'Sutanto' WHEN 11 THEN 'Hartono'
        WHEN 12 THEN 'Nugroho' WHEN 13 THEN 'Rahman' WHEN 14 THEN 'Hakim' WHEN 15 THEN 'Anwar'
        WHEN 16 THEN 'Budiman' WHEN 17 THEN 'Susanto' WHEN 18 THEN 'Kurniawan' WHEN 19 THEN 'Gunawan'
        WHEN 20 THEN 'Utomo' WHEN 21 THEN 'Mulyadi' WHEN 22 THEN 'Suharto' WHEN 23 THEN 'Pranoto'
        WHEN 24 THEN 'Suryanto' WHEN 25 THEN 'Ramadhan' WHEN 26 THEN 'Prasetyo' WHEN 27 THEN 'Saputro'
        WHEN 28 THEN 'Permana' ELSE 'Mahendra'
    END AS nama_belakang,
    DATE '1940-01-01' + (n % 30000) * INTERVAL '1 day' AS tanggal_lahir,
    CASE (n % 2) WHEN 0 THEN 'Laki-laki' ELSE 'Perempuan' END AS jenis_kelamin,
    CONCAT(LOWER('user'), '.', n, '@email.com') AS email,
    CONCAT('08', (n % 3) + 1, LPAD((n % 9000000 + 1000000)::TEXT, 8, '0')) AS no_telepon,
    CONCAT('Jl. Dummy No. ', (n % 200) + 1) AS alamat,
    'Jakarta' AS kota,
    'DKI Jakarta' AS provinsi,
    LPAD(((n % 99999) + 10000)::TEXT, 5, '0') AS kode_pos,
    CASE (n % 8)
        WHEN 0 THEN 'O+' WHEN 1 THEN 'A+' WHEN 2 THEN 'B+'
        WHEN 3 THEN 'AB+' WHEN 4 THEN 'O-' WHEN 5 THEN 'A-'
        WHEN 6 THEN 'B-' ELSE 'AB-'
    END AS golongan_darah,
    'Kontak Dummy' AS kontak_darurat,
    CONCAT('08', ((n + 5000) % 3) + 1, LPAD(((n + 3000) % 9000000 + 1000000)::TEXT, 8, '0')) AS no_kontak_darurat,
    DATE '2010-01-01' + (n % 5475) * INTERVAL '1 day' AS tanggal_registrasi
FROM generate_series(1, 2000000) AS n;