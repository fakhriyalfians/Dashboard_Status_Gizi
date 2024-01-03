-- Membuat Tabel Dataset
CREATE TABLE dataset (
    nik VARCHAR(20)  NOT NULL,
    nama VARCHAR(225),
    jk VARCHAR(1),
    tgl_lahir DATE,
    bb_lahir FLOAT,
    tb_lahir FLOAT,
    nama_ortu VARCHAR(225),
    prov VARCHAR(50),
    kab_kota VARCHAR(50),
    kec VARCHAR(50),
    pukesmas VARCHAR(50),
    desa_kel VARCHAR(50),
    posyandu VARCHAR(50),
    alamat VARCHAR(150),
    usia_saat_ukur VARCHAR(50),
    tanggal_pengukuran DATE,
    berat FLOAT,
    tinggi FLOAT,
    bb_u VARCHAR(150),
    zs_bb_u FLOAT,
    tb_u VARCHAR(50),
    zs_tb_u FLOAT,
    bb_tb VARCHAR(50),
    zs_bb_tb FLOAT
);


-- Membuat Tabel Lokasi
CREATE TABLE lokasi (
    lokasi_id SERIAL PRIMARY KEY,
    prov VARCHAR(45),
    kab_kota VARCHAR(45),
    kec VARCHAR(55),
    pukesmas VARCHAR(45),
    desa_kel VARCHAR(45),
    posyandu VARCHAR(45));

-- Membuat Tabel Utama Balita
CREATE TABLE balita (
    balita_id SERIAL PRIMARY KEY,
    nik VARCHAR(16),
    nama VARCHAR(50),
    jk VARCHAR(1),
    tgl_lahir DATE,
    bb_lahir FLOAT,
    tb_lahir FLOAT,
    nama_ortu VARCHAR(50),
    lokasi_id INT REFERENCES lokasi(lokasi_id));

-- Membuat Tabel Utama Pengukuran
CREATE TABLE pengukuran (
    pengukuran_id SERIAL PRIMARY KEY,
    balita_id INT REFERENCES balita(balita_id),
    usia_saat_ukur VARCHAR(45),
    tanggal_pengukuran DATE,
    berat FLOAT,
    tinggi FLOAT,
    bb_u VARCHAR(45),
    zs_bb_u FLOAT,
    tb_u VARCHAR(45),
    zs_tb_u FLOAT,
    bb_tb VARCHAR(45),
    zs_bb_tb FLOAT
);

-- Memindahkan data lokasi ke tabel 'lokasi'
INSERT INTO lokasi (prov, kab_kota, kec, pukesmas, desa_kel, posyandu)
SELECT DISTINCT d.prov, d.kab_kota, d.kec, d.pukesmas, d.desa_kel, d.posyandu
FROM dataset d
LEFT JOIN lokasi l ON d.prov = l.prov 
                   AND d.kab_kota = l.kab_kota 
                   AND d.kec = l.kec 
                   AND d.pukesmas = l.pukesmas
                   AND d.desa_kel = l.desa_kel 
WHERE l.prov IS NULL 
      AND l.kab_kota IS NULL 
      AND l.kec IS NULL 
      AND l.pukesmas IS NULL
      AND l.desa_kel IS NULL ;

-- Memindahkan data balita ke tabel 'balita'                
INSERT INTO balita (nik, nama, jk, tgl_lahir, bb_lahir, tb_lahir, nama_ortu, lokasi_id)
SELECT DISTINCT
    d.nik, d.nama, d.jk, d.tgl_lahir, d.bb_lahir, d.tb_lahir, d.nama_ortu, 
    l.lokasi_id
FROM
    dataset d
JOIN
    lokasi l ON d.prov = l.prov AND d.kab_kota = l.kab_kota AND d.kec = l.kec AND d.pukesmas = l.pukesmas
              AND d.desa_kel = l.desa_kel AND d.posyandu = l.posyandu
LEFT JOIN
    balita b ON d.nik = b.nik
WHERE
    b.nik IS NULL;
             
-- Memindahkan data pengukuran ke tabel 'Pengukuran'
INSERT INTO pengukuran (balita_id, usia_saat_ukur, tanggal_pengukuran, berat, tinggi, bb_u, zs_bb_u, tb_u, zs_tb_u, bb_tb, zs_bb_tb)
SELECT distinct i.balita_id, 
       d.usia_saat_ukur, d.tanggal_pengukuran, d.berat, d.tinggi, d.bb_u, d.zs_bb_u, d.tb_u, d.zs_tb_u, d.bb_tb, d.zs_bb_tb
FROM dataset d
JOIN balita i ON d.nik = i.nik
LEFT JOIN pengukuran p ON p.balita_id = i.balita_id AND p.tanggal_pengukuran = d.tanggal_pengukuran
WHERE p.balita_id IS NULL;
