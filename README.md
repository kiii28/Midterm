# Analisis Big Data NYC Yellow Taxi (Desember 2025) dengan PySpark

**Nama:** Muhammad Rifky Alfajri · **NIM:** 103012430036 · **Kelas:** BS1 IF 48 GAB01
**Mata kuliah:** Big Data and Artificial Intelligence, Midterm Project · S1 Informatika, Telkom University

Proyek individu. Seluruh pekerjaan (analisis, kode, dan laporan) dikerjakan oleh satu orang.

## Ringkasan

Analisis satu bulan data perjalanan NYC Yellow Taxi (Desember 2025, 4.305.006 baris, 20 kolom) dengan PySpark DataFrame API, Spark SQL, dan MLlib. Pertanyaan utama: bagaimana struktur tarif, perilaku tip, dan pola perjalanan jauh, dan seberapa akurat tarif argo standar dapat diprediksi dibanding baseline sederhana. Lima pertanyaan turunan (Q1-Q5) beserta jawabannya ada di notebook bagian 6-7 dan 9, serta di laporan PDF.

## Sumber data dan cara memperolehnya

| Berkas | Sumber | Keterangan |
|---|---|---|
| `yellow_tripdata_2025-12.parquet` | NYC Taxi & Limousine Commission (TLC) Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page | Pilih "Yellow Taxi Trip Records", bulan Desember 2025, format Parquet. Ukuran 73,7 MB. |
| `taxi_zone_lookup.csv` | Halaman TLC yang sama (bagian "Taxi Zone Lookup Table") | Pemetaan ID zona ke nama zona dan borough (265 baris). |

- **Lisensi/ketentuan:** data terbuka NYC (NYC Open Data), tersedia gratis untuk dipakai publik tanpa jaminan kelengkapan atau kecocokan tujuan tertentu. Baca ketentuan lengkapnya di portal NYC Open Data sebelum memakai ulang.
- **Tanggal akses:** 1 November 2026. TLC dapat memperbarui berkas bulanan; angka di notebook dan laporan berasal dari salinan yang diunduh pada tanggal itu.
- Data tidak memuat identitas pribadi. Dataset penuh tidak disertakan dalam pengumpulan.

## Lingkungan yang dipakai

| Komponen | Versi / nilai |
|---|---|
| Python | 3.13.1 |
| PySpark | 4.2.0 (Spark runtime 4.2.0) |
| Sistem operasi | Windows 11 (build 10.0.26340), 16 core CPU |
| `spark.driver.memory` | 4g |
| `spark.sql.shuffle.partitions` | 8 |
| Pustaka lain | pandas, matplotlib, seaborn, Jupyter (versi mengikuti lingkungan; tidak dikunci) |
| Seed | 42 (`randomSplit`, `sample`, Random Forest) |

Spark membutuhkan Java (JDK) terpasang dan terdeteksi (`JAVA_HOME`); gunakan versi yang didukung oleh versi PySpark Anda. Pada Windows, konfigurasi Hadoop dapat diperlukan sesuai lingkungan masing-masing.

## Struktur folder

```
folder_proyek/
├── Proyek_MidTerm.ipynb                      # notebook utama (ganti nama bila perlu)
├── README.md
├── Laporan_Midterm_NYC_Taxi.pdf               # laporan
├── yellow_tripdata_2025-12.parquet            # DIUNDUH (tidak disertakan)
├── taxi_zone_lookup.csv                       # DIUNDUH (tidak disertakan)
└── yellow_tripdata_2025-12_clean.parquet/     # DIBUAT notebook (bagian 5.5)
```

Jalur berkas diatur di sel konfigurasi (bagian 3): `RAW_PATH`, `ZONE_PATH`, `CLEAN_PATH`. Ubah di sana jika letak berkas berbeda.

## Cara menjalankan

1. Pasang dependensi: `pip install pyspark pandas matplotlib seaborn jupyter` (pastikan Java terpasang).
2. Unduh kedua berkas data (tabel di atas) dan letakkan di folder yang sama dengan notebook.
3. Buka notebook dengan Jupyter, pilih **Kernel > Restart & Run All**. Notebook dirancang berurutan dari atas ke bawah dan harus dijalankan penuh sekali, karena sel-sel akhir memakai variabel dari sel sebelumnya (mis. `df`, `df_std`, `tip_pdf`, `models`, `imp_pdf`, `corr_pdf`).
4. Bagian 5.5 menulis zona clean (`yellow_tripdata_2025-12_clean.parquet`) lalu membacanya kembali dan men-cache-nya. Bagian 7 melatih 12 model (3 cakupan data x 4 kombinasi model dan fitur) ditambah 6 Random Forest untuk drop-column importance; ini langkah paling lama.
5. Jika `taxi_zone_lookup.csv` tidak ditemukan, notebook tetap berjalan dengan label `Zone <id>`, tetapi tabel tujuan dan asal tidak akan berisi nama zona.

## Struktur notebook

| Bagian | Isi |
|---|---|
| 1 | Rumusan masalah (Q1-Q5), pengguna, penilaian 5V |
| 2 | Sumber data, pilihan penyimpanan, data dictionary, diagram pipeline |
| 3 | Setup, konfigurasi, ingestion (schema, jumlah baris, partisi, sampel) |
| 4 | Profiling data mentah, investigasi NULL struktural, dampak pada tarif |
| 5 | Cleaning, deduplikasi, feature engineering, 9 quality checks, penyimpanan zona clean |
| 6 | Analisis: Q1 (tip), Q2 (perjalanan jauh), pengukuran waktu, Q5 (`extra`, `cbd_congestion_fee`) |
| 7 | Pemodelan fare (Q3): baseline, regresi linear, Random Forest, drop-column importance |
| 8 | Visualisasi G1-G8 dengan interpretasi |
| 9 | Temuan, keterbatasan, rekomendasi |

## Parameter yang dapat diubah

| Parameter | Nilai | Pengaruh |
|---|---|---|
| `ML_FRACTION` | 0.5 | Fraksi data untuk pemodelan; 1.0 butuh memori driver lebih dari 4 GB dan mengubah angka bagian 7 |
| `IMP_FRACTION` | 0.3 | Fraksi data latih untuk drop-column importance |
| `MIN_TRIPS_SLOT` | 500 | Minimum trip per slot hari-jam pada Q1 |
| `SEED` | 42 | Seed untuk sampling, split, dan Random Forest |

## Keterbatasan reproduksi

- **Hasil dapat sedikit berbeda antar mesin.** `sample` dan `randomSplit` di Spark bergantung pada jumlah partisi dan inti CPU, dan `percentile_approx` bersifat aproksimasi. Angka bagian 7 dan persentil dapat bergeser tipis walaupun seed sama.
- **Waktu eksekusi bergantung pada perangkat keras**, cache sistem operasi, dan proses lain; tabel pengukuran waktu bersifat indikatif (3 ulangan, satu mesin).
- **Versi data.** TLC dapat memperbarui berkas bulanan setelah tanggal akses sehingga jumlah baris dapat berbeda.
- **Memori.** Dengan driver 4 GB, notebook berjalan pada `ML_FRACTION = 0.5`; mesin dengan memori lebih kecil perlu menurunkan nilai ini.
- **Angka di sel markdown** (interpretasi) ditulis dari output eksekusi pertama. Setelah menjalankan ulang, cocokkan dengan output terbaru.

## Penggunaan AI generatif

Claude (Anthropic) dipakai sebagai pendamping untuk: merevisi teks notebook (menghapus paragraf berulang dan merapikan keterangan), menambah sel diagram arsitektur, pengukuran waktu, visualisasi G2-G8, dan bagian 9, serta menyusun README. Kode dan kesimpulan diverifikasi penulis dengan menjalankan notebook dan mencocokkannya dengan output.

## Kontribusi

Proyek Kelompok: Muhammad Rifky Alfajri mengerjakan seluruh bagian.
