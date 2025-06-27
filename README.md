# Final Project : Big Data & Data Lakehouse

## Daftar Anggota

| No | Nama Lengkap                  | NRP         |
|----|-------------------------------|-------------|
| 1  | Aswalia Novitriasari          | 5025231012  |
| 2  | Agnes Zenobia Griselda P      | 5025231034  |
| 3  | Rafika Az Zahra Kusumastuti   | 5025231050  |
| 4  | Nisrina Atiqah Dwiputri R     | 5025231075  |

# Analisis Penerbangan: Prediksi Keterlambatan & Sentimen Pelanggan

## Deskripsi Masalah

Keterlambatan penerbangan adalah masalah kritis dalam industri aviasi yang berdampak pada finansial maskapai dan kepuasan pelanggan. Proyek ini bertujuan untuk membangun sebuah pipeline data end-to-end yang tidak hanya mampu memprediksi keterlambatan penerbangan menggunakan data terstruktur, tetapi juga menganalisis sentimen pelanggan dari ulasan teks tidak terstruktur. Dengan menggabungkan kedua analisis ini, pemangku kepentingan dapat memperoleh gambaran yang lebih holistik tentang performa operasional dan persepsi publik.

## Tujuan Proyek

- Membangun pipeline data streaming menggunakan **Apache Kafka** dan **MinIO** untuk menangani data terstruktur (detail penerbangan) dan tidak terstruktur (ulasan pelanggan).
- Mengimplementasikan proses ETL dan pelatihan model machine learning secara terdistribusi menggunakan **Apache Spark (PySpark)** untuk memprediksi durasi keterlambatan.
- Mengembangkan pipeline analisis sentimen untuk mengkategorikan ulasan pelanggan menjadi positif, negatif, atau netral.
- Menyajikan seluruh wawasan dalam sebuah dashboard interaktif yang dibangun dengan **Streamlit**.

## Dataset

- **Nama Dataset**: [2015 Flight Delays and Cancellations](https://www.kaggle.com/datasets/usdot/flight-delays?select=flights.csv)
- **Ukuran**: ~375 MB (`flights.csv` ~5,8 juta baris data)
- **Format**: CSV (Terstruktur)
- **Usability Score**: 10.0
- **Deskripsi**: Dataset ini berisi catatan penerbangan domestik di Amerika Serikat sepanjang tahun 2015. Data mencakup informasi detail seperti maskapai, nomor penerbangan, bandara asal dan tujuan, waktu keberangkatan dan kedatangan, durasi keterlambatan, jarak, dan lainnya.

### Mengapa Dataset Ini Cocok?

- **Skala Data Lebih Besar**: Ukuran 375 MB lebih cocok untuk mensimulasikan tantangan pipeline "Big Data" dibandingkan dataset prediksi sebelumnya, menguji kemampuan Kafka dan MinIO secara lebih efektif.
- **Masalah Regresi Klasik**: Memprediksi keterlambatan adalah kasus penggunaan yang relevan dan dipahami dengan baik dalam machine learning.
- **Kombinasi Fitur yang Kaya**: Dataset ini memiliki campuran fitur numerik, kategorikal, dan temporal yang ideal untuk melatih kemampuan feature engineering.

---

##  Arsitektur Solusi (Lakehouse Sederhana)

![arsi fp bigdata drawio](https://github.com/user-attachments/assets/4a06d7df-7d6c-41cd-b8fb-999944c996d4)

Pendekatan data lakehouse yang fleksibel sangat sesuai untuk menangani volume data penerbangan yang besar. Arsitektur ini memungkinkan kita untuk melakukan streaming, menyimpan data mentah, memprosesnya menjadi data terstruktur, dan melatih model dalam satu ekosistem yang terintegrasi.

## Komponen

| Komponen     | Deskripsi |
|--------------|-----------|
| **Apache Kafka** | Mensimulasikan aliran data penerbangan dan ulasan secara *real-time*. |
| **MinIO** | Berfungsi sebagai Data Lake untuk menyimpan data mentah (JSON Lines) dan data yang telah diproses (Delta Lake, Parquet).|
| **PySpark** | Digunakan untuk ETL dan pelatihan model prediksi keterlambatan secara terdistribusi, menunjukkan skalabilitas untuk data besar.|
| **Python** | Digunakan untuk data streaming (Kafka), analisis sentimen (VADER), dan sebagai bahasa utama proyek. |
| **Streamlit** | Membangun dashboard interaktif untuk menampilkan wawasan dari model PySpark dan hasil analisis sentimen. |
| **FastAPI** *(opsional)* | API ringan untuk menyajikan model secara programatis agar dapat digunakan oleh sistem lain. |

### Tech Stack

| Kategori              | Teknologi                                      |
|-----------------------|------------------------------------------------|
| **Containerization**  | Docker, Docker Compose                         |
| **Data Streaming**    | Apache Kafka                                   |
| **Data Lake Storage** | MinIO, Delta Lake                              |
| **Backend & ML**      | Apache Spark (PySpark), MLlib                  |
| **Library Python**    | Kafka-Python, MinIO, VaderSentiment            |
| **API Service**       | FastAPI, Uvicorn                               |
| **Dashboard**         | Streamlit                                      |
| **Frontend**          | HTML, CSS, JavaScript                          |

### Struktur Proyek
```
fp-bigdata
├── api_service/
│   └── main.py
├── dashboard/
│   └── app.py
├── data_pipeline/
│   ├── consumer.py
│   └── producer.py
├── data_source/
│   └── flights.csv
│   └── consumer_reviews.txt
├── frontend_ui/
│   ├── index.html
│   └── script.js
├── machine_learning/
│   ├── prepare_data_spark.py
│   └── train_model_spark.py
│   └── analyze_sentiment.py
├── .gitignore
├── docker-compose.yml
├── README.md
└── requirements.txt
```

---

## Langkah Pengerjaan
### Langkah 1: Persiapan Lingkungan Awal (Setup)
1. Buat File requirements.txt
   Buat file baru bernama requirements.txt di direktori utama proyek dan isi dengan semua library Python yang dibutuhkan oleh seluruh tim:
   Setelah file dibuat, instal semua library dengan menjalankan:
   ```
   pip install -r requirements.txt
   ```

2. Buat File docker-compose.yml
   Buat file baru bernama docker-compose.yml. File ini akan menjalankan infrastruktur yang Anda butuhkan (Kafka & MinIO).
 
### Langkah 2:  Menjalankan dan Mengkonfigurasi Infrastruktur
1. Jalankan Layanan Docker
   ```
   docker-compose up -d
   ```
   ![image](https://github.com/user-attachments/assets/4973b148-898a-447e-9af3-1eb7c3455b0e)

2. Konfigurasi MinIO (Data Lake)
   - Buka browser dan akses MinIO Web UI di http://localhost:9001.
   - Login dengan kredensial:
      - Username: minioadmin
      - Password: minioadmin
   - Penting: Buat dua bucket baru melalui UI. Anda akan menggunakan raw-data sebagai tujuan.
      - raw-data
      - processed-data
      - unstructured-raw-data

### Langkah 3: Mengembangkan Pipa Data
1. Lokasi dan Pembuatan File
   ```
   fp_bigdata/
      └── data_pipeline/      
          ├── producer.py    
          └── consumer.py
   ```
2. Kembangkan Producer (data_pipeline/producer.py)
3. Kembangkan Consumer (data_pipeline/consumer.py)

   ### Langkah 4: Eksekusi dan Validasi
   Jalankan pipeline
      - Di Terminal #1, jalankan Consumer terlebih dahulu. Dia harus siap mendengarkan.
        ```
        python data_pipeline/consumer.py
        ```

     ![Screenshot 2025-06-16 013533](https://github.com/user-attachments/assets/aeecb0f7-f47a-4b6c-af89-d173980a830a)
   
     ![Screenshot 2025-06-16 020739](https://github.com/user-attachments/assets/db4d3d58-9bd0-4240-8273-2a75e910f397)

       
      - Di Terminal #2, jalankan Producer untuk mulai mengirim data.
        ```
        python data_pipeline/producer.py
        ```
        ![Screenshot 2025-06-16 013314](https://github.com/user-attachments/assets/79730ee2-3b01-40d4-a137-d69454497091)

#### Menambahkan Variety Data
file dengan type `unstructured` format .txt kami tambahkan ke data_source untuk memastikan proses stream data dari producer-consumer berjalan dengan benar.

![Screenshot 2025-06-18 231044](https://github.com/user-attachments/assets/588e38e6-b72f-4a14-abf0-3e6031c97c6d)

![Screenshot 2025-06-18 232033](https://github.com/user-attachments/assets/a7cb0900-4d27-4abb-a248-124b5996730b)

![Screenshot 2025-06-18 232111](https://github.com/user-attachments/assets/a649a1ed-bd65-4692-b069-571b99defff5)


### Langkah 5-6: Memproses Data dan Melatih Model (Machine Learning)

Setelah data mentah berhasil dikumpulkan di bucket `raw-data` dan `unstructured-raw-data` MinIO, langkah selanjutnya adalah memprosesnya menjadi dataset bersih dan membangun model prediktif berbasis Machine Learning.

#### Struktur Direktori
```
fp-bigdata
├── api_service/
│   └── main.py
├── dashboard/
│   └── app.py
├── data_pipeline/
│   ├── consumer.py
│   └── producer.py
├── data_source/
│   └── flights.csv
│   └── consumer_reviews.txt
├── frontend_ui/
│   ├── index.html
│   └── script.js
├── machine_learning/
│   ├── prepare_data_spark.py (kita isi bagian ini)
│   └── train_model_spark.py  (dan ini)
│   └── analyze_sentiment.py  (dan ini)
```

### 1. Persiapan Data (prepare_data.py)

#### Lokasi:
`machine_learning/prepare_data_spark.py`

#### Fungsi:
- Mengambil semua file JSON dari bucket `raw-data`.
- Membersihkan data dan melakukan feature engineering.
- Menyimpan hasil ke dalam bucket `processed-data` sebagai file `cleaned_flight_data_delta`.

#### Jalankan Skrip:

```bash
python machine_learning/prepare_data_spark.py
```

![Screenshot 2025-06-20 150312](https://github.com/user-attachments/assets/7c699b17-b58c-4ba4-a6b6-75ac94252735)

### 2. Train Model (train_model_spark.py & analyze_sentiment.py)

#### Lokasi:
`machine_learning/train_model_spark.py`  `machine_learning/analyze_sentiment.py`

#### Jalankan Skrip:

```bash
python machine_learning/train_model_spark.py
```

![Screenshot 2025-06-20 150821](https://github.com/user-attachments/assets/3bc38971-cb5a-43fe-bcd2-c05489e4a12f)

```bash
python machine_learning/analyze_sentiment.py
```
![Screenshot 2025-06-20 115803](https://github.com/user-attachments/assets/79e80afe-aedb-43f9-86d2-3a9b8d8aaca1)


### 3. Validasi Hasil

#### Buka MinIO Web UI
Akses antarmuka pengguna MinIO melalui browser: `http://localhost:9001`

#### Navigasi ke Bucket `processed-data`
Pastikan tiga file berikut telah berhasil diunggah:

![Screenshot 2025-06-20 155237](https://github.com/user-attachments/assets/d95b33e2-b585-48cb-9ee7-3fb1e6be10aa)


#### _Notes_ :
- Pastikan `producer.py` dan `consumer.py` telah selesai dijalankan sehingga data mentah tersedia di bucket `raw-data`.
---


### Dashboard Streamlit

![Screenshot 2025-06-20 155641](https://github.com/user-attachments/assets/cdd2543e-f768-4575-bf20-7d3469a581c3)


![Screenshot 2025-06-20 155652](https://github.com/user-attachments/assets/306890c7-e8c7-4775-a6d6-1538befb27ff)


![Screenshot 2025-06-20 155709](https://github.com/user-attachments/assets/ad29876f-652d-4826-a05d-e8110ca10efb)


---
### Dashboard Frontend

![Screenshot 2025-06-27 095453](https://github.com/user-attachments/assets/fd29a4ee-f5a0-420e-8c41-eb76c4f11a44)

![Screenshot 2025-06-27 095505](https://github.com/user-attachments/assets/13389d6e-6b99-4c7f-8b47-282b66b00af4)



## 🚀 Menjalankan Proyek dengan 1 command

Proyek ini dirancang untuk dijalankan sepenuhnya menggunakan Docker Compose. Seluruh alur kerja, dari data ingestion hingga layanan frontend, akan berjalan secara otomatis.

### 1. Prasyarat

-   **Docker & Docker Compose:** Pastikan keduanya sudah ter-install dan Docker Desktop (atau Engine) sedang berjalan.
-   **Dataset:**
    1.  Unduh dataset dari **[Kaggle: US Flight Delays and Cancellations](https://www.kaggle.com/datasets/usdot/flight-delays)**.
    2.  Buat folder bernama `data_source` di direktori utama proyek.
    3.  Letakkan file `flights.csv` yang sudah diunduh ke dalam folder `data_source/`.

    > **Catatan:** Dataset tidak disertakan di repository ini karena ukurannya yang besar.

### 2. Jalankan Proyek

Buka terminal di direktori utama proyek, lalu jalankan satu perintah di bawah ini.
Perintah `--build` akan memastikan semua kode terbaru dan dependensi di-install dengan benar.

```bash
docker-compose up --build
```
#### DOKUMENTASI 
![image](https://github.com/user-attachments/assets/9dbbfb96-e772-4946-b332-ae8d96218d86)

![image](https://github.com/user-attachments/assets/ad828c3b-9a9e-427b-9bc7-87942ec29a10)


### 3. Akses Hasil Akhir

Setelah terminal menampilkan log bahwa layanan Uvicorn dan Streamlit berjalan, buka browser dan akses:

-   **Dashboard Analisis:**
    -  **`http://localhost:8501`**
      
![Screenshot 2025-06-20 155641](https://github.com/user-attachments/assets/5a7f82c1-2f7b-4df7-91cb-d05d2e25a883)

![Screenshot 2025-06-20 155709](https://github.com/user-attachments/assets/1b2d73de-c0ec-44c7-909e-55bc0786f554)


-   **UI Prediktor:**
    -  Buka file **`frontend_ui/index.html`**
    
![Screenshot 2025-06-27 095453](https://github.com/user-attachments/assets/a9f48cba-678b-4fe0-bf42-35ab57e6922d)



---

## 🛠️ Menghentikan Proyek

Untuk menghentikan semua layanan, buka terminal baru di direktori proyek dan jalankan:
```bash
docker-compose down
```
Untuk menghapus juga data yang ada di MinIO, gunakan:
```bash
docker-compose down -v
```
