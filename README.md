# Final Project : Big Data & Data Lakehouse

## Daftar Anggota

| No | Nama Lengkap                  | NRP         |
|----|-------------------------------|-------------|
| 1  | Aswalia Novitriasari          | 5025231012  |
| 2  | Agnes Zenobia Griselda P      | 5025231034  |
| 3  | Rafika Az Zahra Kusumastuti   | 5025231050  |
| 4  | Nisrina Atiqah Dwiputri R     | 5025231075  |

# Prediksi Keterlambatan Penerbangan dengan Machine Learning

## Deskripsi Masalah
Keterlambatan penerbangan adalah masalah kritis dalam industri aviasi yang menyebabkan kerugian finansial bagi maskapai, menurunkan tingkat kepuasan pelanggan, dan mengganggu efisiensi operasional bandara. Kemampuan untuk memprediksi potensi keterlambatan secara akurat dapat membantu para pemangku kepentingan untuk mengambil tindakan mitigasi, seperti penyesuaian jadwal, alokasi sumber daya yang lebih baik, dan komunikasi proaktif kepada penumpang.

Dengan memanfaatkan data historis penerbangan yang masif, kita dapat membangun model prediktif untuk mengidentifikasi pola dan faktor-faktor utama yang berkontribusi terhadap keterlambatan.

## Tujuan Proyek

Membangun model regresi prediktif menggunakan dataset `flights.csv` dari Kaggle untuk:
- Memprediksi durasi keterlambatan kedatangan (ARRIVAL_DELAY) sebuah penerbangan dalam satuan menit.
- Menganalisis kontribusi variabel seperti keterlambatan keberangkatan, jarak tempuh, maskapai, dan waktu penerbangan terhadap potensi keterlambatan.
- Menyajikan hasil analisis dalam bentuk dashboard interaktif yang dapat diakses oleh pengambil keputusan di industri penerbangan.

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
| **Apache Kafka** | Mensimulasikan aliran data penerbangan secara real-time, seolah-olah data diterima langsung dari sistem bandara. |
| **MinIO** | Berfungsi sebagai Data Lake untuk menyimpan data mentah (JSON), data bersih (CSV), dan model terlatih (.pkl). |
| **Python** | Digunakan untuk seluruh pipeline: Kafka producer & consumer, ETL, dan pelatihan model regresi dengan `pandas` dan `scikit-learn`. |
| **Streamlit** | Dashboard interaktif untuk prediksi keterlambatan dan visualisasi faktor penyebabnya. |
| **FastAPI** *(opsional)* | API ringan untuk menyajikan model secara programatis agar dapat digunakan oleh sistem lain. |

## Alur Kerja

1. **Ingest Data**
   - File `producer.py` membaca `flights.csv` baris demi baris.
   - Data dikirim ke *topic* Kafka bernama `flight-data`.

2. **Store Raw Data**
   - File `consumer.py` menerima data dari Kafka.
   - Setiap data disimpan sebagai file JSON individual di bucket `raw-data` MinIO.

3. **Preprocessing (ETL)**
   - File `prepare_data.py` membaca seluruh JSON dari bucket `raw-data`.
   - Melakukan:
     - Pembersihan data
     - Feature engineering
   - Menyimpan hasil bersih sebagai `cleaned_flight_data.csv` di bucket `processed-data`.

4. **Modeling**
   - File `train_model.py` membaca data CSV yang sudah dibersihkan.
   - Melatih model regresi untuk memprediksi `ARRIVAL_DELAY`.
   - Menyimpan model sebagai file `.pkl` ke MinIO.

5. **Result Storage**
   - Model (`flight_delay_model.pkl`) disimpan di bucket `processed-data`.

6. **Visualization**
   - Dashboard di `app.py` (Streamlit) memuat model dari MinIO.
   - Pengguna dapat:
     - Memasukkan detail penerbangan
     - Melihat prediksi keterlambatan
     - Melihat visualisasi faktor-faktor keterlambatan

### Tech Stack

| Kategori              | Teknologi                                      |
|-----------------------|------------------------------------------------|
| **Containerization**  | Docker, Docker Compose                         |
| **Data Streaming**    | Apache Kafka                                   |
| **Data Lake Storage** | MinIO                                          |
| **Backend & ML**      | Python                                         |
| **Library Python**    | Pandas, Scikit-learn, Kafka-Python, MinIO      |
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
│   └── flight.csv
├── frontend_ui/
│   ├── index.html
│   └── script.js
├── machine_learning/
│   ├── prepare_data.py
│   └── train_model.py
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
   ```
   # Core Libraries untuk Data Processing & ML
   pandas
   scikit-learn

   # Untuk Data Pipeline 
   kafka-python
   minio

   # Untuk API Service 
   fastapi
   uvicorn[standard]

   # Untuk Dashboard
   streamlit
   plotly
   ```
   Setelah file dibuat, instal semua library dengan menjalankan:
   ```
   pip install -r requirements.txt
   ```

2. Buat File docker-compose.yml
   Buat file baru bernama docker-compose.yml. File ini akan menjalankan infrastruktur yang Anda butuhkan (Kafka & MinIO).
   ```
   version: '3.8'

   services:
     # Zookeeper adalah prasyarat wajib untuk Kafka.
     # Kafka menggunakannya untuk mengelola konfigurasi, sinkronisasi, dan state cluster.
     zookeeper:
       image: confluentinc/cp-zookeeper:7.0.1
       container_name: zookeeper
       hostname: zookeeper
       ports:
         - "2181:2181"
       environment:
         ZOOKEEPER_CLIENT_PORT: 2181
         ZOOKEEPER_TICK_TIME: 2000

     # Kafka adalah platform streaming yang akan menerima data dari producer.
     kafka:
       image: confluentinc/cp-kafka:7.0.1
       container_name: kafka
       hostname: kafka
       ports:
         # Port 9092 diekspos ke host agar skrip Python (producer/consumer) bisa terhubung.
         - "9092:9092"
       depends_on:
         - zookeeper
       environment:
         KAFKA_BROKER_ID: 1
         KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
         # Konfigurasi listener ini krusial agar Kafka bisa diakses dari luar container (localhost).
         KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
         KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
         KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
         KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

     # MinIO berfungsi sebagai Data Lake untuk menyimpan data mentah, data olahan, dan model.
     minio:
       image: minio/minio:RELEASE.2023-09-07T02-05-02Z
       container_name: minio
       hostname: minio
       ports:
         # Port 9000 untuk API access (digunakan oleh skrip Python).
         - "9000:9000"
         # Port 9001 untuk Web UI (diakses via browser).
         - "9001:9001"
       volumes:
         # 'minio_data' adalah volume yang akan menyimpan data secara permanen.
         # Jika container dihapus, data di volume ini tidak akan hilang.
         - minio_data:/data
       environment:
         # Kredensial untuk login ke MinIO. JANGAN gunakan ini di lingkungan produksi.
         MINIO_ROOT_USER: minioadmin
         MINIO_ROOT_PASSWORD: minioadmin
       command: server /data --console-address ":9001"

   # Mendefinisikan volume yang akan digunakan oleh layanan MinIO.
   volumes:
     minio_data:
   ```

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

Setelah data mentah berhasil dikumpulkan di bucket `raw-data` MinIO, langkah selanjutnya adalah memprosesnya menjadi dataset bersih dan membangun model prediktif berbasis Machine Learning.

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
│   └── insurance.csv
├── frontend_ui/
│   ├── index.html
│   └── script.js
├── machine_learning/
│   ├── prepare_data.py (kita isi bagian ini)
│   └── train_model.py  (dan ini)
```

### 1. Persiapan Data (prepare_data.py)

#### Lokasi:
`machine_learning/prepare_data.py`

#### Fungsi:
- Mengambil semua file JSON dari bucket `raw-data`.
- Membersihkan data dan melakukan feature engineering.
- Menyimpan hasil ke dalam bucket `processed-data` sebagai file `cleaned_insurance_data.csv`.

#### Jalankan Skrip:

```bash
python machine_learning/prepare_data.py
```
![Screenshot 2025-06-16 030520](https://github.com/user-attachments/assets/94ab28cc-5d56-4a52-82c5-aa2f5485ee58)

![Screenshot 2025-06-16 030540](https://github.com/user-attachments/assets/918e6571-12fd-4521-88e0-58d2043e0e7c)


### 2. Train Model (train_model.py)

#### Lokasi:
`machine_learning/train_model.py`

#### Fungsi:
- Mengambil `cleaned_insurance_data.csv` dari bucket processed-data.
- Melatih model _RandomForestRegressor_ untuk memprediksi charges.
- Menyimpan model hasil pelatihan `(insurance_model.pkl)` ke `bucket processed-data`.

#### Jalankan Skrip:

```bash
python machine_learning/train_model.py
```
### 3. Validasi Hasil

#### Buka MinIO Web UI
Akses antarmuka pengguna MinIO melalui browser: `http://localhost:9001`

#### Navigasi ke Bucket `processed-data`
Pastikan dua file berikut telah berhasil diunggah:

- **`cleaned_insurance_data.csv`** — hasil *data cleaning* dan *feature engineering*
- **`insurance_model.pkl`** — model Machine Learning yang telah dilatih dan disimpan dalam format pickle

#### Dokumentasi Tampilan:
![Screenshot 2025-06-16 073632](https://github.com/user-attachments/assets/1244d176-b1e6-43af-9c43-93bed693c546)

#### _Notes_ :
- Pastikan `producer.py` dan `consumer.py` telah selesai dijalankan sehingga data mentah tersedia di bucket `raw-data`.
- Skrip `prepare_data.py` dan `train_model.py` menggunakan:
  - **MinIO SDK for Python** untuk komunikasi ke object storage
  - **pandas** untuk manipulasi data
  - **scikit-learn** untuk pelatihan dan evaluasi model
  - **pickle** untuk serialisasi model
- Proses transformasi data mencakup:
  - *Handling missing values* (menghapus baris kosong)
  - *Encoding* kolom kategorikal
  - Normalisasi nama kolom agar konsisten

#### Evaluasi Model
![Screenshot 2025-06-16 072711](https://github.com/user-attachments/assets/22183e8a-50a4-4f60-a3d0-bc18272f2832)

---

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
      
![image](https://github.com/user-attachments/assets/bac660a2-56cf-42d1-a978-ef108c2ae881)


-   **UI Prediktor:**
    -  Buka file **`frontend_ui/index.html`**
    
![image](https://github.com/user-attachments/assets/7710ef50-99ec-440b-9611-c10ac2c9b17a)


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
