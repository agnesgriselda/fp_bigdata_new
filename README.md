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
- **Ukuran**: ~375 MB
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
3. Kembangkan Producer (data_pipeline/producer.py)
   ```
   import time
   import csv
   import json
   from kafka import KafkaProducer
   import os

   # Konfigurasi yang disesuaikan untuk dataset penerbangan
   KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
   KAFKA_TOPIC = 'flight-data'  # <-- DIUBAH
   CSV_FILE_PATH = 'data_source/flights.csv'  # <-- DIUBAH
   SIMULATION_DELAY_SECONDS = 0.01  # <-- DIUBAH (agar lebih cepat)

   def create_producer():
       """Membuat dan mengembalikan instance KafkaProducer."""
       print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
       try:
           producer = KafkaProducer(
               bootstrap_servers=[KAFKA_BROKER],
               value_serializer=lambda v: json.dumps(v).encode('utf-8'),
               api_version=(0, 10, 2)
           )
           print("Successfully connected to Kafka Broker.")
           return producer
       except Exception as e:
           print(f"Failed to connect to Kafka Broker: {e}")
           time.sleep(5)
           return create_producer()

   def publish_data(producer, topic):
       """Membaca data dari flights.csv dan mengirimkannya ke Kafka."""
       print(f"Reading data from {CSV_FILE_PATH} and publishing to topic '{topic}'...")
 
       try:
           with open(CSV_FILE_PATH, mode='r') as file:
               # Menggunakan DictReader untuk kemudahan akses kolom berdasarkan nama
               csv_reader = csv.DictReader(file)
            
               print("Starting to send flight data...")
               for i, row in enumerate(csv_reader):
                   try:
                       message = {
                           "YEAR": int(row.get("YEAR", 0)),
                           "MONTH": int(row.get("MONTH", 0)),
                           "DAY": int(row.get("DAY", 0)),
                           "AIRLINE": row.get("AIRLINE", ""),
                           "FLIGHT_NUMBER": int(row.get("FLIGHT_NUMBER", 0)),
                           "ORIGIN_AIRPORT": row.get("ORIGIN_AIRPORT", ""),
                           "DESTINATION_AIRPORT": row.get("DESTINATION_AIRPORT", ""),
                           "DEPARTURE_DELAY": float(row.get("DEPARTURE_DELAY") or 0),
                           "ARRIVAL_DELAY": float(row.get("ARRIVAL_DELAY") or 0),
                           "DISTANCE": int(row.get("DISTANCE", 0))
                       }
                 
                       producer.send(topic, value=message)
                    
                       # Print status setiap 1000 data agar tidak membanjiri terminal
                       if (i + 1) % 1000 == 0:
                           print(f"Sent {i + 1} records...")
                 
                       time.sleep(SIMULATION_DELAY_SECONDS)
                 
                   except (ValueError, KeyError) as e:
                       print(f"Skipping row due to data error: {row}. Error: {e}")
                       continue
       except FileNotFoundError:
           print(f"Error: File not found at {CSV_FILE_PATH}")
           return
 
       producer.flush()
       print("All data has been sent.")

   if __name__ == "__main__":
       kafka_producer = create_producer()
       if kafka_producer:
           publish_data(kafka_producer, KAFKA_TOPIC)
           kafka_producer.close()
           print("Producer has been closed.")
   ```
   
3. Kembangkan Consumer (data_pipeline/consumer.py)
   ```
   import json
   import os
   import time
   from kafka import KafkaConsumer
   from minio import Minio
   from minio.error import S3Error
   import io

   # Konfigurasi Kafka
   KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
   KAFKA_TOPIC = 'flight-data'  # <-- DIUBAH

   # Konfigurasi MinIO (Port 9000 untuk API, 9001 untuk Web UI)
   MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
   MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
   MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
   MINIO_BUCKET_NAME = 'raw-data'

   def create_kafka_consumer():
       """Membuat dan mengembalikan instance KafkaConsumer."""
       print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
       try:
           consumer = KafkaConsumer(
               KAFKA_TOPIC,
               bootstrap_servers=[KAFKA_BROKER],
               auto_offset_reset='earliest',
               group_id='flight-data-consumer-group', # <-- Group ID diubah agar sesuai konteks
               value_deserializer=lambda x: json.loads(x.decode('utf-8')),
               api_version=(0, 10, 2)
           )
           print("Successfully connected to Kafka Broker.")
           return consumer
       except Exception as e:
           print(f"Failed to connect to Kafka: {e}")
           time.sleep(5)
           return create_kafka_consumer()

   def create_minio_client():
       """Membuat dan mengembalikan instance MinIO client."""
       print(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
       try:
           client = Minio(
               MINIO_ENDPOINT,
               access_key=MINIO_ACCESS_KEY,
               secret_key=MINIO_SECRET_KEY,
               secure=False
           )
           print("Successfully connected to MinIO.")
           return client
       except Exception as e:
           print(f"Failed to connect to MinIO: {e}")
           return None

   def setup_minio_bucket(client, bucket_name):
       """Memastikan bucket di MinIO sudah ada, jika tidak, maka dibuat."""
       try:
           found = client.bucket_exists(bucket_name)
           if not found:
               client.make_bucket(bucket_name)
               print(f"Bucket '{bucket_name}' created.")
           else:
               print(f"Bucket '{bucket_name}' already exists.")
       except S3Error as e:
           print(f"Error checking or creating bucket: {e}")
           return False
       return True

   def consume_and_store_data():
       """Mengkonsumsi data dari Kafka dan menyimpannya ke MinIO."""
       consumer = create_kafka_consumer()
       minio_client = create_minio_client()

       if not consumer or not minio_client:
           print("Could not initialize Kafka Consumer or MinIO Client. Exiting.")
           return

       if not setup_minio_bucket(minio_client, MINIO_BUCKET_NAME):
           print(f"Failed to setup MinIO bucket '{MINIO_BUCKET_NAME}'. Exiting.")
           return

       print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")
       try:
           for message in consumer:
               data = message.value
               print(f"Received flight data for flight: {data.get('FLIGHT_NUMBER')}")

               timestamp_ms = int(time.time() * 1000)
               flight_number = data.get('FLIGHT_NUMBER', 'UNKNOWN')
               file_name = f"flight_{flight_number}_{timestamp_ms}.json"

               json_data = json.dumps(data, indent=4).encode('utf-8')
               json_stream = io.BytesIO(json_data)

               try:
                   minio_client.put_object(
                       MINIO_BUCKET_NAME,
                       file_name,
                       json_stream,
                       len(json_data),
                       content_type='application/json'
                   )
                   print(f"Successfully uploaded {file_name} to bucket '{MINIO_BUCKET_NAME}'")
               except S3Error as e:
                   print(f"Error uploading to MinIO: {e}")

       except KeyboardInterrupt:
           print("Stopping consumer...")
       finally:
           consumer.close()
           print("Consumer has been closed.")

   if __name__ == "__main__":
       consume_and_store_data()
   ```

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
#![Screenshot 2025-06-16 073632](https://github.com/user-attachments/assets/1244d176-b1e6-43af-9c43-93bed693c546)

#### _Notes_ :
- Pastikan `producer.py` dan `consumer.py` telah selesai dijalankan sehingga data mentah tersedia di bucket `raw-data`.
- Skrip `prepare_data.py` dan `train_model.py` menggunakan:
  - **MinIO SDK for Python** untuk komunikasi ke object storage
  - **pandas** untuk manipulasi data
  - **scikit-learn** untuk pelatihan dan evaluasi model
  - **pickle** untuk serialisasi model
- Proses transformasi data mencakup:
  - *Handling missing values* (menghapus baris kosong)
  - *Encoding* kolom kategorikal (`sex`, `smoker`, `region`)
  - Normalisasi nama kolom agar konsisten

#### Evaluasi Model
![Screenshot 2025-06-16 072711](https://github.com/user-attachments/assets/22183e8a-50a4-4f60-a3d0-bc18272f2832)

---

