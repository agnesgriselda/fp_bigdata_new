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
│   └── insurance.csv
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
