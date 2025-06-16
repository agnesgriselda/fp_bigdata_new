import os
import io
import json
import pandas as pd
from minio import Minio
from minio.error import S3Error

# --- Konfigurasi MinIO ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
RAW_DATA_BUCKET = 'raw-data'
PROCESSED_DATA_BUCKET = 'processed-data'
OUTPUT_CSV_FILE = 'cleaned_flight_data.csv'

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

def main():
    """Fungsi utama untuk menjalankan pipeline ETL."""
    minio_client = create_minio_client()
    if not minio_client:
        return

    # 1. EXTRACT: Baca semua file .jsonl dari bucket 'raw-data'
    print(f"Listing objects in bucket '{RAW_DATA_BUCKET}'...")
    try:
        objects = minio_client.list_objects(RAW_DATA_BUCKET, recursive=True)
        data_list = []
        
        for obj in objects:
            print(f"Reading file: {obj.object_name}")
            response = minio_client.get_object(RAW_DATA_BUCKET, obj.object_name)
            
            # Membaca konten file sebagai string
            content = response.read().decode('utf-8')
            
            # Setiap baris dalam file adalah objek JSON (format JSON Lines)
            for line in content.splitlines():
                if line: # Pastikan baris tidak kosong
                    data_list.append(json.loads(line))
        
        if not data_list:
            print("No data found in 'raw-data' bucket. Exiting.")
            return

        # Buat DataFrame dari semua data yang terkumpul
        df = pd.DataFrame(data_list)
        print(f"Successfully loaded {len(df)} records into a DataFrame.")

    except S3Error as e:
        print(f"Error reading from MinIO: {e}")
        return

    # 2. TRANSFORM: Bersihkan data dan lakukan feature engineering
    print("Starting data transformation for Flight Delays dataset...")
    
    # Hapus baris di mana target (arrival_delay) tidak ada nilainya
    df.dropna(subset=['ARRIVAL_DELAY'], inplace=True)
    
    # Mengisi nilai kosong di departure_delay dengan 0 (asumsi tidak terlambat)
    df['DEPARTURE_DELAY'].fillna(0, inplace=True)

    # Feature Engineering: One-Hot Encoding untuk kolom 'AIRLINE'
    # Ini sama persis dengan yang Anda lakukan pada kolom 'region' sebelumnya
    print("Performing one-hot encoding on 'AIRLINE' column...")
    df = pd.get_dummies(df, columns=['AIRLINE'], prefix='airline', drop_first=True)

    # Menghapus kolom yang tidak berguna untuk prediksi
    # Airport ID akan menghasilkan terlalu banyak kolom (curse of dimensionality)
    # Flight Number adalah identifier unik, tidak memiliki pola prediktif
    print("Dropping identifier columns: FLIGHT_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT...")
    df.drop(['FLIGHT_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'], axis=1, inplace=True)
    
    # Mengubah semua nama kolom menjadi huruf kecil untuk konsistensi
    df.columns = df.columns.str.lower()
    
    print("Data transformation complete. Final columns:")
    print(df.columns)

    # 3. LOAD: Simpan DataFrame yang sudah bersih ke bucket 'processed-data'
    # Pastikan bucket tujuan ada
    try:
        if not minio_client.bucket_exists(PROCESSED_DATA_BUCKET):
            minio_client.make_bucket(PROCESSED_DATA_BUCKET)
            print(f"Bucket '{PROCESSED_DATA_BUCKET}' created.")
    except S3Error as e:
        print(f"Error creating bucket: {e}")
        return

    # Konversi DataFrame ke format CSV dalam bentuk byte
    print("Converting DataFrame to CSV format...")
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    csv_buffer = io.BytesIO(csv_bytes)
    
    print(f"Uploading '{OUTPUT_CSV_FILE}' to bucket '{PROCESSED_DATA_BUCKET}'...")

    try:
        minio_client.put_object(
            PROCESSED_DATA_BUCKET,
            OUTPUT_CSV_FILE,
            data=csv_buffer,
            length=len(csv_bytes),
            content_type='application/csv'
        )
        print(f"Successfully uploaded '{OUTPUT_CSV_FILE}' to MinIO.")
    except S3Error as e:
        print(f"Error uploading to MinIO: {e}")

if __name__ == "__main__":
    main()