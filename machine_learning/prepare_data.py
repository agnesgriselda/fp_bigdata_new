# machine_learning/prepare_data.py (Versi Final Optimal)

import os
import io
import json
import pandas as pd
from minio import Minio
from minio.error import S3Error

# --- Konfigurasi ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
RAW_DATA_BUCKET = 'raw-data'
PROCESSED_DATA_BUCKET = 'processed-data'
# Mengubah nama output menjadi .parquet untuk efisiensi
OUTPUT_FILE_NAME = 'cleaned_flight_data.parquet'

def create_minio_client():
    """Membuat dan mengembalikan instance MinIO client."""
    print(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    try:
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        print("Successfully connected to MinIO.")
        return client
    except Exception as e:
        print(f"Failed to connect to MinIO: {e}")
        return None

# --- REVISI: Menggunakan fungsi setup yang sama seperti di consumer.py ---
def setup_minio_buckets(client):
    """Memastikan semua bucket yang dibutuhkan oleh proyek sudah ada."""
    buckets_to_create = [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET]
    for bucket_name in buckets_to_create:
        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' created successfully.")
            else:
                print(f"Bucket '{bucket_name}' already exists.")
        except S3Error as e:
            print(f"Error setting up bucket '{bucket_name}': {e}")
            return False
    return True

def main():
    """Fungsi utama untuk menjalankan pipeline ETL."""
    minio_client = create_minio_client()
    if not minio_client: return

    # Panggil setup bucket di awal untuk memastikan semuanya siap
    if not setup_minio_buckets(minio_client): return

    # 1. EXTRACT
    print(f"Listing objects in bucket '{RAW_DATA_BUCKET}'...")
    try:
        objects = list(minio_client.list_objects(RAW_DATA_BUCKET, recursive=True))
        if not objects:
            print("No data found in 'raw-data' bucket. Exiting ETL process.")
            return
            
        data_list = []
        for obj in objects:
            response = minio_client.get_object(RAW_DATA_BUCKET, obj.object_name)
            content = response.read().decode('utf-8')
            for line in content.strip().split('\n'):
                if line: data_list.append(json.loads(line))
        
        df = pd.DataFrame(data_list)
        print(f"Successfully loaded {len(df)} records into a DataFrame.")

    except S3Error as e:
        print(f"Error reading from MinIO: {e}"); return

    # 2. TRANSFORM
    print("Starting data transformation...")
    df.dropna(subset=['ARRIVAL_DELAY'], inplace=True)
    df['DEPARTURE_DELAY'].fillna(0, inplace=True)
    df = pd.get_dummies(df, columns=['AIRLINE'], prefix='airline', drop_first=False)
    df.drop(['FLIGHT_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'], axis=1, inplace=True, errors='ignore')
    df.columns = df.columns.str.lower()
    print("Data transformation complete.")
    
    # 3. LOAD (sebagai Parquet)
    print(f"Saving as Parquet to '{PROCESSED_DATA_BUCKET}/{OUTPUT_FILE_NAME}'...")
    try:
        with io.BytesIO() as buffer:
            # Gunakan to_parquet dengan engine pyarrow
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            minio_client.put_object(
                PROCESSED_DATA_BUCKET,
                OUTPUT_FILE_NAME,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
        print("Successfully uploaded Parquet file.")
    except Exception as e:
        print(f"Error uploading Parquet to MinIO: {e}")

if __name__ == "__main__":
    main()