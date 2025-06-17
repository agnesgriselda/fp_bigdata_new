# data_pipeline/consumer.py (Versi Final dengan Batching & Auto Create Bucket)

import json
import os
import time
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import io

# --- Konfigurasi ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = 'flight-data'
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Definisikan kedua nama bucket yang dibutuhkan oleh proyek
RAW_BUCKET_NAME = 'raw-data'
PROCESSED_BUCKET_NAME = 'processed-data'

# Ukuran batch untuk disimpan ke MinIO
BATCH_SIZE = 10000

def create_kafka_consumer():
    """Membuat dan mengembalikan instance KafkaConsumer yang sudah di-tuning."""
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            group_id='flight-data-consumer-group-batched',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(0, 10, 2),
            consumer_timeout_ms=10000, # Tambahan: Berhenti jika tidak ada pesan selama 10 detik
            fetch_max_bytes=15728640, # 15MB
            max_poll_records=500,
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

# --- REVISI UTAMA: FUNGSI UNTUK MEMBUAT BUCKET OTOMATIS ---
def setup_minio_buckets(client):
    """Memastikan semua bucket yang dibutuhkan oleh proyek sudah ada."""
    buckets_to_create = [RAW_BUCKET_NAME, PROCESSED_BUCKET_NAME]
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

def consume_and_store_data():
    """Mengkonsumsi data dari Kafka dan menyimpannya ke MinIO dalam batch."""
    consumer = create_kafka_consumer()
    minio_client = create_minio_client()

    if not consumer or not minio_client:
        return

    # Panggil fungsi setup bucket sebelum memulai proses
    if not setup_minio_buckets(minio_client):
        print("Failed to setup MinIO buckets. Exiting.")
        return

    print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")
    print(f"Will store data in batches of {BATCH_SIZE} records to bucket '{RAW_BUCKET_NAME}'.")
    
    record_batch = []
    total_records_processed = 0
    start_time = time.time()

    try:
        for message in consumer:
            record_batch.append(message.value)
            
            if len(record_batch) >= BATCH_SIZE:
                batch_file_name = f"batch_{int(time.time() * 1000)}.jsonl"
                batch_data_str = '\n'.join(json.dumps(record) for record in record_batch)
                data_stream = io.BytesIO(batch_data_str.encode('utf-8'))
                
                try:
                    minio_client.put_object(
                        RAW_BUCKET_NAME,
                        batch_file_name,
                        data_stream,
                        len(batch_data_str),
                        content_type='application/x-json-stream'
                    )
                    
                    processed_count = len(record_batch)
                    total_records_processed += processed_count
                    elapsed_time = time.time() - start_time
                    print(f"Uploaded batch '{batch_file_name}'. Processed {total_records_processed} records in {elapsed_time:.2f}s.")

                    record_batch = []
                    
                except S3Error as e:
                    print(f"Error uploading batch to MinIO: {e}")

    except KeyboardInterrupt:
        print("\nStopping consumer by user request...")
    finally:
        if record_batch:
            print(f"Uploading final batch of {len(record_batch)} records...")
            batch_file_name = f"batch_{int(time.time() * 1000)}_final.jsonl"
            batch_data_str = '\n'.join(json.dumps(record) for record in record_batch)
            data_stream = io.BytesIO(batch_data_str.encode('utf-8'))
            
            minio_client.put_object(
                RAW_BUCKET_NAME, batch_file_name, data_stream,
                len(batch_data_str), content_type='application/x-json-stream'
            )
            total_records_processed += len(record_batch)

        consumer.close()
        print("Consumer has been closed.")
        print(f"Total records processed and stored: {total_records_processed}")

if __name__ == "__main__":
    consume_and_store_data()