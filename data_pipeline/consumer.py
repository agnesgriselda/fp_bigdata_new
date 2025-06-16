import json
import os
import time
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import io

# --- Konfigurasi (Tidak ada perubahan) ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = 'flight-data'
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET_NAME = 'raw-data'

# --- UKURAN BATCH UNTUK DISIMPAN KE MINIO ---
# Simpan data ke MinIO setiap 10,000 record terkumpul
BATCH_SIZE = 10000

def create_kafka_consumer():
    """Membuat dan mengembalikan instance KafkaConsumer yang sudah di-tuning."""
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            group_id='flight-data-consumer-group-batched', # Nama group baru untuk reset offset
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(0, 10, 2),
            # Ambil data dalam batch besar dari Kafka
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

def consume_and_store_data():
    """Mengkonsumsi data dari Kafka dan menyimpannya ke MinIO dalam batch."""
    consumer = create_kafka_consumer()
    minio_client = create_minio_client()

    if not consumer or not minio_client:
        return

    print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")
    print(f"Will store data in batches of {BATCH_SIZE} records.")
    
    # Inisialisasi list untuk menampung batch dan counter
    record_batch = []
    total_records_processed = 0
    start_time = time.time()

    try:
        for message in consumer:
            # Tambahkan record ke batch
            record_batch.append(message.value)
            
            # Jika ukuran batch sudah tercapai, proses dan simpan ke MinIO
            if len(record_batch) >= BATCH_SIZE:
                # Buat nama file unik untuk batch ini
                batch_file_name = f"batch_{int(time.time() * 1000)}.jsonl"
                
                # Gabungkan semua record JSON menjadi satu string dengan format JSON Lines
                # (satu objek JSON per baris, dipisahkan oleh newline)
                batch_data_str = '\n'.join(json.dumps(record) for record in record_batch)
                
                # Ubah string menjadi stream byte untuk diunggah
                data_stream = io.BytesIO(batch_data_str.encode('utf-8'))
                
                try:
                    # Unggah batch sebagai satu file ke MinIO
                    minio_client.put_object(
                        MINIO_BUCKET_NAME,
                        batch_file_name,
                        data_stream,
                        len(batch_data_str),
                        content_type='application/x-json-stream' # Tipe konten untuk JSON Lines
                    )
                    
                    # Update counter dan berikan status
                    processed_count = len(record_batch)
                    total_records_processed += processed_count
                    elapsed_time = time.time() - start_time
                    print(f"Uploaded batch '{batch_file_name}'. Processed {total_records_processed} records in {elapsed_time:.2f}s.")

                    # Kosongkan batch untuk siklus berikutnya
                    record_batch = []
                    
                except S3Error as e:
                    print(f"Error uploading batch to MinIO: {e}")

    except KeyboardInterrupt:
        print("\nStopping consumer by user request...")
    finally:
        # PENTING: Simpan sisa data di batch terakhir sebelum keluar
        if record_batch:
            print(f"Uploading final batch of {len(record_batch)} records...")
            batch_file_name = f"batch_{int(time.time() * 1000)}_final.jsonl"
            batch_data_str = '\n'.join(json.dumps(record) for record in record_batch)
            data_stream = io.BytesIO(batch_data_str.encode('utf-8'))
            
            minio_client.put_object(
                MINIO_BUCKET_NAME,
                batch_file_name,
                data_stream,
                len(batch_data_str),
                content_type='application/x-json-stream'
            )
            total_records_processed += len(record_batch)

        consumer.close()
        print("Consumer has been closed.")
        print(f"Total records processed and stored: {total_records_processed}")

if __name__ == "__main__":
    consume_and_store_data()