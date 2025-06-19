import json
import os
import time
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import io

# --- Konfigurasi ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# MODIFIKASI: Definisikan topic dan bucket untuk kedua jenis data
FLIGHT_TOPIC = 'flight-data'
REVIEW_TOPIC = 'flight-reviews'

FLIGHT_BUCKET_NAME = 'raw-data'
REVIEW_BUCKET_NAME = 'unstructured-raw-data'

# Ukuran batch bisa di-tuning per jenis data jika perlu
FLIGHT_BATCH_SIZE = 10000
REVIEW_BATCH_SIZE = 100 

def create_kafka_consumer():
    """Membuat KafkaConsumer yang mendengarkan DUA topic sekaligus."""
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    try:
        consumer = KafkaConsumer(
            FLIGHT_TOPIC, REVIEW_TOPIC, # MODIFIKASI UTAMA: Langganan ke dua topic
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            group_id='unified-consumer-group-batched', # Group ID untuk semua data
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=30000, # Waktu tunggu lebih lama (30 detik)
        )
        print(f"Successfully connected to Kafka. Listening on topics: {FLIGHT_TOPIC}, {REVIEW_TOPIC}")
        return consumer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        time.sleep(5)
        return create_kafka_consumer()

def create_minio_client():
    """Membuat instance MinIO client."""
    print(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    try:
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        print("Successfully connected to MinIO.")
        return client
    except Exception as e:
        print(f"Failed to connect to MinIO: {e}")
        return None

def setup_minio_buckets(client):
    """Memastikan semua bucket yang dibutuhkan (termasuk untuk data unstruktur) sudah ada."""
    # MODIFIKASI: Tambahkan bucket baru ke daftar
    buckets_to_create = [FLIGHT_BUCKET_NAME, REVIEW_BUCKET_NAME, 'processed-data']
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

def upload_batch_to_minio(client, bucket, batch_data, file_prefix):
    """Helper function untuk meng-upload batch ke MinIO."""
    if not batch_data:
        return 0
    
    batch_file_name = f"{file_prefix}_batch_{int(time.time() * 1000)}.jsonl"
    batch_data_str = '\n'.join(json.dumps(record) for record in batch_data)
    data_stream = io.BytesIO(batch_data_str.encode('utf-8'))
    
    try:
        client.put_object(
            bucket, batch_file_name, data_stream, len(batch_data_str),
            content_type='application/x-json-stream'
        )
        return len(batch_data)
    except S3Error as e:
        print(f"Error uploading batch '{batch_file_name}' to MinIO: {e}")
        return 0

def consume_and_store_data():
    """Mengkonsumsi data dari berbagai topic dan menyimpannya ke MinIO yang sesuai."""
    consumer = create_kafka_consumer()
    minio_client = create_minio_client()

    if not consumer or not minio_client or not setup_minio_buckets(minio_client):
        print("Initialization failed. Exiting.")
        return

    # MODIFIKASI: Buat batch terpisah untuk setiap jenis data
    flight_batch, review_batch = [], []
    total_flights, total_reviews = 0, 0
    start_time = time.time()

    try:
        for message in consumer:
            # MODIFIKASI UTAMA: Arahkan data berdasarkan topic
            if message.topic == FLIGHT_TOPIC:
                flight_batch.append(message.value)
            elif message.topic == REVIEW_TOPIC:
                review_batch.append(message.value)

            # Cek dan upload batch data penerbangan jika sudah penuh
            if len(flight_batch) >= FLIGHT_BATCH_SIZE:
                count = upload_batch_to_minio(minio_client, FLIGHT_BUCKET_NAME, flight_batch, "flights")
                total_flights += count
                print(f"Uploaded FLIGHT batch. Total flights processed: {total_flights}")
                flight_batch = []

            # Cek dan upload batch data ulasan jika sudah penuh
            if len(review_batch) >= REVIEW_BATCH_SIZE:
                count = upload_batch_to_minio(minio_client, REVIEW_BUCKET_NAME, review_batch, "reviews")
                total_reviews += count
                print(f"Uploaded REVIEW batch. Total reviews processed: {total_reviews}")
                review_batch = []

    except KeyboardInterrupt:
        print("\nStopping consumer by user request...")
    finally:
        print("Uploading final batches...")
        # Upload sisa batch yang ada
        if flight_batch:
            count = upload_batch_to_minio(minio_client, FLIGHT_BUCKET_NAME, flight_batch, "flights_final")
            total_flights += count
        if review_batch:
            count = upload_batch_to_minio(minio_client, REVIEW_BUCKET_NAME, review_batch, "reviews_final")
            total_reviews += count

        consumer.close()
        print("Consumer has been closed.")
        print(f"Total records processed: {total_flights} flights, {total_reviews} reviews.")
        print(f"Total time taken for consuming: {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    consume_and_store_data()
