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

            # <-- PENAMAAN FILE DIUBAH agar lebih informatif
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
