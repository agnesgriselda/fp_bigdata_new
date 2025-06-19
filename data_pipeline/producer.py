import time
import csv
import json
from kafka import KafkaProducer
import os
import uuid
from datetime import datetime

# --- Konfigurasi ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

# MODIFIKASI: Definisikan dua topic untuk dua jenis data
FLIGHT_TOPIC = 'flight-data'
REVIEW_TOPIC = 'flight-reviews' 

# Path ke sumber data
FLIGHT_CSV_PATH = 'data_source/flights.csv'
REVIEW_TXT_PATH = 'data_source/customer_reviews.txt'


def create_producer():
    """Membuat instance KafkaProducer yang sudah di-tuning."""
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            api_version=(0, 10, 2),
            batch_size=65536,
            linger_ms=5,
            compression_type='lz4'
        )
        print("Successfully connected to Kafka Broker.")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka Broker: {e}")
        time.sleep(5)
        return create_producer()

def publish_flight_data(producer, topic):
    """Membaca data penerbangan (terstruktur) dan mengirimkannya ke Kafka."""
    print("\n" + "="*50)
    print(f"STREAMING DATA PENERBANGAN (TERSTRUKTUR) ke topic '{topic}'")
    print("="*50)

    RECORD_LIMIT = 500000 
    
    try:
        with open(FLIGHT_CSV_PATH, mode='r') as file:
            csv_reader = csv.DictReader(file)
            
            print(f"Starting to send up to {RECORD_LIMIT} flight data records...")
            start_time = time.time()
            records_sent = 0

            for i, row in enumerate(csv_reader):
                if records_sent >= RECORD_LIMIT:
                    print(f"Reached record limit of {RECORD_LIMIT}. Stopping flight data stream.")
                    break
                
                try:
                    message = {
                        "YEAR": int(row.get("YEAR", 0)), "MONTH": int(row.get("MONTH", 0)),
                        "DAY": int(row.get("DAY", 0)), "AIRLINE": row.get("AIRLINE", ""),
                        "FLIGHT_NUMBER": int(row.get("FLIGHT_NUMBER", 0)),
                        "ORIGIN_AIRPORT": row.get("ORIGIN_AIRPORT", ""),
                        "DESTINATION_AIRPORT": row.get("DESTINATION_AIRPORT", ""),
                        "DEPARTURE_DELAY": float(row.get("DEPARTURE_DELAY") or 0),
                        "ARRIVAL_DELAY": float(row.get("ARRIVAL_DELAY") or 0),
                        "DISTANCE": int(row.get("DISTANCE", 0))
                    }
                    producer.send(topic, value=message)
                    records_sent += 1
                    
                    if records_sent % 50000 == 0:
                        print(f"Sent {records_sent} flight records...")
                 
                except (ValueError, KeyError):
                    continue
        
        end_time = time.time()
        print(f"Finished sending {records_sent} flight records in {end_time - start_time:.2f} seconds.")

    except FileNotFoundError:
        print(f"ERROR: File not found at {FLIGHT_CSV_PATH}")

def publish_review_data(producer, topic):
    """Membaca data ulasan (tidak terstruktur) dan mengirimkannya ke Kafka."""
    print("\n" + "="*50)
    print(f"STREAMING DATA ULASAN (TIDAK TERSTRUKTUR) ke topic '{topic}'")
    print("="*50)
    
    try:
        with open(REVIEW_TXT_PATH, 'r') as f:
            for review_text in f:
                review_text = review_text.strip()
                if not review_text: continue

                message = {
                    'review_id': str(uuid.uuid4()),
                    'text': review_text,
                    'source': 'customer_review_text_file',
                    'ingestion_timestamp': datetime.now().isoformat()
                }
                
                producer.send(topic, value=message)
                print(f"Sent review: {message['text'][:60]}...")
                time.sleep(1) # Jeda untuk simulasi real-time
                
        print("Finished sending all reviews.")
    except FileNotFoundError:
        print(f"ERROR: File not found at {REVIEW_TXT_PATH}")


if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        # Panggil fungsi untuk mengirim kedua jenis data
        publish_flight_data(kafka_producer, FLIGHT_TOPIC)
        publish_review_data(kafka_producer, REVIEW_TOPIC)
        
        print("\nFlushing all final messages...")
        kafka_producer.flush()
        kafka_producer.close()
        print("Producer has been closed.")
