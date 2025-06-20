import time
import csv
import json
from kafka import KafkaProducer
import os
import uuid
from datetime import datetime

# --- Konfigurasi ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
FLIGHT_TOPIC = 'flight-data'
REVIEW_TOPIC = 'flight-reviews'
FLIGHT_CSV_PATH = 'data_source/flights.csv'
REVIEW_TXT_PATH = 'data_source/customer_reviews.txt'

def create_producer():
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            api_version=(0, 10, 2), batch_size=65536, linger_ms=5, compression_type='lz4'
        )
        print("Successfully connected to Kafka Broker.")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka Broker: {e}")
        time.sleep(5)
        return create_producer()

def publish_flight_data(producer, topic):
    print("\n" + "="*50 + f"\nSTREAMING DATA PENERBANGAN (TERSTRUKTUR) ke topic '{topic}'")
    
    # MODIFIKASI: Konfigurasi untuk simulasi burst
    RECORD_LIMIT = 500000 
    BURST_SIZE = 1000
    
    try:
        with open(FLIGHT_CSV_PATH, mode='r') as file:
            csv_reader = csv.DictReader(file)
            print(f"Starting to send up to {RECORD_LIMIT} records in bursts of {BURST_SIZE}...")
            start_time = time.time()
            records_sent_total = 0
            
            batch = []
            for i, row in enumerate(csv_reader):
                if records_sent_total >= RECORD_LIMIT:
                    print(f"Reached record limit of {RECORD_LIMIT}."); break
                
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
                    records_sent_total += 1
                    
                    # MODIFIKASI: Kirim per burst
                    if records_sent_total % BURST_SIZE == 0:
                        producer.flush() # Paksa kirim pesan yang ada di buffer
                        print(f"Sent burst of {BURST_SIZE}. Total sent: {records_sent_total}. Pausing for 1 sec...")
                        time.sleep(1) # Jeda 1 detik
                 
                except (ValueError, KeyError): continue
        
        end_time = time.time()
        print(f"Finished sending {records_sent_total} flight records in {end_time - start_time:.2f} seconds.")
    except FileNotFoundError:
        print(f"ERROR: File not found at {FLIGHT_CSV_PATH}")

def publish_review_data(producer, topic):
    print("\n" + "="*50 + f"\nSTREAMING DATA ULASAN (TIDAK TERSTRUKTUR) ke topic '{topic}'")
    try:
        with open(REVIEW_TXT_PATH, 'r', encoding='utf-8') as f:
            for review_text in f:
                review_text = review_text.strip()
                if not review_text: continue
                message = {
                    'review_id': str(uuid.uuid4()), 'text': review_text,
                    'source': 'customer_reviews_txt_file', 'ingestion_timestamp': datetime.now().isoformat()
                }
                producer.send(topic, value=message)
            print("Finished sending all reviews from text file.")
    except FileNotFoundError:
        print(f"ERROR: File '{REVIEW_TXT_PATH}' not found. Run 'python generate_reviews.py' first!")

if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        publish_flight_data(kafka_producer, FLIGHT_TOPIC)
        publish_review_data(kafka_producer, REVIEW_TOPIC)
        print("\nFlushing all final messages...")
        kafka_producer.flush()
        kafka_producer.close()
        print("Producer has been closed.")