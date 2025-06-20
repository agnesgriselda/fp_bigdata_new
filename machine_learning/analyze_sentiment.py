import os
import io
import json
import pandas as pd
from minio import Minio
from minio.error import S3Error
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --- Konfigurasi ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
REVIEW_BUCKET = 'unstructured-raw-data'
PROCESSED_BUCKET = 'processed-data'
OUTPUT_FILE = 'sentiment_analysis_results.parquet'

def main():
    print("--- SENTIMENT ANALYSIS PIPELINE ---")
    
    # 1. Koneksi ke MinIO
    try:
        minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        print("Successfully connected to MinIO.")
    except Exception as e:
        print(f"Failed to connect to MinIO: {e}"); return

    # 2. Extract: Baca semua data ulasan dari MinIO
    try:
        print(f"Fetching reviews from bucket '{REVIEW_BUCKET}'...")
        review_objects = minio_client.list_objects(REVIEW_BUCKET, recursive=True)
        reviews = []
        for obj in review_objects:
            response = minio_client.get_object(REVIEW_BUCKET, obj.object_name)
            lines = response.read().decode('utf-8').strip().split('\n')
            for line in lines:
                if line: reviews.append(json.loads(line))
        
        if not reviews:
            print("No reviews found. Aborting."); return
        print(f"Successfully loaded {len(reviews)} reviews.")
        df = pd.DataFrame(reviews)
    except S3Error as e:
        print(f"Error fetching data from MinIO: {e}"); return

    # 3. Transform: Lakukan Analisis Sentimen
    print("Performing sentiment analysis using VADER...")
    analyzer = SentimentIntensityAnalyzer()
    
    def get_sentiment(text):
        scores = analyzer.polarity_scores(str(text))
        if scores['compound'] >= 0.05: return 'Positive'
        elif scores['compound'] <= -0.05: return 'Negative'
        return 'Neutral'

    df['sentiment'] = df['text'].apply(get_sentiment)
    print("Sentiment analysis complete.")
    print("Sentiment Distribution:\n", df['sentiment'].value_counts())

    # 4. Load: Simpan hasil ke bucket 'processed-data'
    print(f"Saving analysis results to '{PROCESSED_BUCKET}/{OUTPUT_FILE}'...")
    try:
        with io.BytesIO() as buffer:
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            minio_client.put_object(
                PROCESSED_BUCKET, OUTPUT_FILE, data=buffer,
                length=buffer.getbuffer().nbytes, content_type='application/octet-stream'
            )
        print("Successfully uploaded sentiment analysis results.")
    except Exception as e:
        print(f"Error uploading results to MinIO: {e}")

if __name__ == "__main__":
    main()