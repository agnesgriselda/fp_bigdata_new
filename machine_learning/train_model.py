# machine_learning/train_model.py (Revisi Final Anti-Seek Error)

import os
import io
import pandas as pd
import joblib
import numpy as np
from minio import Minio
from minio.error import S3Error
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_squared_error

# --- Konfigurasi ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
PROCESSED_DATA_BUCKET = 'processed-data'
CLEANED_DATA_FILE = 'cleaned_flight_data.parquet'
MODEL_FILE_NAME = 'flight_delay_model.pkl'

def create_minio_client():
    """Membuat dan mengembalikan instance MinIO client."""
    print(f"Connecting to MinIO at {MINIO_ENDPOINT} from train_model.py...")
    try:
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        print("Successfully connected to MinIO.")
        return client
    except Exception as e:
        print(f"Failed to connect to MinIO: {e}")
        return None

def main():
    """Fungsi utama untuk menjalankan pipeline training model."""
    minio_client = create_minio_client()
    if not minio_client: return

    # Pastikan bucket ada
    try:
        if not minio_client.bucket_exists(PROCESSED_DATA_BUCKET):
            print(f"Bucket '{PROCESSED_DATA_BUCKET}' does not exist. Run prepare_data.py first."); return
    except S3Error as e:
        print(f"Error checking bucket: {e}"); return

    # 1. Muat data dari MinIO dengan cara yang lebih tangguh
    print(f"Downloading '{CLEANED_DATA_FILE}' from bucket '{PROCESSED_DATA_BUCKET}'...")
    try:
        response = minio_client.get_object(PROCESSED_DATA_BUCKET, CLEANED_DATA_FILE)
        # Download dulu ke bytes, baru dibaca
        parquet_bytes = response.read()
        parquet_buffer = io.BytesIO(parquet_bytes)
        df = pd.read_parquet(parquet_buffer)
        print(f"Successfully loaded data with {len(df)} rows.")
    except S3Error as e:
        print(f"Error downloading data: {e}. File might not exist."); return
    finally:
        if 'response' in locals(): response.close(); response.release_conn()

    if df.empty:
        print("Cleaned data is empty. Skipping training."); return

    # --- SAMPLING DATA ---
    sample_size = 500000
    if len(df) > sample_size:
        print(f"Taking a random sample of {sample_size} rows...")
        df = df.sample(n=sample_size, random_state=42)

    # ... (Sisa kode untuk persiapan data, training, evaluasi, dan upload model tetap sama persis) ...
    # 2. Persiapan Data untuk Model
    print("Preparing data for training...")
    target_column = 'arrival_delay'
    if target_column not in df.columns:
        print(f"Target column '{target_column}' not found."); return

    y = df[target_column]
    X = df.drop(target_column, axis=1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"Data split into {len(X_train)} training and {len(X_test)} testing samples.")

    # 3. Latih Model
    print("Training RandomForestRegressor...")
    model = RandomForestRegressor(n_estimators=50, max_depth=15, random_state=42, n_jobs=-1, verbose=1)
    model.fit(X_train, y_train)
    print("Model training complete.")

    # 4. Evaluasi Model
    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    print("\n--- Model Evaluation Results ---")
    print(f"R-squared (R²): {r2:.4f}")
    print(f"Root Mean Squared Error (RMSE): {rmse:.2f} minutes")
    print("--------------------------------\n")

    # 5. Simpan Model ke MinIO
    print(f"Saving model as '{MODEL_FILE_NAME}'...")
    with io.BytesIO() as buffer:
        joblib.dump(model, buffer)
        buffer.seek(0)
        minio_client.put_object(
            PROCESSED_DATA_BUCKET, MODEL_FILE_NAME, data=buffer,
            length=buffer.getbuffer().nbytes, content_type='application/octet-stream'
        )
    print(f"Successfully uploaded model to MinIO.")


if __name__ == "__main__":
    main()