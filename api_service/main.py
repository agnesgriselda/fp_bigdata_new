import os
import joblib
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio

# --- BAGIAN 1: KONFIGURASI (Tetap Sama) ---
app = FastAPI(title="API Prediksi Keterlambatan Penerbangan")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "processed-data"
MODEL_FILE_KEY = "flight_delay_model.pkl"
LOCAL_MODEL_PATH = "flight_delay_model.pkl"

try:
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    client.fget_object(BUCKET_NAME, MODEL_FILE_KEY, LOCAL_MODEL_PATH)
    model = joblib.load(LOCAL_MODEL_PATH)
    model_features = model.feature_names_in_
except Exception as e:
    model = None
    model_features = []

# --- BAGIAN 2: MODEL DATA INPUT (Tetap Sama) ---
class FlightPredictionRequest(BaseModel):
    year: int; month: int; day: int; day_of_week: int
    airline: str
    departure_delay: int
    distance: int

# --- BAGIAN 3: ENDPOINT PREDIKSI (INI YANG DIREVISI TOTAL) ---
@app.post("/predict")
def predict_flight_delay(data: FlightPredictionRequest):
    if model is None: return {"error": "Model tidak tersedia."}
    
    try:
        # 1. Buat DataFrame dari input tunggal
        input_df = pd.DataFrame([data.dict()])
        
        # 2. Lakukan One-Hot Encoding dengan benar
        # Gunakan prefix 'airline' agar nama kolomnya menjadi 'airline_AA', 'airline_DL', dst.
        input_df_encoded = pd.get_dummies(input_df, columns=['airline'], prefix='airline')
        
        # 3. Buat DataFrame "kosong" dengan semua kolom yang dibutuhkan model
        # Ini adalah trik untuk memastikan semua fitur ada
        final_df = pd.DataFrame(columns=model_features)
        
        # 4. Gabungkan (append) data input kita ke DataFrame kosong tersebut
        final_df = pd.concat([final_df, input_df_encoded], ignore_index=True, sort=False)
        
        # 5. Isi semua kolom NaN (yang tidak ada di input) dengan 0
        final_df.fillna(0, inplace=True)
        
        # 6. Pastikan urutan kolom sama persis dengan saat training
        final_df = final_df[model_features]

        # 7. Lakukan prediksi
        prediction = model.predict(final_df)
        
        return {"predicted_arrival_delay": round(prediction[0], 2)}

    except Exception as e:
        return {"error": f"Terjadi kesalahan saat prediksi: {str(e)}"}