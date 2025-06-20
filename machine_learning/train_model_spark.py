# machine_learning/train_model_spark.py (Versi Final dengan Ekstraksi Artefak)

import os
import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from minio import Minio # Tambahkan MinIO untuk menyimpan artefak

# (Fungsi create_spark_session tetap sama persis seperti sebelumnya)
def create_spark_session():
    print("Creating Spark session configured for MinIO...")
    aws_sdk_version = "1.12.262"; hadoop_aws_version = "3.3.4"
    spark = SparkSession.builder \
        .appName("FlightModelTraining_Spark") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version},io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    print("Spark session created successfully.")
    return spark

def main():
    spark = create_spark_session()
    if not spark: return

    PROCESSED_DATA_BUCKET = 'processed-data'
    DELTA_TABLE_NAME = 'cleaned_flight_data_delta'
    MODEL_PATH = f's3a://{PROCESSED_DATA_BUCKET}/spark_flight_delay_model'
    
    # 1. Muat data dari tabel Delta
    print(f"Loading data from Delta table: {DELTA_TABLE_NAME}")
    df = spark.read.format("delta").load(f"s3a://{PROCESSED_DATA_BUCKET}/{DELTA_TABLE_NAME}")
    
    # Ambil sampel kecil untuk training yang lebih cepat saat demo jika perlu
    df = df.sample(fraction=0.1, seed=42)
    print(f"Training on a sample of {df.count()} records.")

    # 2. Pipeline Feature Engineering & Model
    indexer = StringIndexer(inputCol="AIRLINE", outputCol="AIRLINE_Index", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol="AIRLINE_Index", outputCol="AIRLINE_Vec")
    feature_cols = ["MONTH", "DAY", "DEPARTURE_DELAY", "DISTANCE", "AIRLINE_Vec"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    rf = RandomForestRegressor(featuresCol="features", labelCol="ARRIVAL_DELAY", seed=42)
    pipeline = Pipeline(stages=[indexer, encoder, assembler, rf])
    
    # 3. Latih Model
    print("Splitting data and training model...")
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
    pipeline_model = pipeline.fit(train_data)
    print("Model training complete.")
    
    # 4. Evaluasi Model
    predictions = pipeline_model.transform(test_data)
    r2_eval = RegressionEvaluator(predictionCol="prediction", labelCol="ARRIVAL_DELAY", metricName="r2")
    rmse_eval = RegressionEvaluator(predictionCol="prediction", labelCol="ARRIVAL_DELAY", metricName="rmse")
    r2 = r2_eval.evaluate(predictions)
    rmse = rmse_eval.evaluate(predictions)
    print(f"\n--- Evaluation --- R²: {r2:.4f}, RMSE: {rmse:.2f} minutes ---\n")
    
    # 5. Simpan Model Spark (Lengkap) ke MinIO
    print(f"Saving full Spark ML model to {MODEL_PATH}...")
    pipeline_model.write().overwrite().save(MODEL_PATH)
    print("Successfully saved Spark ML model.")
    
    # --- LANGKAH KUNCI: Ekstrak Artefak untuk Streamlit ---
    print("Extracting and saving model artifacts for Streamlit dashboard...")
    try:
        # a. Dapatkan model RandomForest dari pipeline yang sudah dilatih
        rf_model = pipeline_model.stages[-1]
        
        # b. Dapatkan nama-nama fitur dari pipeline
        # Ini sedikit rumit karena melibatkan transformer yang berbeda
        ohe_features = pipeline_model.stages[1].getOutputCol()
        numeric_features = ["MONTH", "DAY", "DEPARTURE_DELAY", "DISTANCE"]
        
        # Mendapatkan nama kategori dari StringIndexer
        airline_labels = pipeline_model.stages[0].labels
        ohe_labels = [f"AIRLINE_{label}" for label in airline_labels]
        
        all_feature_names = numeric_features + ohe_labels

        # c. Buat DataFrame Pandas untuk Feature Importances
        importances_df = pd.DataFrame(
            list(zip(all_feature_names, rf_model.featureImportances)),
            columns=["feature", "importance"]
        ).sort_values(by="importance", ascending=False)
        
        # d. Simpan sebagai file CSV ke MinIO
        minio_client = Minio(
            os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key="minioadmin", secret_key="minioadmin", secure=False
        )
        csv_bytes = importances_df.to_csv(index=False).encode('utf-8')
        minio_client.put_object(
            PROCESSED_DATA_BUCKET, 'spark_model_feature_importances.csv',
            data=io.BytesIO(csv_bytes), length=len(csv_bytes), content_type='application/csv'
        )
        print("Feature importances artifact saved successfully to MinIO.")

    except Exception as e:
        print(f"Could not save feature importances artifact: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()