import os
import streamlit as st
import pandas as pd
import plotly.express as px
import boto3
from io import StringIO

# ========================
# KONFIGURASI STREAMLIT
# ========================
st.set_page_config(page_title="Flight Delay Dashboard", layout="wide", initial_sidebar_state="expanded")

# ========================
# KONFIGURASI MINIO
# ========================
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "processed-data"
FILE_NAME = "cleaned_flight_data.csv"

@st.cache_data
def load_data_from_minio():
    session = boto3.session.Session()
    client = session.client(
        service_name='s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    response = client.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
    content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(content))
    return df

# ========================
# LOAD DATA
# ========================
df = load_data_from_minio()

# ========================
# SIDEBAR FILTER
# ========================
st.sidebar.header("Filter Data Penerbangan")
airlines = df['airline'].unique().tolist()
origins = df['origin'].unique().tolist()
dests = df['dest'].unique().tolist()
months = sorted(df['month'].dropna().unique().tolist())

selected_airline = st.sidebar.selectbox("Pilih Maskapai", ['Semua'] + airlines)
selected_origin = st.sidebar.selectbox("Bandara Asal", ['Semua'] + origins)
selected_dest = st.sidebar.selectbox("Bandara Tujuan", ['Semua'] + dests)
selected_month = st.sidebar.selectbox("Bulan", ['Semua'] + list(map(str, months)))

filtered_df = df.copy()
if selected_airline != 'Semua':
    filtered_df = filtered_df[filtered_df['airline'] == selected_airline]
if selected_origin != 'Semua':
    filtered_df = filtered_df[filtered_df['origin'] == selected_origin]
if selected_dest != 'Semua':
    filtered_df = filtered_df[filtered_df['dest'] == selected_dest]
if selected_month != 'Semua':
    filtered_df = filtered_df[filtered_df['month'].astype(str) == selected_month]

# ========================
# STYLING TAMBAHAN
# ========================
st.markdown("""
    <style>
    .stApp {
        background-color: #0f0f23;
        color: #e0e0e0;
    }
    .css-1d391kg, .css-1v0mbdj {
        background-color: #1a1a2e;
        padding: 1rem;
        border-radius: 10px;
    }
    .block-container {
        padding-top: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

# ========================
# HEADER & KPI
# ========================
st.title("Flight Delay Dashboard")
st.markdown("Analisis keterlambatan keberangkatan dan kedatangan penerbangan berdasarkan maskapai, rute, dan waktu.")

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Rata-rata Delay Keberangkatan", f"{filtered_df['dep_delay'].mean():.2f} menit")
kpi2.metric("Rata-rata Delay Kedatangan", f"{filtered_df['arr_delay'].mean():.2f} menit")
kpi3.metric("Rata-rata Waktu Terbang", f"{filtered_df['air_time'].mean():.2f} menit")

# ========================
# VISUALISASI UTAMA
# ========================
col1, col2 = st.columns(2)

with col1:
    st.subheader("Rata-rata Delay per Maskapai")
    fig1 = px.bar(
        filtered_df.groupby("airline")["arr_delay"].mean().reset_index(),
        x="airline", y="arr_delay", color="airline",
        title="Keterlambatan Kedatangan per Maskapai"
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Korelasi Waktu Terbang dan Delay")
    fig2 = px.scatter(
        filtered_df, x="air_time", y="arr_delay", color="month",
        title="Hubungan Waktu Terbang vs Keterlambatan"
    )
    st.plotly_chart(fig2, use_container_width=True)

# ========================
# PIE DESTINASI
# ========================
st.subheader("Distribusi Tujuan Penerbangan")
fig3 = px.pie(
    filtered_df['dest'].value_counts().reset_index(),
    names='index', values='dest',
    title='Distribusi Destinasi Tujuan'
)
st.plotly_chart(fig3, use_container_width=True)

# ========================
# TABEL DATA
# ========================
with st.expander("Lihat Data Lengkap"):
    st.dataframe(filtered_df)