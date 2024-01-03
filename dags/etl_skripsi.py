# Mengimport library dan modul
from google.cloud import storage
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import io

# Membuat variabel
BQ_PROJECT = 'exemplary-city-396006'
BQ_DS = 'skripsi'
GCS_BUCKET = 'data-gizi'
GCS_OBJECT_PATH = 'extracted-data'
POSTGRES_CONNECTION_ID = 'postgres_default'

# Mengatur Parameter Google Cloud Storage
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

# Mengatur koneksi ke database PostgreSQL
hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)

# Extract Data ke GCS
def postgres_to_gcs(tables):

    client = storage.Client()
    
    for table_name in tables:
        df = hook.get_pandas_df(f"SELECT * FROM {table_name};")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        bucket_name = GCS_BUCKET
        blob_name = f'{GCS_OBJECT_PATH}/{table_name}.csv'
        
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')


def cleaning_balita():
    client = storage.Client()
    bucket_name = GCS_BUCKET
    file_name= 'balita'
    source_blob_name = f'{GCS_OBJECT_PATH}/{file_name}.csv'
    destination_blob_name = f'clean_data/clean_{file_name}.csv'

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    data = blob.download_as_string()
    data = pd.read_csv(io.StringIO(data.decode('utf-8')))


    # Menggantikan nilai '-' dengan NaN
    data_cleaned = data.replace('-', pd.NA)
    data_cleaned = data_cleaned.applymap(lambda x: str(x).title() if isinstance(x, str) else x)
    

    # mengubah 'tgl_lahir' menjadi format datetime 
    data_cleaned['tgl_lahir'] = pd.to_datetime(data_cleaned['tgl_lahir'], errors='coerce')

    # Mengisi missing values untuk 'bb_lahir' dan 'tb_lahir'dengan rata-rata
    data_cleaned['bb_lahir'].fillna(data_cleaned['bb_lahir'].median(), inplace=True)
    data_cleaned['tb_lahir'].fillna(data_cleaned['tb_lahir'].median(), inplace=True)

    # Mengisi nilai "Tidak Diketahui" pada kolom 'Nama Ortu' yang hilang
    data_cleaned['nama_ortu'].fillna('Tidak Diketahui', inplace=True)
    data_cleaned['nama_ortu'] = data_cleaned['nama_ortu'].str.replace(' /', '/').str.replace('/ ', '/').str.replace('/', ' / ')
    print(data_cleaned)


    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data_cleaned.to_csv(index=False), 'application/csv')

def cleaning_pengukuran():
    client = storage.Client()
    bucket_name = GCS_BUCKET
    file_name= 'pengukuran'
    source_blob_name = f'{GCS_OBJECT_PATH}/{file_name}.csv'
    destination_blob_name = f'clean_data/clean_{file_name}.csv'
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    data = blob.download_as_string()
    data = pd.read_csv(io.StringIO(data.decode('utf-8')))

    # Menggantikan nilai '-' dengan NaN
    data_cleaned = data.replace('-', pd.NA)

    # Menghapus baris yang memiliki nilai 0 dan missing values pada kolom 'berat' dan 'tinggi'
    data_cleaned = data_cleaned[(data_cleaned['berat'] != 0) & (data_cleaned['tinggi'] != 0)].dropna(subset=['berat', 'tinggi','tanggal_pengukuran'])
    
    # Mendefinisikan Katagori Status Gizi 
    status_gizi = {
        'BB/U': [
            ('Berat Badan Sangat Kurang', -9999, -3),
            ('Berat Badan Kurang', -3, -2),
            ('Berat Badan Normal', -2, 1),
            ('Risiko Lebih', 1, 9999)
        ],
        'TB/U': [
            ('Sangat Pendek', -9999, -3),
            ('Pendek', -3, -2),
            ('Normal', -2, 2),
            ('Tinggi', 2, 9999)
        ],
        'BB/TB': [
            ('Gizi Buruk', -9999, -3),
            ('Gizi Kurang', -3, -2),
            ('Gizi Baik', -2, 1),
            ('Berisiko Gizi Lebih', 1, 2),
            ('Gizi Lebih', 2, 3),
            ('Obesitas', 3, 9999)
        ]
    }

    # Mendapatkan nilai status gizi berdasarkan Z-Score dan kategori
    def get_status_gizi(zscore, kategori):
        try:
            zscore = float(zscore)  
        except ValueError:
            return None  
        
        for status, min_val, max_val in status_gizi[kategori]:
            if min_val <= zscore < max_val:
                return status
        return None

    # Konversi Z-scores ke numeric
    data_cleaned['zs_bb_u'] = pd.to_numeric(data_cleaned['zs_bb_u'], errors='coerce')
    data_cleaned['zs_tb_u'] = pd.to_numeric(data_cleaned['zs_tb_u'], errors='coerce')
    data_cleaned['zs_bb_tb'] = pd.to_numeric(data_cleaned['zs_bb_tb'], errors='coerce')

    # Mengisi missing valeau status gizi Z-score
    data_cleaned['bb_u'] = data_cleaned.apply(lambda row: get_status_gizi(row['zs_bb_u'], 'BB/U') if pd.isnull(row['bb_u']) else row['bb_u'], axis=1)
    data_cleaned['tb_u'] = data_cleaned.apply(lambda row: get_status_gizi(row['zs_tb_u'], 'TB/U') if pd.isnull(row['tb_u']) else row['tb_u'], axis=1)
    data_cleaned['bb_tb'] = data_cleaned.apply(lambda row: get_status_gizi(row['zs_bb_tb'], 'BB/TB') if pd.isnull(row['bb_tb']) else row['bb_tb'], axis=1)
    
    #  Konversi menjadi tipe data datetime
    data_cleaned['tanggal_pengukuran'] = pd.to_datetime(data_cleaned['tanggal_pengukuran'], errors='coerce')

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data_cleaned.to_csv(index=False), 'application/csv')

def cleaning_lokasi():
    client = storage.Client()
    bucket_name = GCS_BUCKET
    file_name= 'lokasi'
    source_blob_name = f'{GCS_OBJECT_PATH}/{file_name}.csv'
    destination_blob_name = f'clean_data/clean_{file_name}.csv'
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    data = blob.download_as_string()
    data = pd.read_csv(io.StringIO(data.decode('utf-8')))


    # Menggantikan nilai '-' dengan NaN
    data_cleaned = data.replace('-', pd.NA).drop_duplicates().reset_index(drop=True)
    data_cleaned = data_cleaned.applymap(lambda x: str(x).title() if isinstance(x, str) else x)

    # Mengisi missing value 'posyandu'
    data_cleaned['posyandu'].fillna('Tidak Diketahui', inplace=True)


    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data_cleaned.to_csv(index=False), 'application/csv')



def fact():
    client = storage.Client()
    bucket_name = GCS_BUCKET
    bucket = client.get_bucket(bucket_name)

    data_balita = io.BytesIO(bucket.blob(f'clean_data/clean_balita.csv').download_as_string())
    data_lokasi = io.BytesIO(bucket.blob(f'clean_data/clean_lokasi.csv').download_as_string())
    data_pengukuran = io.BytesIO(bucket.blob(f'clean_data/clean_pengukuran.csv').download_as_string())

    balita_df = pd.read_csv(data_balita)
    lokasi_df = pd.read_csv(data_lokasi)
    pengukuran_df = pd.read_csv(data_pengukuran)


    # membuat tabel dimensi Tanggal
    pengukuran_df['tanggal_pengukuran'] = pd.to_datetime(pengukuran_df['tanggal_pengukuran'])
    dim_date = pengukuran_df['tanggal_pengukuran'].drop_duplicates().sort_values().reset_index(drop=True)
    dim_date = pd.DataFrame({
        'date_id': dim_date.dt.date,
        'year': dim_date.dt.year,
        'month': dim_date.dt.month,
        'day': dim_date.dt.day,
    })

    # menghapus NaN values
    dim_date = dim_date.dropna(subset=['date_id'])

    # Membuat tabel dimensi lokasi juga menambahkan surrogate key
    selected_lokasi = ['prov', 'kab_kota', 'kec', 'pukesmas', 'desa_kel', 'posyandu']
    dim_lokasi = lokasi_df[selected_lokasi]
    dim_lokasi.insert(0, 'lokasi_id', range(1, 1 + len(dim_lokasi)))


    fact_table = pd.merge(pengukuran_df,
                        balita_df,
                        on='balita_id',
                        how='inner')
    
    fact_table = fact_table.rename(columns={
        'tanggal_pengukuran': 'date_id',
    })

    # konversi tipe data ke datetime
    fact_table['date_id'] = pd.to_datetime(fact_table['date_id']).dt.date

    fact_table = fact_table[
        ['pengukuran_id', 'balita_id', 'lokasi_id', 'date_id',
        'berat', 'tinggi', 'bb_u', 'zs_bb_u', 'tb_u', 'zs_tb_u', 'bb_tb', 'zs_bb_tb']
    ]

    # Membuat tabel dimensi pengukuran
    selected_pengukuran = ['usia_saat_ukur', 'berat', 'tinggi', 'zs_bb_u', 'zs_tb_u', 'zs_bb_tb']
    dim_pengukuran = pengukuran_df[selected_pengukuran]
    dim_pengukuran.insert(0, 'pengukuran_id', range(1, 1 + len(dim_pengukuran)))

    # Mengektrak kombinasi unik untuk dimensi status gizi
    status_gizi_combinations = fact_table[['bb_u', 'tb_u', 'bb_tb']].drop_duplicates().reset_index(drop=True)

    # Menambahkan surrogate key untuk dimensi status gizi
    status_gizi_combinations.insert(0, 'status_gizi_id', range(1, 1 + len(status_gizi_combinations)))

    # Mengubah nama kolom tabel status gizi
    dim_status_gizi = status_gizi_combinations.rename(columns={
        'bb_u': 'bb_u_status',
        'tb_u': 'tb_u_status',
        'bb_tb': 'bb_tb_status'
    })

    # Menggabungkan dimensi status gizi dengan tabel fakta
    fact_table_with_status = pd.merge(
        fact_table,
        dim_status_gizi,
        how='left',
        left_on=['bb_u', 'tb_u', 'bb_tb'],
        right_on=['bb_u_status', 'tb_u_status', 'bb_tb_status']
    )

    fact_table_final = fact_table_with_status[['pengukuran_id', 'balita_id', 'lokasi_id', 'date_id',
                                            'berat', 'tinggi', 'status_gizi_id']]

 
    # Menambahkan status_gizi_id
    fact_table = pd.merge(
        fact_table,
        dim_status_gizi[['status_gizi_id', 'bb_u_status', 'tb_u_status', 'bb_tb_status']],
        how='left',
        left_on=['bb_u', 'tb_u', 'bb_tb'],
        right_on=['bb_u_status', 'tb_u_status', 'bb_tb_status']
    )


    # Menambahkan kolom jumlahkasus pada tabel fakta
    fact_table['jumlahkasus'] = 0

    # Menghitung jumlahkasus dengan kondisi
    fact_table['jumlahkasus'] = (
        ((fact_table['zs_bb_u'] < -2) | (fact_table['zs_bb_u'] > 1)).astype(int) +
        ((fact_table['zs_tb_u'] < -2) | (fact_table['zs_tb_u'] > 3)).astype(int) +
        ((fact_table['zs_bb_tb'] < -2) | (fact_table['zs_bb_tb'] > 1)).astype(int)
    )


    # Tabel fakta final
    fact_table_final = fact_table[['pengukuran_id', 'balita_id', 'lokasi_id', 'date_id', 'status_gizi_id','jumlahkasus' ]]


    # membuat tabel dimensi balita juga menambahkan surogate key
    selected_balita = ['nik', 'nama', 'jk', 'tgl_lahir', 'bb_lahir', 'tb_lahir']
    dim_balita = balita_df[selected_balita]
    dim_balita.insert(0, 'balita_id', range(1, 1 + len(dim_balita)))

    # menambahkan id pada tabel fakta
    fact_table_final.insert(0, 'fact_id', range(1, 1 + len(fact_table_final)))

    # Menyimpan kembali ke GCS
    destination_blob_name=f'data_ready/fact_gizi.csv'
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(fact_table_final.to_csv(index=False), 'application/csv')
    bucket.blob('data_ready/dim_status.csv').upload_from_string(dim_status_gizi.to_csv(index=False), 'application/csv')
    bucket.blob('data_ready/dim_balita.csv').upload_from_string(dim_balita.to_csv(index=False), 'application/csv')
    bucket.blob('data_ready/dim_date.csv').upload_from_string(dim_date.to_csv(index=False), 'application/csv')
    bucket.blob('data_ready/dim_pengukuran.csv').upload_from_string(dim_pengukuran.to_csv(index=False), 'application/csv')
    bucket.blob('data_ready/dim_lokasi.csv').upload_from_string(dim_lokasi.to_csv(index=False), 'application/csv')


# Mendeklarasikan DAG 
with DAG(
    dag_id='ETL_SKRIPSI',
    start_date=days_ago(1),
    default_args={
        'owner': 'airflow',
        'retries': 2,
    },
    schedule_interval='@daily',
    max_active_runs=1,
) as dag:
    
    postgres_to_gcs_task = PythonOperator(
    task_id='postgres_to_gcs',
    python_callable=postgres_to_gcs,
    provide_context=True,  
    op_kwargs={
        'tables': ['lokasi','pengukuran','balita',]
    },
    dag=dag
    ) 

    cleaning_balita_task = PythonOperator(
    task_id='cleaning_balita',
    python_callable=cleaning_balita,
    provide_context=True,  
    dag=dag
    )

    cleaning_pengukuran_task = PythonOperator(
    task_id='cleaning_pengukuran',
    python_callable=cleaning_pengukuran,
    provide_context=True,  
    dag=dag
    )

    cleaning_lokasi_task = PythonOperator(
    task_id='cleaning_lokasi',
    python_callable=cleaning_lokasi,
    provide_context=True,  
    dag=dag
    )

    create_fact_table_task = PythonOperator(
    task_id='create_fact_table',
    python_callable=fact,
    provide_context=True,
    dag=dag
)

    
tables = ['dim_balita', 'dim_lokasi', 'dim_pengukuran', 'dim_status', 'dim_date', 'fact_gizi']

gcs_to_bq_tasks = {}

# Load tabel dimensi dan fakta ke bigquery
for table_name in tables:
    gcs_to_bq_tasks[table_name] = GCSToBigQueryOperator(
        task_id=f'gcs_to_bq_{table_name}',
        bucket=GCS_BUCKET,
        source_objects=[f'data_ready/{table_name}.csv'],  
        destination_project_dataset_table=f'{BQ_PROJECT}.{BQ_DS}.{table_name}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        autodetect=True,
        source_format='CSV',  
    )

# Melakukan Extract postgres_to_gcs dilanjutkan dengan cleaning
postgres_to_gcs_task >> [cleaning_balita_task, cleaning_pengukuran_task, cleaning_lokasi_task]

cleaning_balita_task >> create_fact_table_task
cleaning_pengukuran_task >> create_fact_table_task
cleaning_lokasi_task >> create_fact_table_task

# tranformasi tabel dan Load ke Bigquery
create_fact_table_task >> gcs_to_bq_tasks['dim_date']
create_fact_table_task >> gcs_to_bq_tasks['dim_lokasi']
create_fact_table_task >> gcs_to_bq_tasks['dim_pengukuran']
create_fact_table_task >> gcs_to_bq_tasks['dim_status']
create_fact_table_task >> gcs_to_bq_tasks['dim_balita']
create_fact_table_task >> gcs_to_bq_tasks['fact_gizi']

