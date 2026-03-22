"""
DAG Airflow — Pipeline ETL Africa Tech Up Tour
avec Airflow
"""

import os
import sys
from datetime import datetime, timedelta


from dotenv import load_dotenv
load_dotenv()
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DEFAULT_ARGS = {
    "owner":                     "africa_tech_etl",
    "depends_on_past":           False,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=2),
    "retry_exponential_backoff": True,
}

DATABASE_URL = os.getenv("DATABASE_URL",
    "postgresql+psycopg2://postgres:password@db.supabase.co:5432/postgres")
CSV_URL      = os.getenv("CSV_URL",
    "https://drive.google.com/file/d/1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD/view?usp=sharing")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")
DATA_DIR     = os.path.join(os.path.dirname(__file__), "..", "data", "raw")



def extract_web(**context):
    from src.extraction.web_extractor import WebExtractor
    os.makedirs(DATA_DIR, exist_ok=True)
    df = WebExtractor(max_pages=5).extract()
    path = os.path.join(DATA_DIR, "web_data.csv")
    df.to_csv(path, index=False)
    context["ti"].xcom_push(key="web_path", value=path)
    print(f"[WEB]  {len(df)} livres extraits → {path}")
    return len(df)


def extract_csv(**context):
    import pandas as pd
    from src.extraction.csv_extractor import CSVExtractor
    os.makedirs(DATA_DIR, exist_ok=True)
    local_raw = os.path.join(DATA_DIR, "dataset.csv")
    if os.path.exists(local_raw):
        df = pd.read_csv(local_raw, nrows=1000)
        if "source" not in df.columns:
            df["source"] = "csv"
    else:
        df = CSVExtractor(csv_url=CSV_URL).extract()
        df = df.head(1000)
    path = os.path.join(DATA_DIR, "csv_data.csv")
    df.to_csv(path, index=False)
    context["ti"].xcom_push(key="csv_path", value=path)
    print(f"[CSV]  {len(df)} lignes extraites → {path}")
    return len(df)


def extract_sql(**context):
    from src.extraction.sql_extractor import SQLExtractor
    os.makedirs(DATA_DIR, exist_ok=True)
    query = """
        SELECT
            id::text    AS title,
            email       AS availability,
            NULL::float AS price,
            0           AS rating,
            'sql'       AS source
        FROM customers LIMIT 500
    """
    df = SQLExtractor(db_url=DATABASE_URL, query=query).extract()
    path = os.path.join(DATA_DIR, "sql_data.csv")
    df.to_csv(path, index=False)
    context["ti"].xcom_push(key="sql_path", value=path)
    print(f"[SQL]  {len(df)} lignes extraites → {path}")
    return len(df)


def transform_and_load(**context):
    import pandas as pd
    from src.transformation.spark_transformer import SparkTransformer
    from src.loading.minio_loader import MinIOLoader

    ti = context["ti"]

    def load_csv(key, task_id):
        path = ti.xcom_pull(task_ids=task_id, key=key)
        if not path or not os.path.exists(path):
            return None
        df = pd.read_csv(path)
        for col in ["price", "rating"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
        for col in ["title", "availability", "source"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)
        return df

    dfs = [df for df in [
        load_csv("web_path", "extract_web"),
        load_csv("csv_path", "extract_csv"),
        load_csv("sql_path", "extract_sql"),
    ] if df is not None]

    if not dfs:
        raise ValueError("Aucune donnée extraite — pipeline interrompu")

    print(f"[TRANSFORM] {len(dfs)} sources, {sum(len(d) for d in dfs)} lignes")

    transformer = SparkTransformer()
    df_final = transformer.transform(dfs)
    print("[TRANSFORM]  Transformation terminée")

    loader = MinIOLoader(bucket=MINIO_BUCKET, dataset="processed/books")
    loader.load(df_final, partition_cols=["source"])
    print(f"[LOAD]  Données chargées → s3a://{MINIO_BUCKET}/processed/books")

    transformer.spark.stop()


def check_data_quality(**context):
    from src.loading.minio_loader import MinIOLoader
    loader = MinIOLoader(bucket=MINIO_BUCKET)
    objects = loader.list_objects(prefix="processed/books")
    if not objects:
        raise ValueError(" Qualité : aucun fichier Parquet dans MinIO !")
    print(f"[QUALITY]  {len(objects)} fichier(s) Parquet vérifiés")
    for obj in objects[:5]:
        print(f"  → {obj}")


with DAG(
    dag_id="etl_pipeline_dag",
    description="Pipeline ETL : Web + CSV + SQL → Spark → MinIO",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),  
    schedule="0 6 * * *",              
    catchup=False,
    tags=["etl", "africa-tech", "data-engineering"],
    doc_md="""
# Pipeline ETL — Africa Tech Up Tour

# Sources
- **Web** : books.toscrape.com
- **CSV** : Google Drive (Books dataset)
- **SQL** : PostgreSQL Supabase

       ┌── extract_web ──┐
start ─┤── extract_csv ──├─ transform_and_load ─ quality_check ─ end
       └── extract_sql ──┘

# Schedule : tous les jours à 6h UTC
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    t_web = PythonOperator(task_id="extract_web", python_callable=extract_web)
    t_csv = PythonOperator(task_id="extract_csv", python_callable=extract_csv)
    t_sql = PythonOperator(task_id="extract_sql", python_callable=extract_sql)

    t_transform = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
        execution_timeout=timedelta(minutes=30),
    )

    t_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    end = EmptyOperator(task_id="end")

    start >> [t_web, t_csv, t_sql] >> t_transform >> t_quality >> end