"""
Test complet : Extraction 3 sources + Transformation Spark
"""

import os
import sys
import importlib.util
import numpy as np
import pandas as pd

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from dotenv import load_dotenv
load_dotenv()


def clean_df_for_spark(df: pd.DataFrame) -> pd.DataFrame:
    """   
    Nettoyage et préparation d'un DataFrame pour Spark en assurant la présence de toutes les colonnes
    pour que Spark puisse inférer le schéma correctement.
    """

    schema = {
        "title":        str,
        "availability": str,
        "price":        float,
        "rating":       float,
        "source":       str,
    }

    for col, dtype in schema.items():
        if col not in df.columns:
            df[col] = np.nan if dtype == float else ""

    df = df[[c for c in schema.keys() if c in df.columns]].copy()

    df["price"]  = pd.to_numeric(df["price"],  errors="coerce").astype(float)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").astype(float)

    df["title"]        = df["title"].fillna("").astype(str)
    df["availability"] = df["availability"].fillna("").astype(str)
    df["source"]       = df["source"].fillna("unknown").astype(str)

    return df


print("=" * 55)
print("  Test Transformation Spark — Pipeline ETL")
print("=" * 55)

print("\n[1] Extraction Web...")
from src.extraction.web_extractor import WebExtractor
df_web = clean_df_for_spark(WebExtractor(max_pages=1).extract())
print(f"    Web : {len(df_web)} livres")

print("\n[2] Extraction SQL (Supabase)...")
from src.extraction.sql_extractor import SQLExtractor

QUERY = """
    SELECT first_name AS title, country AS availability,
           age::float AS price, 0 AS rating, 'sql' AS source
    FROM customers LIMIT 20
"""
df_sql = clean_df_for_spark(
    SQLExtractor(os.getenv("DATABASE_URL"), QUERY).extract()
)
print(f"     SQL : {len(df_sql)} lignes")

print("\n[3] Extraction CSV (échantillon)...")
csv_file = os.path.join(project_root, "data", "raw", "dataset.csv")
df_csv = None
if os.path.exists(csv_file):
    raw = pd.read_csv(csv_file, encoding="utf-8", nrows=100)
    raw.columns = [c.lower().replace("-", "_") for c in raw.columns]
    raw = raw.rename(columns={
        "book_title": "title",
        "book_author": "availability",
        "year_of_publication": "rating"
    })
    raw["price"]  = np.nan
    raw["source"] = "csv"
    df_csv = clean_df_for_spark(raw)
    print(f"     CSV : {len(df_csv)} lignes")
else:
    print("     CSV non trouvé")

print("\n[4] Transformation Spark...")

spec = importlib.util.spec_from_file_location(
    "spark_transformer",
    os.path.join(project_root, "src", "transformation", "spark_transformer.py")
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
SparkTransformer = module.SparkTransformer

dfs = [df_web, df_sql]
if df_csv is not None:
    dfs.append(df_csv)

transformer = SparkTransformer()
df_final = transformer.transform(dfs)

print(f"\n{'=' * 55}")
print(f"   TRANSFORMATION RÉUSSIE !")
print(f"{'=' * 55}")
print(f"  Lignes finales : {df_final.count()}")
print(f"  Colonnes       : {df_final.columns}")
print(f"\nAperçu :")
df_final.select("title", "price", "source", "price_category").show(5, truncate=35)

transformer.spark.stop()
print("\n Pipeline ETL terminé avec succès !")