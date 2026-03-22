# pragma: no cover
"""
spark_transformer.py
Transformations distribuées avec Apache Spark
- Union des 3 sources (web, csv, sql)
- Nettoyage, normalisation, enrichissement
- Gestion des erreurs par source
"""

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType
from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SparkTransformer:
    """
    Transformations distribuées sur les données unifiées des 3 sources ETL.
    """

    def __init__(self):
        self.spark = get_spark_session("ETL_Transformation")

    def to_spark_df(self, pandas_df) -> DataFrame:
        """Convertit un DataFrame Pandas en DataFrame Spark."""
        logger.info(f"Conversion Pandas → Spark ({len(pandas_df)} lignes)")
        return self.spark.createDataFrame(pandas_df)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Nettoyage des données :
        - Suppression des lignes sans titre
        - Valeurs nulles remplacées par des valeurs par défaut
        - Suppression des doublons
        """
        logger.info("Nettoyage des données...")

        # initial_count = df.cache().count() if False else -1 

        df_cleaned = (
            df
            .dropna(subset=["title"])
            .dropDuplicates(["title", "source"])
            .withColumn(
                "availability",
                F.when(F.col("availability").isNull(), "Unknown")
                 .otherwise(F.trim(F.col("availability")))
            )
            .withColumn(
                "price",
                F.when(F.col("price").isNull(), 0.0)
                 .otherwise(F.col("price"))
            )
        )

        logger.info("Nettoyage terminé")
        return df_cleaned

    def normalize_data(self, df: DataFrame) -> DataFrame:
        """
        Normalisation des types et formats :
        - Typage explicite des colonnes
        - Normalisation des chaînes (trim, lowercase source)
        - Ajout de métadonnées
        """
        logger.info("Normalisation des données...")

        df_normalized = (
            df
            .withColumn("price",        F.col("price").cast(DoubleType()))
            .withColumn("source",       F.lower(F.trim(F.col("source"))).cast(StringType()))
            .withColumn("title",        F.trim(F.col("title")))
            .withColumn("availability", F.trim(F.col("availability")))
            .withColumn("processed_at", F.lit(datetime.utcnow().isoformat()).cast(TimestampType()))
            .withColumn("pipeline_version", F.lit("1.0.0"))
        )

        logger.info("Normalisation terminée")
        return df_normalized

    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrichissement des données :
        - Catégorie de prix
        - Flag disponibilité
        - Indice de qualité (si rating présent)
        """
        logger.info("Enrichissement des données...")

        df_enriched = df

        if "price" in df.columns:
            df_enriched = df_enriched.withColumn(
                "price_category",
                F.when(F.col("price") < 10,  "low")
                 .when(F.col("price") < 30,  "medium")
                 .when(F.col("price") < 60,  "high")
                 .otherwise("premium")
            )

        if "availability" in df.columns:
            df_enriched = df_enriched.withColumn(
                "is_available",
                F.when(F.lower(F.col("availability")).contains("in stock"), True)
                 .otherwise(False)
            )

        if "rating" in df.columns:
            df_enriched = df_enriched.withColumn(
                "rating_label",
                F.when(F.col("rating") >= 4, "excellent")
                 .when(F.col("rating") >= 3, "good")
                 .when(F.col("rating") >= 2, "average")
                 .otherwise("poor")
            )

        logger.info("Enrichissement terminé")
        return df_enriched

    def transform(self, dfs: list) -> DataFrame:
        """
        Pipeline complet de transformation.

        Args:
            dfs : Liste de DataFrames Pandas (web, csv, sql)

        Returns:
            DataFrame Spark transformé, prêt pour le chargement
        """
        if not dfs:
            raise ValueError("La liste de DataFrames est vide")

        logger.info(f"Début transformation — {len(dfs)} source(s)")

        spark_dfs = [self.to_spark_df(df) for df in dfs]

        df_union = spark_dfs[0]
        for df in spark_dfs[1:]:
            df_union = df_union.unionByName(df, allowMissingColumns=True)

        # total = df_union.count()
        logger.info("Union des sources terminée")

        df_cleaned    = self.clean_data(df_union)
        df_normalized = self.normalize_data(df_cleaned)
        df_final      = self.enrich_data(df_normalized)

        logger.info("Transformation terminée avec succès")
        return df_final