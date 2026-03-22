"""
Chargement des données transformées dans MinIO
- Écriture Parquet partitionné via Spark
- Vérification d'existence du bucket via boto3
- En cloud et local
"""

import os
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MinIOLoader:
    """
    Chargegement des DataFrames Spark dans MinIO au format Parquet partitionné.
    """

    def __init__(self, bucket: str = None, dataset: str = "etl_output"):
        self.bucket  = bucket  or os.getenv("MINIO_BUCKET",      "datalake")
        self.endpoint= os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
        self.access  = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
        self.secret  = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
        self.dataset = dataset
        self._ensure_bucket_exists()

    def _get_s3_client(self):          # Création  d'un client boto3 connecté à MinIO.
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access,
            aws_secret_access_key=self.secret,
        )

    def _ensure_bucket_exists(self) -> None:
        try:
            client = self._get_s3_client()
            existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
            if self.bucket not in existing:
                client.create_bucket(Bucket=self.bucket)
                logger.info(f"Bucket créé : {self.bucket}")
            else:
                logger.info(f"Bucket existant : {self.bucket}")
        except Exception as e:
            logger.warning(f"Impossible de vérifier/créer le bucket : {e}")

    def load(self, df: DataFrame, partition_cols: list = None) -> None:
        """
        Écrit le DataFrame Spark dans MinIO au format Parquet.

        Args:
            df              : DataFrame Spark transformé
            partition_cols  : Colonnes de partitionnement (défaut : ["source"])
        """
        if partition_cols is None:
            partition_cols = ["source"]

        path = f"s3a://{self.bucket}/{self.dataset}"
        logger.info(f"Chargement vers MinIO : {path}")
        logger.info(f"Partitionnement par : {partition_cols}")
        logger.info(f"Nombre de lignes à charger : {df.count()}")

        try:
            (
                df.write
                .mode("overwrite")
                .partitionBy(*partition_cols)
                .parquet(path)
            )
            logger.info(f"Chargement terminé avec succès → {path}")

        except Exception as e:
            logger.error(f"Erreur lors du chargement MinIO : {e}")
            raise

    def list_objects(self, prefix: str = "") -> list:
        """
        Liste les objets dans le bucket (utile pour vérification).

        Args:
            prefix : Filtre par préfixe de chemin

        Returns:
            Liste des clés S3
        """
        try:
            client = self._get_s3_client()
            response = client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            keys = [obj["Key"] for obj in response.get("Contents", [])]
            logger.info(f"Objets dans {self.bucket}/{prefix} : {len(keys)}")
            return keys
        except Exception as e:
            logger.error(f"Erreur listing MinIO : {e}")
            return []
