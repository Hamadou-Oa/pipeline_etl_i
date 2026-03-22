"""
Tests unitaires pour MinIOLoader
"""

import pytest
from unittest.mock import patch, MagicMock


class TestMinIOLoader:

    def test_load_calls_parquet_write(self):
        """load() doit écrire en Parquet dans MinIO"""
        with patch("boto3.client") as mock_boto:
            mock_boto.return_value.list_buckets.return_value = {"Buckets": [{"Name": "datalake"}]}

            from src.loading.minio_loader import MinIOLoader
            loader = MinIOLoader(bucket="datalake", dataset="test")

            mock_df = MagicMock()
            mock_df.count.return_value = 10
            mock_df.write.mode.return_value.partitionBy.return_value.parquet = MagicMock()

            loader.load(mock_df)

            mock_df.write.mode.assert_called_once_with("overwrite")

    def test_bucket_created_if_not_exists(self):
        """Le bucket doit être créé s'il n'existe pas"""
        with patch("boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_client.list_buckets.return_value = {"Buckets": []}
            mock_boto.return_value = mock_client

            from src.loading.minio_loader import MinIOLoader
            MinIOLoader(bucket="new-bucket")

            mock_client.create_bucket.assert_called_once_with(Bucket="new-bucket")

    def test_list_objects_returns_keys(self):
        """list_objects() doit retourner les clés des fichiers"""
        with patch("boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_client.list_buckets.return_value = {"Buckets": [{"Name": "datalake"}]}
            mock_client.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "processed/books/source=web/part-0.parquet"},
                    {"Key": "processed/books/source=csv/part-0.parquet"},
                ]
            }
            mock_boto.return_value = mock_client

            from src.loading.minio_loader import MinIOLoader
            loader = MinIOLoader(bucket="datalake")
            keys = loader.list_objects(prefix="processed/books")

        assert len(keys) == 2
        assert all(".parquet" in k for k in keys)

    def test_load_raises_on_spark_error(self):
        """Une erreur Spark doit être propagée"""
        with patch("boto3.client") as mock_boto:
            mock_boto.return_value.list_buckets.return_value = {"Buckets": [{"Name": "datalake"}]}

            from src.loading.minio_loader import MinIOLoader
            loader = MinIOLoader(bucket="datalake")

            mock_df = MagicMock()
            mock_df.count.return_value = 5
            mock_df.write.mode.return_value.partitionBy.return_value.parquet.side_effect = Exception("S3 error")

            with pytest.raises(Exception, match="S3 error"):
                loader.load(mock_df)
