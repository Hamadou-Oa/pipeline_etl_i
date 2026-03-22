"""
Tests unitaires pour SparkTransformer
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


@pytest.fixture(scope="module")
def transformer():
    """Fixture SparkTransformer avec SparkSession mockée"""
    with patch("src.utils.spark_session.get_spark_session") as mock_spark:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("test_etl")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        mock_spark.return_value = spark

        from src.transformation.spark_transformer import SparkTransformer
        t = SparkTransformer()
        t.spark = spark
        yield t
        spark.stop()


class TestSparkTransformer:

    def test_transform_returns_spark_df(self, transformer):
        """transform() doit retourner un DataFrame Spark non vide"""
        df1 = pd.DataFrame({"title": ["Book A"], "price": [10.0], "source": ["web"]})
        df2 = pd.DataFrame({"title": ["Book B"], "price": [12.5], "source": ["csv"]})

        result = transformer.transform([df1, df2])
        assert result.count() == 2

    def test_transform_union_all_sources(self, transformer):
        """Les 3 sources doivent être unifiées"""
        df_web = pd.DataFrame({"title": ["W1"], "price": [5.0],  "source": ["web"]})
        df_csv = pd.DataFrame({"title": ["C1"], "price": [8.0],  "source": ["csv"]})
        df_sql = pd.DataFrame({"title": ["S1"], "price": [None], "source": ["sql"]})

        result = transformer.transform([df_web, df_csv, df_sql])
        assert result.count() == 3

    def test_clean_removes_null_title(self, transformer):
        """Les lignes sans titre doivent être supprimées"""
        df = pd.DataFrame({
            "title": ["Book A", None, "Book C"],
            "price": [10.0, 5.0, 8.0],
            "source": ["web", "web", "csv"]
        })
        spark_df = transformer.to_spark_df(df)
        result = transformer.clean_data(spark_df)
        assert result.count() == 2

    def test_normalize_price_is_double(self, transformer):
        """La colonne price doit être de type double après normalisation"""
        df = pd.DataFrame({"title": ["Book"], "price": ["10.5"], "source": ["web"]})
        spark_df = transformer.to_spark_df(df)
        result = transformer.normalize_data(spark_df)

        price_type = dict(result.dtypes)["price"]
        assert price_type == "double"

    def test_enrich_adds_price_category(self, transformer):
        """La colonne price_category doit être ajoutée"""
        df = pd.DataFrame({
            "title": ["Cheap", "Pricey"],
            "price": [5.0, 100.0],
            "source": ["web", "web"]
        })
        spark_df = transformer.to_spark_df(df)
        result = transformer.enrich_data(spark_df)

        assert "price_category" in result.columns
        rows = {r["title"]: r["price_category"] for r in result.collect()}
        assert rows["Cheap"] == "low"
        assert rows["Pricey"] == "premium"

    def test_transform_empty_list_raises(self, transformer):
        """Une liste vide doit lever une ValueError"""
        with pytest.raises(ValueError, match="vide"):
            transformer.transform([])

    def test_processed_at_column_added(self, transformer):
        """La colonne processed_at doit être présente après normalisation"""
        df = pd.DataFrame({"title": ["Book"], "price": [9.9], "source": ["csv"]})
        spark_df = transformer.to_spark_df(df)
        result = transformer.normalize_data(spark_df)
        assert "processed_at" in result.columns
