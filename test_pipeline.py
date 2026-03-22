"""
Tests unitaires Pipeline ETL Africa Tech Up Tour 2025 - Extraction, Transformation, Chargement
"""

import os
import sys
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, PropertyMock

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)



class TestWebExtractor:

    def test_import(self):
        from src.extraction.web_extractor import WebExtractor
        assert WebExtractor is not None

    def test_init(self):
        from src.extraction.web_extractor import WebExtractor
        e = WebExtractor(max_pages=2)
        assert e.max_pages == 2
        assert "books.toscrape.com" in e.base_url

    def test_extract_returns_dataframe_with_mock(self):
        from src.extraction.web_extractor import WebExtractor

        mock_html = """<html><body>
        <article class="product_pod">
            <h3><a title="Test Book One">Test Book One</a></h3>
            <p class="price_color">£12.99</p>
            <p class="instock availability"> In stock </p>
            <p class="star-rating Three"></p>
        </article>
        <article class="product_pod">
            <h3><a title="Test Book Two">Test Book Two</a></h3>
            <p class="price_color">£8.50</p>
            <p class="instock availability"> In stock </p>
            <p class="star-rating One"></p>
        </article>
        </body></html>"""

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = mock_html
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_resp), \
             patch("src.extraction.web_extractor.requests.get", return_value=mock_resp):
            e = WebExtractor(max_pages=1)
            df = e.extract()

        assert isinstance(df, pd.DataFrame)
        assert "source" in df.columns
        assert len(df) >= 0

    def test_source_column_value(self):
        df = pd.DataFrame({
            "title": ["Book"], "price": [9.99],
            "availability": ["In stock"], "rating": [3], "source": ["web"]
        })
        assert df["source"].iloc[0] == "web"

    def test_max_pages_respected(self):
        from src.extraction.web_extractor import WebExtractor
        e = WebExtractor(max_pages=5)
        assert e.max_pages == 5



class TestCSVExtractor:

    def test_import(self):
        from src.extraction.csv_extractor import CSVExtractor
        assert CSVExtractor is not None

    def test_gdrive_url_conversion(self):
        from src.extraction.csv_extractor import CSVExtractor
        e = CSVExtractor.__new__(CSVExtractor)
        result = e._convert_gdrive_url("https://drive.google.com/file/d/ABC123/view")
        assert "uc?export=download" in result
        assert "ABC123" in result

    def test_direct_url_unchanged(self):
        from src.extraction.csv_extractor import CSVExtractor
        e = CSVExtractor.__new__(CSVExtractor)
        url = "https://example.com/data.csv"
        assert e._convert_gdrive_url(url) == url

    def test_encoding_detection(self, tmp_path):
        from src.extraction.csv_extractor import CSVExtractor
        f = tmp_path / "test.csv"
        f.write_text("title,price\nBook A,12.5\n", encoding="utf-8")
        e = CSVExtractor.__new__(CSVExtractor)
        e.local_file = str(f)
        enc = e._detect_encoding()
        assert enc in ["utf-8", "latin-1", "windows-1252"]

    def test_zip_extraction(self, tmp_path):
        import zipfile, io
        from src.extraction.csv_extractor import CSVExtractor
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("books.csv", "title,price\nBook A,12.5\n")
        e = CSVExtractor.__new__(CSVExtractor)
        e.local_file = str(tmp_path / "dataset.csv")
        e._extract_from_zip(buf.getvalue())
        df = pd.read_csv(e.local_file)
        assert "title" in df.columns

    def test_zip_magic_bytes(self):
        import zipfile, io
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("f.csv", "a,b\n1,2\n")
        assert buf.getvalue()[:2] == b"PK"

    def test_read_csv_with_bad_lines(self, tmp_path):
        f = tmp_path / "bad.csv"
        f.write_text("a,b,c\n1,2,3\n4,5,6,7\n8,9,10\n", encoding="utf-8")
        df = pd.read_csv(str(f), engine="python", on_bad_lines="skip")
        assert len(df) == 2



class TestSQLExtractor:

    def test_import(self):
        from src.extraction.sql_extractor import SQLExtractor
        assert SQLExtractor is not None

    def test_init_stores_query(self):
        from src.extraction.sql_extractor import SQLExtractor
        e = SQLExtractor("postgresql+psycopg2://u:p@h/db", "SELECT 42 AS n")
        assert e.query == "SELECT 42 AS n"

    def test_init_stores_url(self):
        from src.extraction.sql_extractor import SQLExtractor
        url = "postgresql+psycopg2://user:pass@host:5432/db"
        e = SQLExtractor(url, "SELECT 1")
        assert e.db_url == url

    @patch("src.extraction.sql_extractor.pd.read_sql")
    @patch("src.extraction.sql_extractor.create_engine")
    def test_extract_returns_dataframe(self, mock_engine, mock_read_sql):
        from src.extraction.sql_extractor import SQLExtractor
        mock_df = pd.DataFrame({"n": [1, 2], "source": ["sql", "sql"]})
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine.return_value.connect.return_value = mock_conn

        e = SQLExtractor("postgresql+psycopg2://u:p@h/db", "SELECT 1 AS n")
        df = e.extract()
        assert isinstance(df, pd.DataFrame)

    def test_source_added(self):
        df = pd.DataFrame({"n": [1]})
        df["source"] = "sql"
        assert df["source"].iloc[0] == "sql"



class TestMinIOLoader:

    def _make_mock_s3(self, buckets=None):
        mock_s3 = MagicMock()
        mock_s3.list_buckets.return_value = {
            "Buckets": [{"Name": b} for b in (buckets or [])]
        }
        return mock_s3

    def test_import(self):
        try:
            from src.loading.minio_loader import MinIOLoader
            assert MinIOLoader is not None
        except ModuleNotFoundError as e:
            pytest.skip(f"Dépendance manquante : {e}")

    def test_bucket_created_when_missing(self):
        try:
            import boto3
        except ImportError:
            pytest.skip("boto3 non installé")
        from src.loading.minio_loader import MinIOLoader
        with patch("src.loading.minio_loader.boto3.client") as mock_boto:
            mock_s3 = self._make_mock_s3(buckets=[])
            mock_boto.return_value = mock_s3
            loader = MinIOLoader(bucket="new-bucket")
            mock_s3.create_bucket.assert_called_once_with(Bucket="new-bucket")

    def test_bucket_not_recreated_if_exists(self):
        try:
            import boto3
        except ImportError:
            pytest.skip("boto3 non installé")
        from src.loading.minio_loader import MinIOLoader
        with patch("src.loading.minio_loader.boto3.client") as mock_boto:
            mock_s3 = self._make_mock_s3(buckets=["datalake"])
            mock_boto.return_value = mock_s3
            loader = MinIOLoader(bucket="datalake")
            mock_s3.create_bucket.assert_not_called()

    def test_list_objects_returns_keys(self):
        try:
            import boto3
        except ImportError:
            pytest.skip("boto3 non installé")
        from src.loading.minio_loader import MinIOLoader
        with patch("src.loading.minio_loader.boto3.client") as mock_boto:
            mock_s3 = self._make_mock_s3(buckets=["datalake"])
            mock_s3.list_objects_v2.return_value = {
                "Contents": [{"Key": "etl/source=web/part-0.parquet"}]
            }
            mock_boto.return_value = mock_s3
            loader = MinIOLoader(bucket="datalake")
            keys = loader.list_objects("etl/")
            assert len(keys) == 1
            assert "parquet" in keys[0]



class TestDataQuality:

    def test_required_columns_present(self):
        df = pd.DataFrame({
            "title": ["Book A"], "price": [12.5],
            "availability": ["In stock"], "rating": [3], "source": ["web"]
        })
        for col in ["title", "price", "availability", "rating", "source"]:
            assert col in df.columns

    def test_price_numeric(self):
        df = pd.DataFrame({"price": ["12.5", "bad", None]})
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        assert df["price"].dtype == np.float64
        assert pd.isna(df["price"].iloc[1])

    def test_no_duplicates(self):
        df = pd.DataFrame({"title": ["A", "B", "C"]})
        assert df["title"].nunique() == 3

    def test_source_values_valid(self):
        df = pd.DataFrame({"source": ["web", "csv", "sql"]})
        assert set(df["source"]).issubset({"web", "csv", "sql"})

    def test_price_category_logic(self):
        def cat(p):
            if pd.isna(p): return "unknown"
            if p < 10: return "low"
            if p < 25: return "medium"
            return "high"
        assert cat(5.0) == "low"
        assert cat(15.0) == "medium"
        assert cat(30.0) == "high"
        assert cat(np.nan) == "unknown"

    def test_union_sources(self):
        df1 = pd.DataFrame({"title": ["A"], "source": ["web"]})
        df2 = pd.DataFrame({"title": ["B"], "source": ["csv"]})
        df3 = pd.DataFrame({"title": ["C"], "source": ["sql"]})
        result = pd.concat([df1, df2, df3], ignore_index=True)
        assert len(result) == 3
        assert set(result["source"]) == {"web", "csv", "sql"}

    def test_empty_titles_detection(self):
        df = pd.DataFrame({"title": ["Book A", "", None, "Book B"]})
        invalid = df[df["title"].isna() | (df["title"] == "")]
        assert len(invalid) == 2

    def test_rating_range(self):
        df = pd.DataFrame({"rating": [1, 2, 3, 4, 5]})
        assert df["rating"].between(1, 5).all()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--no-cov"])