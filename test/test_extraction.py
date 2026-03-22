"""
test_extraction.py
Tests unitaires pour les 3 extracteurs
Couverture cible : > 80%
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, Mock

#  TESTS — WebExtractor

class TestWebExtractor:
    """Tests pour le scraping de books.toscrape.com"""

    def test_extract_returns_dataframe(self):
        """Le résultat doit être un DataFrame non vide"""
        from src.extraction.web_extractor import WebExtractor

        html_sample = """
        <html><body>
          <article class="product_pod">
            <h3><a title="A Light in the Attic" href="...">A Light...</a></h3>
            <p class="price_color">Â£51.77</p>
            <p class="availability">In stock</p>
            <p class="star-rating Three"></p>
          </article>
        </body></html>
        """
        mock_resp = Mock()
        mock_resp.text = html_sample
        mock_resp.raise_for_status = Mock()

        with patch("requests.Session.get", return_value=mock_resp):
            extractor = WebExtractor(max_pages=1, delay=0)
            with patch.object(extractor, "_get_next_page_url", return_value=None):
                df = extractor.extract()

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "title" in df.columns
        assert "price" in df.columns
        assert "source" in df.columns

    def test_extract_source_is_web(self):
        """La colonne source doit valoir 'web'"""
        from src.extraction.web_extractor import WebExtractor

        html_sample = """
        <article class="product_pod">
          <h3><a title="Test Book" href="#">Test</a></h3>
          <p class="price_color">£10.00</p>
          <p class="availability">In stock</p>
          <p class="star-rating Four"></p>
        </article>
        """
        mock_resp = Mock()
        mock_resp.text = f"<html><body>{html_sample}</body></html>"
        mock_resp.raise_for_status = Mock()

        with patch("requests.Session.get", return_value=mock_resp):
            extractor = WebExtractor(max_pages=1, delay=0)
            with patch.object(extractor, "_get_next_page_url", return_value=None):
                df = extractor.extract()

        assert (df["source"] == "web").all()

    def test_price_is_positive_float(self):
        """Les prix doivent être des nombres positifs"""
        from src.extraction.web_extractor import WebExtractor

        html_sample = """
        <article class="product_pod">
          <h3><a title="Cheap Book" href="#">Cheap</a></h3>
          <p class="price_color">Â£9.99</p>
          <p class="availability">In stock</p>
          <p class="star-rating Two"></p>
        </article>
        """
        mock_resp = Mock()
        mock_resp.text = f"<html><body>{html_sample}</body></html>"
        mock_resp.raise_for_status = Mock()

        with patch("requests.Session.get", return_value=mock_resp):
            extractor = WebExtractor(max_pages=1, delay=0)
            with patch.object(extractor, "_get_next_page_url", return_value=None):
                df = extractor.extract()

        assert df["price"].dtype in ["float64", "float32", "float"]
        assert df["price"].min() > 0

    def test_network_error_is_handled(self):
        """Une erreur réseau doit retourner un DataFrame vide (pas lever d'exception)"""
        import requests
        from src.extraction.web_extractor import WebExtractor

        with patch("requests.Session.get", side_effect=requests.RequestException("timeout")):
            extractor = WebExtractor(max_pages=1, delay=0)
            df = extractor.extract()

        assert isinstance(df, pd.DataFrame)


#  TESTS — CSVExtractor
class TestCSVExtractor:
    """Tests pour l'extraction CSV"""

    def test_gdrive_url_conversion(self):
        """Une URL Google Drive doit être convertie en lien direct"""
        from src.extraction.csv_extractor import CSVExtractor

        gdrive_url = "https://drive.google.com/file/d/1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD/view"
        extractor = CSVExtractor(csv_url=gdrive_url, output_dir="/tmp")

        assert "uc?export=download" in extractor.csv_url
        assert "1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD" in extractor.csv_url

    def test_direct_url_unchanged(self):
        from src.extraction.csv_extractor import CSVExtractor

        direct_url = "https://example.com/data.csv"
        extractor = CSVExtractor(csv_url=direct_url, output_dir="/tmp")
        assert extractor.csv_url == direct_url

    def test_extract_returns_dataframe(self, tmp_path):
        from src.extraction.csv_extractor import CSVExtractor

        # Création d'un CSV de test
        csv_content = b"name,age,city\nAlice,30,Paris\nBob,25,Lyon\n"

        mock_resp = Mock()
        mock_resp.raise_for_status = Mock()
        mock_resp.iter_content = Mock(return_value=[csv_content])
        mock_resp.cookies = {}

        with patch("requests.Session.get", return_value=mock_resp):
            extractor = CSVExtractor(csv_url="https://example.com/data.csv", output_dir=str(tmp_path))
            df = extractor.extract()

        assert isinstance(df, pd.DataFrame)
        assert "source" in df.columns
        assert (df["source"] == "csv").all()
        assert len(df) == 2

    def test_extract_raises_on_http_error(self, tmp_path):
        """Une erreur HTTP doit propager une exception"""
        import requests
        from src.extraction.csv_extractor import CSVExtractor

        mock_resp = Mock()
        mock_resp.raise_for_status = Mock(side_effect=requests.HTTPError("404"))
        mock_resp.iter_content = Mock(return_value=[])
        mock_resp.cookies = {}

        with patch("requests.Session.get", return_value=mock_resp):
            extractor = CSVExtractor(csv_url="https://example.com/bad.csv", output_dir=str(tmp_path))
            with pytest.raises(requests.HTTPError):
                extractor.extract()



#  TESTS — SQLExtractor
class TestSQLExtractor:
    """Tests pour l'extraction PostgreSQL (Supabase)"""

    def test_url_normalization(self):
        """L'URL postgresql:// doit être convertie en postgresql+psycopg2://"""
        from src.extraction.sql_extractor import SQLExtractor

        url = "postgresql://user:pass@host:5432/db"
        extractor = SQLExtractor(db_url=url, query="SELECT 1")
        assert "psycopg2" in extractor.db_url

    def test_already_normalized_url(self):
        """Une URL déjà normalisée ne doit pas être modifiée"""
        from src.extraction.sql_extractor import SQLExtractor

        url = "postgresql+psycopg2://user:pass@host:5432/db"
        extractor = SQLExtractor(db_url=url, query="SELECT 1")
        assert extractor.db_url == url

    def test_extract_returns_dataframe(self):
        """L'extraction SQL doit retourner un DataFrame avec source='sql'"""
        from src.extraction.sql_extractor import SQLExtractor

        mock_df = pd.DataFrame({"id": [1, 2], "email": ["a@b.com", "c@d.com"]})

        with patch("pandas.read_sql", return_value=mock_df):
            with patch("sqlalchemy.create_engine") as mock_engine:
                mock_conn = MagicMock()
                mock_engine.return_value.connect.return_value.__enter__ = Mock(return_value=mock_conn)
                mock_engine.return_value.connect.return_value.__exit__ = Mock(return_value=False)

                extractor = SQLExtractor(
                    db_url="postgresql+psycopg2://user:pass@host/db",
                    query="SELECT * FROM customers"
                )
                df = extractor.extract()

        assert isinstance(df, pd.DataFrame)
        assert "source" in df.columns

    def test_retry_on_connection_error(self):
        """Le retry doit être tenté en cas d'OperationalError"""
        from src.extraction.sql_extractor import SQLExtractor
        from sqlalchemy.exc import OperationalError

        with patch("sqlalchemy.create_engine") as mock_engine:
            mock_engine.return_value.connect.side_effect = OperationalError(
                "conn", {}, Exception("refused")
            )
            extractor = SQLExtractor(
                db_url="postgresql+psycopg2://user:pass@host/db",
                query="SELECT 1",
                max_retries=2
            )
            with patch("time.sleep"):
                with pytest.raises(OperationalError):
                    extractor.extract()
