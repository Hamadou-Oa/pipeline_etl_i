"""
Extraction des données depuis PostgreSQL Cloud (Supabase / Neon.tech)
- Connexion SSL sécurisée (en cloud)
- Retry automatique sur échec de connexion
- Compatible SQLAlchemy 2.1 (psycopg2)
"""

import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Requête sur la table customers (Supabase)
DEFAULT_QUERY = """
    SELECT
        id,
        first_name,
        last_name,
        email,
        country,
        created_at
    FROM customers
    LIMIT 1000
"""


class SQLExtractor:
    """
    Extrait des données depuis une base PostgreSQL (cloud ou locale).
    Compatible Supabase, Neon.tech, et PostgreSQL standard.
    """

    def __init__(self, db_url: str, query: str = DEFAULT_QUERY, max_retries: int = 3):
        """
        Args:
            db_url      : URL de connexion PostgreSQL
                          Format : postgresql+psycopg2://user:password@host:port/dbname
            query       : Requête SQL à exécuter
            max_retries : Nombre de tentatives en cas d'échec
        """
        self.db_url = self._normalize_url(db_url)
        self.query = query
        self.max_retries = max_retries

    def _normalize_url(self, url: str) -> str:
        if url.startswith("postgresql://") and "+psycopg2" not in url:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
            logger.info("URL normalisée pour SQLAlchemy 2.x (psycopg2)")
        return url

    def _create_engine(self):
        """Création du moteur SQLAlchemy avec SSL pour le cloud."""
        return create_engine(
            self.db_url,
            connect_args={"sslmode": "require"},   
            pool_pre_ping=True,                      
            pool_recycle=300,                        
        )

    def extract(self) -> pd.DataFrame:
        """
        Exécution de la requête SQL et retourne un DataFrame.
        Retry automatique jusqu'à max_retries fois.

        Returns:
            pd.DataFrame avec colonne 'source' = 'sql'
        """
        attempt = 0
        last_error = None

        while attempt < self.max_retries:
            attempt += 1
            try:
                logger.info(f"Connexion PostgreSQL (tentative {attempt}/{self.max_retries})")
                engine = self._create_engine()

                with engine.connect() as conn:
                    df = pd.read_sql(text(self.query), conn)

                df["source"] = "sql"
                logger.info(f"Extraction SQL réussie : {df.shape[0]} lignes, {df.shape[1]} colonnes")
                return df

            except OperationalError as e:
                last_error = e
                wait = 2 ** attempt  
                logger.warning(f"Échec connexion (tentative {attempt}) — retry dans {wait}s : {e}")
                time.sleep(wait)

            except Exception as e:
                logger.error(f"Erreur SQL inattendue : {e}")
                raise

        logger.error(f"Extraction SQL échouée après {self.max_retries} tentatives")
        raise last_error


if __name__ == "__main__":
    DB_URL = "postgresql://postgres:MON_MOT_DE_PASSE@db.TON_ID.supabase.co:5432/postgres"
    QUERY = "SELECT * FROM customers LIMIT 10"

    extractor = SQLExtractor(DB_URL, QUERY)
    df = extractor.extract()
    print(df)
