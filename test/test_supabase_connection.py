"""
test_supabase_connection.py
Script de test rapide pour vérifier la connexion Supabase.
"""

import sys
import pandas as pd
from sqlalchemy import create_engine, text

MOT_DE_PASSE = "uYHtqwr91Zj1g8hg"  # ← mon mot de passe Supabase

DB_URL = f"postgresql+psycopg2://postgres.bqaznvlbzgckvmofsiqk:uYHtqwr91Zj1g8hg@aws-1-eu-west-1.pooler.supabase.com:6543/postgres"
def test_connection():
    print("=" * 50)
    print("  Test connexion Supabase")
    print("=" * 50)

    try:
        engine = create_engine(
            DB_URL,
            connect_args={"sslmode": "require"},
            pool_pre_ping=True,
        )
        with engine.connect() as conn:
            # Connexion simple
            result = conn.execute(text("SELECT 1 AS ok"))
            print("Connexion réussie !")

            # Compter les clients
            result = conn.execute(text("SELECT COUNT(*) AS total FROM customers"))
            total = result.fetchone()[0]
            print(f"Table customers : {total} lignes")

            # Affichagedd des 3 lignes
            df = pd.read_sql(text("SELECT * FROM customers LIMIT 3"), conn)
            print("\nAperçu des données :")
            print(df.to_string(index=False))

    except Exception as e:
        print(f"Erreur : {e}")
        print("\nVérifie :")
        print("  1. Mon mot de passe qui est dans ce fichier")
        print("  2. la table customers qui existe")
        sys.exit(1)

if __name__ == "__main__":
    test_connection()