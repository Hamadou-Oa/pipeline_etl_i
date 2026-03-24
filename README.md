# Pipeline ETL — Africa Tech Up Tour 

![Tests](https://img.shields.io/badge/Tests-29%20passing-brightgreen)
![Python](https://img.shields.io/badge/Python-3.10-green)
![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.9.3-red)

Pipeline ETL professionnel — Projet de fin de foramtion Data Engineering.

## Architecture
```
Web (books.toscrape.com)  ─┐
CSV (Google Drive)         ├─► Apache Spark ─► MinIO (datalake)
SQL (PostgreSQL Supabase)  ─┘         ↑
                                 Airflow DAG
```

## Stack technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Extraction | Python, BeautifulSoup, psycopg2 | 3.10 |
| Transformation | Apache Spark (PySpark) | 3.5.1 |
| Orchestration | Apache Airflow | 2.9.3 |
| Stockage | MinIO (S3-compatible) | Latest |
| Tests | pytest + coverage | 8.0.0 |
| CI/CD | GitHub Actions | — |

## Installation
```bash
git clone https://github.com/Hamadou-Oa/pipeline_etl_i.git
cd pipeline_etl_i
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
```

## Lancer le pipeline
```bash
make pipeline
```

Ou étape par étape :
```bash
make extract    # Extraction 3 sources
make transform  # Transformation Spark
make load       # Chargement MinIO
```

## Tests
```bash
make test       # 29 tests unitaires
make coverage   # Rapport de couverture
```

## Services
```bash
make start   # Démarrer Docker (MinIO + Airflow)
make stop    # Arrêter les services
make status  # État des services
```

| Service | URL | Login |
|---------|-----|-------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Airflow UI | http://localhost:8080 | admin / admin |

## Structure
```
pipeline_etl_i/
├── src/extraction/      ← Web + CSV + SQL
├── src/Transformation/  ← Spark
├── src/loading/         ← MinIO
├── src/utils/           ← Logger + SparkSession
├── dags/                ← DAG Airflow
├── .github/workflows/   ← CI/CD
├── docker-compose.yml
├── Makefile
└── requirements.txt
```

## Auteur

**Hamadou Oumarou Abdalahi** — Data Engineering  
Africa Tech Up Tour — 2025/2026
