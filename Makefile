.PHONY: help install start stop test coverage clean pipeline

help:
	@echo "Pipeline ETL — Africa Tech Up Tour"
	@echo "===================================="
	@echo "  make install    Installer les dependances"
	@echo "  make start      Demarrer les services Docker"
	@echo "  make stop       Arreter les services"
	@echo "  make pipeline   Lancer le pipeline complet"
	@echo "  make extract    Extraction 3 sources"
	@echo "  make transform  Transformation Spark"
	@echo "  make load       Chargement MinIO"
	@echo "  make test       Lancer les 29 tests"
	@echo "  make coverage   Rapport de couverture"
	@echo "  make clean      Nettoyer les fichiers temp"
	@echo "  make status     Etat des services Docker"

install:
	pip install --upgrade pip
	pip install -r requirements.txt

start:
	docker-compose up -d
	@echo "MinIO   -> http://localhost:9001"
	@echo "Airflow -> http://localhost:8080"

stop:
	docker-compose down

extract:
	python src/extraction/web_extractor.py
	python src/extraction/csv_extractor.py
	python src/extraction/sql_extractor.py

transform:
	python src/Transformation/spark_transformer.py

load:
	python src/loading/minio_loader.py

pipeline: extract transform load
	@echo "Pipeline ETL complet execute avec succes !"

test:
	pytest test_pipeline.py test_transformation_complete.py -v

coverage:
	pytest test_pipeline.py test_transformation_complete.py \
		--cov=src --cov-report=term-missing --cov-report=html

dag-validate:
	python -c "from dags.etl_pipeline_dag import dag; print('DAG valide:', dag.dag_id)"

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

status:
	docker-compose ps