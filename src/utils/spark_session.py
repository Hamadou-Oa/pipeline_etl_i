# pragma: no cover
import os
import sys
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_spark_session(app_name="ETL_Pipeline"):

    java17 = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.18.8-hotspot"
    java17_bin = java17 + "\\bin"

    os.environ["JAVA_HOME"] = java17
    os.environ["PATH"] = java17_bin + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.pop("SPARK_HOME", None)

    logger.info(f"Initialisation SparkSession : {app_name}")
    logger.info(f"Java : {java17}")
    logger.info(f"Python : {sys.executable}")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName(app_name)
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.python.worker.reuse", "true")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    logger.info(f"SparkSession créée : version {spark.version}")
    return spark