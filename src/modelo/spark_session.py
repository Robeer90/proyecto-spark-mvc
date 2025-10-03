# src/modelo/spark_session.py
from pyspark.sql import SparkSession

def create_spark_session(app_name="IBEX35", jar_path=None):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
    )
    if jar_path:
        builder = builder.config("spark.driver.extraClassPath", jar_path)
        # Si ejecutas en cluster local con ejecutores: podrías añadir también
        # builder = builder.config("spark.executor.extraClassPath", jar_path)
    return builder.getOrCreate()