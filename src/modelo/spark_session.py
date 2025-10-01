from pyspark.sql import SparkSession

def create_spark_session(app_name="IBEX35", jar_path=None):
    builder = SparkSession.builder.appName(app_name)
    if jar_path:
        builder = builder.config("spark.driver.extraClassPath", jar_path)
    return builder.getOrCreate()
