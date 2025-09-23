from pyspark.sql import SparkSession

def get_db_connection(spark: SparkSession, url: str, table: str, user: str, password: str):
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .load()
    return df
