from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

class IBEXModel:
    def __init__(self, file_path):
        self.file_path = file_path
        self.spark = SparkSession.builder.appName("IBEX35").getOrCreate()
        self.df = self.load_data()

    def load_data(self):
        df = self.spark.read.option("header", True) \
                            .option("sep", ";") \
                            .option("dateFormat", "dd/MM/yyyy") \
                            .csv(self.file_path)

        # Convertir columna Fecha a tipo date
        df = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))

        # Si hay otras columnas numéricas, convertirlas (Precio, Volumen...)
        # df = df.withColumn("Precio", col("Precio").cast("float"))

        return df

