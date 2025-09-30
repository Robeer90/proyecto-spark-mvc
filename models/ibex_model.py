# models/ibex_model.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

class IBEXModel:
    def __init__(self, file_path):
        # Crear SparkSession con el driver JDBC de MySQL
        self.spark = (SparkSession.builder
                      .appName("IBEX35")
                      .config("spark.driver.extraClassPath",
                              r"C:\Users\rogar\Downloads\mysql-connector-j-8.0.33\mysql-connector-j-8.0.33\mysql-connector-j-8.0.33.jar")
                      .getOrCreate())
        
        # Configuración JDBC
        self.url = "jdbc:mysql://localhost:3306/IBEX35"
        self.properties = {
            "user": "root",           # Cambia por tu usuario
            "password": "changeme",   # Cambia por tu contraseña
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Cargar CSV
        self.df = self.load_data(file_path)

    def load_data(self, file_path):
        df = self.spark.read.option("header", True) \
                            .option("sep", ";") \
                            .option("dateFormat", "dd/MM/yyyy") \
                            .csv(file_path)
        
        # Convertir columna Fecha de string a date
        df = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))
        
        # Mostrar primeras filas
        print("=== Primeras filas del DataFrame ===")
        df.show(5)
        
        return df

    def guardar_mysql(self, df, tabla, modo="overwrite"):
        """Guardar DataFrame en MySQL usando JDBC"""
        df.write.jdbc(url=self.url, table=tabla, mode=modo, properties=self.properties)
        print(f"Datos guardados en la tabla {tabla}")

