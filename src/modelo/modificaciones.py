from pyspark.sql.functions import col, to_date

# Ej1-a: carga inicial y conversión de fecha
def convertir_fech_ddMMyyyy(df):
    return df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))