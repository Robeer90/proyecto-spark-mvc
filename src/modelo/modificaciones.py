from pyspark.sql.functions import col

# Ej1-a: carga inicial y conversión de fecha
def convertir_fecha(df):
    return df.withColumn("Fecha", col("Fecha").cast("date"))

# Ej1-b: quitar sufijo .MC
def quitar_sufijo_mc(df):
    for c in df.columns:
        if c.endswith(".MC"):
            df = df.withColumnRenamed(c, c.replace(".MC", ""))
    return df
