from pyspark.sql.functions import col, to_date

# Ej1-a: carga inicial y conversión de fecha
def convertir_fecha_ddMMyyyy(df):
    return df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))

#Ej1-b: elimina el sufijo .MC
def eliminamc(df):
    df_new = df
    for c in df.columns:
        if c.endswith(".MC"):
            df_new = df_new.withColumnRenamed(c, c[:-3])
    return df_new