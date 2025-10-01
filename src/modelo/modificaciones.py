from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import col, count as F_count

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
#Ej2-a: 
def elimina_duplicados(df):
    total_antes = df.count()
    df_clean = df.dropDuplicates(subset=["Fecha"])
    total_despues = df_clean.count()
    eliminadas = total_antes - total_despues
    empresas = [c for c in df_clean.columns if c != "Fecha"]
    cont = df_clean.agg(*[F_count(col(c)).alias(c) for c in empresas]).collect()[0].asDict()
    empresas_con_info = [c for c, cnt in cont.items() if cnt > 0]
    num_empresas = len(empresas_con_info)
    print(f"Duplicados eliminados (por Fecha): {eliminadas}")
    print(f"Empresas con información disponible: {num_empresas}")
    return df_clean