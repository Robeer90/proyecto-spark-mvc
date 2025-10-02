from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import count as F_count
from pyspark.sql.functions import min as F_min, max as F_max, countDistinct, datediff, lit, sequence, explode

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
    num_filas_original = df.count()
    df_sin_duplicados = df.dropDuplicates()
    num_filas_final = df_sin_duplicados.count()
    print("Filas eliminadas: ", num_filas_original - num_filas_final)
    columnas_utiles = [c for c in df_sin_duplicados.columns if df_sin_duplicados.select(c).distinct().count() > 1]
    df_final = df_sin_duplicados.select(columnas_utiles)
    print("Empresas con información: ", len(df_final.columns) - 1)
    return df_final
#Ej2-b
def rango_fechas(df):
    #Saca la fecha mas reciente y la mas antigua
    rangos = df.agg(F_min("Fecha").alias("fecha_min"), F_max("Fecha").alias("fecha_max")).collect()[0]
    fecha_min = rangos["fecha_min"]
    fecha_max = rangos["fecha_max"]
    #Se asegura de que no haya fechas repetidas
    dias_distintos = df.select("Fecha").distinct().count()
    #Se calcula la longitud del intervalo de fechas
    dias_totales = df.select((datediff(lit(fecha_max), lit(fecha_min)) + lit(1)).alias("dias_totales")).collect()[0]["dias_totales"]
    dias_faltantes = dias_totales - dias_distintos
    print(f"Fecha mínima: {fecha_min}")
    print(f"Fecha máxima: {fecha_max}")
    print(f"Nº de días cubiertos (distintos): {dias_distintos}")
    print(f"Nº de días totales en el rango: {dias_totales}")
    print(f"Días faltantes en el rango: {dias_faltantes}")
    return df