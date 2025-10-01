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
    total_antes = df.count()
    #Limpiar los duplicados por fecha
    df_clean = df.dropDuplicates(subset=["Fecha"])
    total_despues = df_clean.count()
    eliminadas = total_antes - total_despues
    #Saca los nombres de las empresas
    empresas = [c for c in df_clean.columns if c != "Fecha"]
    #Hace un diccionario con el nombre de las empresas y el conteo
    cont = df_clean.agg(*[F_count(col(c)).alias(c) for c in empresas]).collect()[0].asDict()
    #Quita las empresas con valores nulos
    empresas_con_info = [c for c, cnt in cont.items() if cnt > 0]
    num_empresas = len(empresas_con_info)
    print(f"Duplicados eliminados (por Fecha): {eliminadas}")
    print(f"Empresas con información disponible: {num_empresas}")
    return df_clean
#Ej2-b
def rango_fechas(df):
    rangos = df.agg(F_min("Fecha").alias("fecha_min"), F_max("Fecha").alias("fecha_max")).collect()[0]
    fecha_min = rangos["fecha_min"]
    fecha_max = rangos["fecha_max"]
    dias_distintos = df.select("Fecha").distinct().count()
    dias_totales = df.select((datediff(lit(fecha_max), lit(fecha_min)) + lit(1)).alias("dias_totales")).collect()[0]["dias_totales"]
    dias_faltantes = dias_totales - dias_distintos
    print(f"Fecha mínima: {fecha_min}")
    print(f"Fecha máxima: {fecha_max}")
    print(f"Nº de días cubiertos (distintos): {dias_distintos}")
    print(f"Nº de días totales en el rango: {dias_totales}")
    print(f"Días faltantes en el rango: {dias_faltantes}")
    return df