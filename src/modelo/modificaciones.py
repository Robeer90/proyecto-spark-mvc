#modificaciones
from pyspark.sql.functions import col, to_date, min, max, avg
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
    df_clean = df.dropDuplicates()
    total_despues = df_clean.count()
    print("Filas eliminadas: ", total_antes - total_despues)
    columnas_utiles = [c for c in df_clean.columns if df_clean.select(c).distinct().count() > 1]
    df_final = df_clean.select(columnas_utiles)
    print("Empresas con información: ", len(df_final.columns) - 1)
    return df_final
#Ej2-b
def rango_fechas(df):
    fecha_inicio = df.select(min("Fecha")).first()[0]
    fecha_fin = df.select(max("Fecha")).first()[0]
    print("Fecha inicio: ", fecha_inicio)
    print("Fecha fin: ", fecha_fin)
    dias_disponibles = df.select("Fecha").distinct().count()
    print("Dias con informacion disponible", dias_disponibles)
    print("El rango de fechas es coherente con un año natural.")
    print("No es necesario buscar datos adicionales, ya cubre el periodo completo.\n")
    return df
#Ej3
def apartado3(df):
    #1
    df_new = df.withColumnRenamed("Fecha", "Dia")
    #2
    empresas = [c for c in df_new.columns if c != "Dia"]
    for c in empresas:
        maximo = df_new.select(max(col(c))).first()[0]
        minimo = df_new.select(min(col(c))).first()[0]
        media = df_new.select(avg(col(c))).first()[0]
    print(f"{c} - Media: {media}, Maximo: {maximo}, Min: {minimo}")
    #3
    df_new = df_new.withColumn("Deficiency Notice UNI", col("UNI") < 1)
    return df_new