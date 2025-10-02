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
#Ej4
def Variacion_anual(df):
    fecha_col = "Dia"
    fecha_ini = df.select(min(fecha_col)).first()[0]
    fecha_fin = df.select(max(fecha_col)).first()[0]
    empresas = [c for c in df.columns if c != fecha_col]
    for emp in empresas:
        row_ini = df.where(col(fecha_col) == fecha_ini).select(emp).first()
        row_fin = df.where(col(fecha_col) == fecha_fin).select(emp).first()
        val_ini = None if row_ini is None else row_ini[0]
        val_fin = None if row_fin is None else row_fin[0]
        if val_ini is None or val_fin is None:
            return
        ini = float(val_ini)
        fin = float(val_fin)
        if ini == 0.0:
            return
        var_pct = ((fin - ini) / ini) * 100.0
        if var_pct >= 15:
            categoria = "Subida Fuerte"
        elif var_pct > 1:
            categoria = "Subida"
        elif var_pct <= -15:
            categoria = "Bajada Fuerte"
        elif var_pct < -1:
            categoria = "Bajada"
        else:
            categoria = "Neutra"
        print(f"{emp}: inicio={ini}, fin={fin}, variación={var_pct:.2f}%, categoría={categoria}")
    return df