# main.py
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).parent.resolve()
os.chdir(PROJECT_ROOT)  

DATA_PATH = PROJECT_ROOT / "data" / "ibex35.csv"
JAR_PATH  = PROJECT_ROOT / "libs" / "mysql-connector-j-8.0.33" / "mysql-connector-j-8.0.33.jar"

from src.modelo.spark_session import create_spark_session
from src.modelo import modificaciones
from src.modelo.conexion import save_to_db  

from pyspark.sql.functions import col  

def sanitize_for_sql(df):
    """Evita puntos/espacios en nombres de columnas (MySQL/JDBC)."""
    safe_cols = [c.strip().replace(" ", "_").replace(".", "_").replace("/", "_") for c in df.columns]
    return df.toDF(*safe_cols)

def main():
    spark = create_spark_session(jar_path=str(JAR_PATH))
    spark.sparkContext.setLogLevel("ERROR")

    print("Ej1-a")
    # 1) Cargar CSV (ruta relativa)
    df = (spark.read
          .option("header", True)
          .option("sep", ";")
          .csv(str(DATA_PATH)))

    # RAW -> a BD 
    df_raw_sql = sanitize_for_sql(df)
    save_to_db(df_raw_sql, table_name="Datos2024", mode="overwrite")
    print("OK -> MySQL: Datos2024 (RAW)")

    # 2) Mostrar esquema y 6 filas antes de convertir fecha
    df.printSchema()

    # 3) Ej1-a convertir fecha
    df_new = modificaciones.convertir_fecha_ddMMyyyy(df)

    # 4) Verificar
    df_new.printSchema()
    df_new.show(6)

    print("Ej1-b")
    df_new1 = modificaciones.eliminamc(df_new)
    df_new1.show(6)

    print("Ej2-a")
    df_new2 = modificaciones.elimina_duplicados(df_new1)

    print("Ej2-b")
    df_new2 = modificaciones.rango_fechas(df_new2)

    # TRATADO SIN COLUMNAS NUEVAS
    df_tratado_sql = sanitize_for_sql(df_new2)
    save_to_db(df_tratado_sql, table_name="Datos2024_tratado", mode="overwrite")
    print("OK -> MySQL: Datos2024_tratado (tratado SIN columnas nuevas)")

    print("Ej3")
    df_new3 = modificaciones.apartado3(df_new2)
    df_new3.show(10)
    df_new3.show(100)

    print("Ej4")
    df_new4 = modificaciones.Variacion_anual(df_new3)

    print("Ej5")
    df_new5 = modificaciones.cuartil(df_new4)
    df_new5.show(1, truncate=False)
    df_new5.select("AENA", "AENACuartil", "BBVA", "BBVACuartil").show(20, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
