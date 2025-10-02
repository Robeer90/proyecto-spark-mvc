# main.py
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).parent.resolve()
os.chdir(PROJECT_ROOT)  # opcional pero ayuda en Windows/OneDrive

DATA_PATH = PROJECT_ROOT / "data" / "ibex35.csv"
JAR_PATH  = PROJECT_ROOT / "libs" / "mysql-connector-j-8.0.33" / "mysql-connector-j-8.0.33.jar"

from src.modelo.spark_session import create_spark_session
from src.modelo import modificaciones  # si ya lo tienes

def main():
    spark = create_spark_session(jar_path=str(JAR_PATH))
    spark.sparkContext.setLogLevel("ERROR")  # menos ruido

    print("Ej1-a")
    # 1) Cargar CSV (RUTA RELATIVA correcta)
    df = (spark.read
          .option("header", True)
          .option("sep", ";")
          .csv(str(DATA_PATH)))  # <- NO pongas "ibex35.csv"; usa data/ibex35.csv

    # 2) Mostrar esquema y 6 filas antes de convertir
    df.printSchema()

    # 3) Aplicar la funcion
    df_new = modificaciones.convertir_fecha_ddMMyyyy(df)

    # 4) Verificar
    df_new.printSchema()
    df_new.show(6)

    print("Ej1-b")
    #1) Aplicar la funcion
    df_new1 = modificaciones.eliminamc(df_new)

    #2) Imprimir muestra
    df_new1.show(6)

    print("Ej2-a")
    #1) Aplicar la funcion
    df_new2 = modificaciones.elimina_duplicados(df_new1)
    print("Ej2-b")
    #1) Aplicar la funcion
    df_new2 = modificaciones.rango_fechas(df_new2)
    print("Ej3")
    #1) Aplicar funcion
    df_new3 = modificaciones.apartado3(df_new2)
    #2)Mostrar las 10 primeras lineas
    df_new3.show(10)
    #3)Mostrar las 100 primeras lineas
    df_new3.show(100)
    print("Ej4")
    df_new4 = modificaciones.Variacion_anual(df_new3)
    spark.stop()
if __name__ == "__main__":
    main()