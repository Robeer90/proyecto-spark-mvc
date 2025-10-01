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
    df.show(6)

    # 3) Convertir Fecha y castear numéricas (si ya añadiste estas funciones)
    df = modificaciones.convertir_fecha_ddMMyyyy(df)
    df = modificaciones.castear_columnas_numericas(df, excluir=("Fecha",))

    # 4) Verificar
    df.printSchema()
    df.show(6)

if __name__ == "__main__":
    main()

