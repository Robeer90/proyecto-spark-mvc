from pathlib import Path
import os, sys

# --- Ajustes de entorno ---
PROJECT_ROOT = Path(__file__).parent.resolve()
os.chdir(PROJECT_ROOT)  # garantiza ejecución desde raíz

DATA_PATH = PROJECT_ROOT / "data" / "ibex35.csv"
JAR_PATH  = PROJECT_ROOT / "libs" / "mysql-connector-j-8.0.33" / "mysql-connector-j-8.0.33.jar"

# --- Imports del proyecto ---
from src.modelo.spark_session import create_spark_session
from src.modelo import modificaciones, conexion

def main():
    # Crear SparkSession
    spark = create_spark_session(jar_path=str(JAR_PATH))

    # Ej1-a: carga del CSV
    print("Ej1-a")
    df = (spark.read
          .option("header", True)
          .option("sep", ";")
          .option("dateFormat", "dd/MM/yyyy")
          .csv(str(DATA_PATH)))

    df.printSchema()
    df.show(6)

    # Aplicar función de conversión de fecha
    df = modificaciones.convertir_fecha(df)
    df.printSchema()
    df.show(6)

    # Ej1-b: quitar sufijo .MC
    print("Ej1-b")
    df = modificaciones.quitar_sufijo_mc(df)
    df.show(6)

    # Guardar versión inicial en la BD
    conexion.save_to_db(df, "Datos2024_raw")

if __name__ == "__main__":
    main()
