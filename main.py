from models.ibex_model import IBEXModel

def main():
    # Inicializar modelo (SparkSession + CSV)
    model = IBEXModel("data/ibex35.csv")

    # Mostrar primeras filas
    model.df.show(5)

    # Guardar DataFrame completo en MySQL
    model.guardar_mysql(model.df, "Datos2024")  # la tabla se crea/reescribe

if __name__ == "__main__":
    main()
