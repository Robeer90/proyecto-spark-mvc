from spark_session import create_spark_session
from models.db_connection import get_db_connection

def main():
    spark = create_spark_session()
    url = "jdbc:postgresql://localhost:5432/mi_base"
    table = "mi_tabla"
    user = "usuario"
    password = "contraseña"
    
    df = get_db_connection(spark, url, table, user, password)
    df.show()

if __name__ == "__main__":
    main()
