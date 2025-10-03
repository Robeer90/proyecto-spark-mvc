# utils/db_connection.py
def get_db_properties():
    return {
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "root",       # <-- cambia por tu usuario real
        "password": "changeme"  # <-- cambia por tu password real
    }

def get_jdbc_url():
    # Puedes añadir parámetros recomendados
    return (
        "jdbc:mysql://localhost:3306/IBEX35"
        "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
    )