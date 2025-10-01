def get_db_properties():
    return {
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "root",         # cámbialo por tu usuario
        "password": "changeme"
    }

def get_jdbc_url():
    return "jdbc:mysql://localhost:3306/IBEX35"
