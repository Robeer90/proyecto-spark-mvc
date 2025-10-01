from utils.db_connection import get_db_properties, get_jdbc_url

def save_to_db(df, table_name, mode="overwrite"):
    url = get_jdbc_url()
    props = get_db_properties()
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=props)
