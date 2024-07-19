import pymysql
import os
from dotenv import load_dotenv

load_dotenv()
HOST = os.environ.get("mysql_host")
USER = os.environ.get("mysql_user")
PASSWORD = os.environ.get("mysql_pass")
WAREHOUSE_DB_NAME = os.environ.get("mysql_db")

def setup_db_connection(host=HOST, user=USER, password=PASSWORD, warehouse_db_name=WAREHOUSE_DB_NAME):

    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=warehouse_db_name
    )
    cursor = connection.cursor()
    return connection, cursor

def create_db_tables(connection, cursor):
    create_sales_data_table = \
    """
        CREATE TABLE IF NOT EXISTS sales_data(
            customer_id int NOT NULL,
            purchase_date date,
            purchase_amount decimal(19,2),
            product_id varchar(10)
        );
    """
    create_customer_spend_table = \
    """
        CREATE TABLE IF NOT EXISTS customer_spend(
            customer_id int NOT NULL,
            average_spend_per_day decimal(19,2),
            total_spend decimal(19,2)
        );
    """
    create_customer_products_table = \
    """
        CREATE TABLE IF NOT EXISTS customer_products(
            customer_id int NOT NULL,
            product_id varchar(10),
            quantity int
        );
    """
    
    cursor.execute(create_sales_data_table)
    cursor.execute(create_customer_spend_table)
    cursor.execute(create_customer_products_table)
    connection.commit()
    cursor.close()
    connection.close()


myconnection, mycursor = setup_db_connection()
create_db_tables(myconnection, mycursor)