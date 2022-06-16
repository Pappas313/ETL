from typing import Dict, Optional

import mysql.connector
from mysql.connector import Error


def create_connection(host_name, user_name, user_password, database_name: Optional[str] = None):
    connection = None
    try:
        params: Dict = {
            'host': host_name,
            'user': user_name,
            'passwd': user_password,
        }
        if database_name is not None:
            params['database'] = database_name
        connection = mysql.connector.connect(**params)
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


def create_database(connection, query) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(query)
    except Error as e:
        print(f"The error '{e}' occurred")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
    except Error as e:
        print(f"The error '{e}' occurred")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
