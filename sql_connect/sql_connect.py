"""
created on Mar 7, 2023

"""
import pymysql
import getpass
from sqlalchemy import create_engine


def SQL_connect(schema, host = 'localhost', port = 3306):
    print('Conectando MySQL')
    print('Introduce el usuario:')
    user = input()
    print('Introduce la contraseña')
    password = getpass.getpass()
    try:
        connection = pymysql.connect(host=host,
                    user=user,
                    password=password,
                    db= schema,
                    port = port
            )
        print('Conexion establecida!')
    except Exception as e:
        print("No se ha podido establecer la conexión con MySQL. El proceso se ha abortado")
        print(e)
        exit()
    db_data = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    engine = create_engine(db_data, encoding='latin1')
    cursor = connection.cursor()

    return (db_data, engine, cursor)
    