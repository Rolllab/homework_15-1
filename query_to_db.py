from connect_to_db import execute_query

def create_database(db_name):
    """
    Создает новую базу данных

    :param db_name: Имя для базы данных
    :return: None
    """
    query = f"CREATE DATABASE \"{db_name}\";"
    result = execute_query(query=query, autocommit=True)
    return result

def drop_database(db_name):
    """
    Удаляет базу данных

    :param db_name: Имя удаляемой базы данных
    :return: None
    """
    query = f"DROP DATABASE \"{db_name}\";"
    result = execute_query(query=query, autocommit=True)
    return result

def create_table(table_name: str, columns: dict, constraints=None, db_name='postgres'):
    """
    Создает таблицу в указанной базе данных

    :param table_name: Имя таблицы в этой базе данных
    :param columns: В словаре содержится название колонки и ее характеристики
    :param constraints: Возможные ограничения колонки
    :param db_name: Имя базы данных
    :return: None
    """

    column_definitions = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])

    if constraints:
        constraint_definitions = ", ".join(constraints)
        table_definition = f"({column_definitions}, {constraint_definitions})"
    else:
        table_definition = f"({column_definitions})"

    query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" {table_definition};"
    print('query = ', query)
    result = execute_query(query=query, database=db_name, autocommit=True)
    return result

def insert_into_table(db_name: str, table_name: str, data: dict):
    """
    Заполняет таблицу данными

    :param db_name: Имя базы данных
    :param table_name: Имя таблицы в этой базе данных
    :param data: Сами данные в виде словаря
    :return: None
    """
    columns = ', '.join(data.keys())
    values = tuple([v for v in data.values()])

    query = f'INSERT INTO {table_name} ({columns}) VALUES {values}'
    result = execute_query(query=query, database=db_name, autocommit=True)
    return result

def cat_all_table(db_name, table_name):
    """
    Запрашивает у базы данных данные по таблице и возвращает ее

    :param db_name: Имя базы данных
    :param table_name: Имя таблицы в этой базе данных
    :return: возвращает данные всей таблицы
    """
    query = f"SELECT * FROM \"{table_name}\""
    result = execute_query(query=query, database=db_name)
    return result

