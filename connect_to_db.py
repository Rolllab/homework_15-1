import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Параметры подключения
HOST = os.getenv('HOST')                    # IP или доменное имя сервера
USER = os.getenv('USER_DB')                 # Пользователь PostgreSQL
PASSWORD = os.getenv('PASSWORD')            # Пароль пользователя
PORT = os.getenv('PORT')                    # Порт по умолчанию для PostgreSQL


class DatabaseConnection:
    """Управляет соединением с базой данных и курсором."""

    def __init__(self, database='postgres', autocommit=False):
        self.database = database
        self.autocommit = autocommit
        self.connect = None
        self.cursor = None

    def __enter__(self):
        """Устанавливает соединение и создает курсор при входе в блок 'with'."""
        try:
            self.connect = psycopg2.connect(
                host=HOST,
                database=self.database,
                user=USER,
                password=PASSWORD,
                port=PORT
            )
            self.connect.autocommit = self.autocommit
            self.cursor = self.connect.cursor()
            print("Подключение к базе данных установлено.")
            return self.cursor

        except Exception as e:
            print(f"Ошибка при подключении к базе данных: {e}")
            if self.connect:
                self.connect.rollback()
            return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Закрывает курсор и соединение при выходе из блока 'with'."""
        try:
            if self.cursor:
                self.cursor.close()
                print("Курсор закрыт.")
            if self.connect:
                self.connect.close()
                print("Соединение с базой данных закрыто.")
        except Exception as e:
            print(f"Ошибка при закрытии соединения или курсора: {e}")



def execute_query(query, *args, database='postgres',  autocommit=False):
    """Выполняет SQL-запрос и возвращает результат."""

    conn = DatabaseConnection(database=database) if not autocommit else DatabaseConnection(database=database, autocommit=True)
    with conn as cursor:
        if cursor:
            try:
                cursor.execute(query, args)
                if query.lower().startswith("select"):
                    return cursor.fetchall()
                elif query.lower().startswith("create"):
                    return 'Новая таблица успешно создалась'
                else:
                    return 'Новая база данных успешно создалась' if query.lower().startswith("create") else 'База успешно удалена' if autocommit else None
            except Exception as e:
                print(f"Ошибка при выполнении запроса: {e}")
                return None
        else:
            print("Не удалось получить курсор.")
            return None
