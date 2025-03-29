import pyodbc
import json
from decimal import Decimal
import pandas as pd


class DatabaseManager:
    """ Класс для подключения к БД и получения ответов на отправленные запросы, с возможностью сохранять в JSON файл. """

    def __init__(self, server, login, password, driver="ODBC Driver 17 for SQL Server"):
        self.server = server  # Сервер, например "ULTRASUPERPC\SQLEXPRESS"
        self.login = login  # Логин для авторизации в Ms SQL, желательно с правами dbo
        self.password = password  # Пароль от логина
        self.driver = driver  # Указать соответствующий драйвер для подключения
        self._connection = None  # Соединение будет создано при первом запросе

    def connection_string(self, database):
        """ Подготовленная строка для подключения к БД """
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={database};"
            f"UID={self.login};"
            f"PWD={self.password}"
        )

    @property
    def connection(self):
        """Ленивая инициализация соединения"""
        if not self._connection:
            conn_str = f"DRIVER={{{self.driver}}};SERVER={self.server};UID={self.login};PWD={self.password}"
            self._connection = pyodbc.connect(conn_str)
        return self._connection

    def close_connection(self):
        """Закрытие соединения при завершении работы"""
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute_query(self, query, database, params=None, autocommit=False):
        """Выполнение SQL-запроса с обработкой результатов"""
        try:
            with self.connection.cursor() as cursor:
                # Установка базы данных
                cursor.execute(f"USE {database}")
                if autocommit:
                    cursor.execute("SET IMPLICIT_TRANSACTIONS OFF")
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                # Проверка типа запроса
                if cursor.description:
                    results = cursor.fetchall()
                    return results
                else:
                    self.connection.commit()
                    return None
        except Exception as e:
            self.connection.rollback()
            raise e

    def create_database(self, database_name):
        """Создание базы данных без активной транзакции"""
        try:
            # Создаем новое подключение для выполнения CREATE DATABASE
            conn_str = f"DRIVER={{{self.driver}}};SERVER={self.server};UID={self.login};PWD={self.password}"
            with pyodbc.connect(conn_str, autocommit=True) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"База данных {database_name} успешно создана")
        except Exception as e:
            print(f"Ошибка при создании базы данных: {e}")
            raise

    def check_database_exists(self, database_name):
        """Проверка существования базы данных через системное представление sys.databases"""
        try:
            query = f"SELECT name FROM master.sys.databases WHERE name = '{database_name}'"
            result = self.execute_query(query=query, database="master")
            return bool(result)
        except Exception as e:
            print(f"Ошибка при проверке существования базы данных: {e}")
            return False

    def write_results_to_json(self, results, file_path):
        """Запись результатов ответа из БД в JSON"""
        def decimal_default(obj):
            """Проверка типов данных на Decimal и перевод в тип float"""
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(
                f"Object of type {obj.__class__.__name__} is not JSON serializable")

        try:
            with open(file=file_path, mode='w', encoding='utf-8') as file:
                json.dump(obj=results, fp=file, ensure_ascii=False,
                          indent=4, default=decimal_default)
            print(f"Результаты записаны в файл: {file_path}")
        except Exception as e:
            print(f"Ошибка при записи результатов в файл: {e}")

    def import_csv_to_table(self, csv_file_path, table_name, database):
        """Импорт данных из CSV в таблицу SQL Server"""
        try:
            # Чтение данных из CSV
            data = pd.read_csv(csv_file_path)
            # Проверка соответствия колонок таблице
            columns_query = f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
            """
            columns = [row[0] for row in self.execute_query(columns_query, database)]
            if set(data.columns) != set(columns):
                raise ValueError(f"Колонки CSV не соответствуют таблице {table_name}")

            # Формирование INSERT-запросов с использованием параметров
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
            rows = [tuple(row) for _, row in data.iterrows()]

            # Выполнение пакетной вставки
            with self.connection.cursor() as cursor:
                cursor.execute(f"USE {database}")
                cursor.executemany(insert_query, rows)
                self.connection.commit()

            print(f"Данные успешно импортированы в {table_name}")
        except Exception as e:
            print(f"Ошибка импорта: {str(e)}")
            raise

