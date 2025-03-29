import pyodbc
import json
from decimal import Decimal


class DatabaseManager:
    """ Класс для полключения к БД и получения ответов на отправленные запросы, с возможностью сохранять в JSON файл. """

    def __init__(self, server, login, password, driver="ODBC Driver 17 for SQL Server"):
        self.server = server  # Сервер на пример "ULTRASUPERPC\SQLEXPRESS"
        self.login = login  # Логин из Ms SQL для авторизации, желательно с правами dbo
        self.password = password  # Пароль от логина
        # Указать соответсвующий драйвер для подключения на пример на ODBC Driver 18 for SQL Server
        self.driver = driver

    def connection_string(self, database):
        """ подготовленная строка для подключения к БД """
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={database};"
            f"UID={self.login};"
            f"PWD={self.password}"
        )

    def execute_query(self, query, database, autocommit=False):
        """ Оправка запроса в БД для получения данных """
        conn_str = self.connection_string(database)
        print("Строка подключения:", conn_str)
        try:
            with pyodbc.connect(conn_str, autocommit=autocommit) as conn:
                cursor = conn.cursor()
                print(f"Выполнение запроса: {query}")
                cursor.execute(query)

                if "SELECT" in query.upper():
                    rows = cursor.fetchall()
                    columns = [column[0] for column in cursor.description]
                    result = [dict(zip(columns, row)) for row in rows]
                    print(f"Результат запроса: {result}")
                    return result

                cursor.close()
        except pyodbc.Error as e:
            print(f"Ошибка выполнения запроса: {e}")
            return None

    def check_connection(self, database):
        """ Проверка подключения к БД """
        try:
            result = self.execute_query(query="SELECT 1;", database=database)
            if result:
                print(f"Подключение к {database} успешно")
                return True
            else:
                print(f"Ошибка подключения к {database}")
                return False
        except Exception as e:
            print(f"Ошибка подключения к {database}: {e}")
            return False

    def execute_sql_file(self, file_path, database, autocommit=False):
        """ Чтение SQL файла и отправка запросов """
        try:
            with open(file=file_path, mode='r', encoding='utf-8') as file:
                sql_script = file.read()

            # Разделение скрипта на отдельные запросы
            queries = sql_script.split(sep=';')
            results = []

            for query in queries:
                query = query.strip()
                if query:
                    result = self.execute_query(
                        query=query, database=database, autocommit=autocommit)
                    if result is not None:
                        results.append(result)

            return results

        except FileNotFoundError:
            print(f"Файл {file_path} не найден.")
            return None
        except Exception as e:
            print(f"Ошибка при выполнении SQL-файла: {e}")
            return None

    def write_results_to_json(self, results, file_path):
        """ Запись результатов ответа из БД в JSON """
        def decimal_default(obj):
            """ Проверка тип данных на Decimal и перевод в тип float во измбежание ошибки при сериализации в json """
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
