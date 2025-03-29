import sys
import os
from dotenv import load_dotenv
from SQL.DatabaseManager import DatabaseManager

if __name__ == '__main__':
    # Проверка доступных драйверов
    import pyodbc
    print("Доступные драйверы:", pyodbc.drivers())

    # Загрузка переменных окружения
    load_dotenv()
    LOGIN = os.getenv(key="LOGIN")
    PASS = os.getenv(key="PASS")
    SERVER = os.getenv(key="SERVER")
    DB_NAME = "Hospital"

    # Создание экземпляра менеджера базы данных
    db_manager = DatabaseManager(server=SERVER, login=LOGIN, password=PASS)

    # Проверка подключения к master
    if not db_manager.check_connection(database="master"):
        exit()

    # Проверка подключения к Hospital
    if not db_manager.check_connection(database=DB_NAME):
        exit()

    
    SQLDATA = {
        "quest1":"result1",
        "quest2":"result2",
        "quest3":"result3",
        "quest4":"result4",
        "quest5":"result5",
        "quest6":"result6",
        "quest7":"result7",
    }
    
    def dovloand_data_sql (folder,SQLDATA,db_manager):
        for kay, value in SQLDATA.items():
            sql_file_path = os.path.join(os.path.dirname(__file__), folder, f'{kay}.sql')
            results = db_manager.execute_sql_file(sql_file_path, DB_NAME)
            json_file_path = os.path.join(os.path.dirname(__file__), folder, f'{value}.json')
            if results:
                db_manager.write_results_to_json(results, json_file_path)
            else:
                print("Не удалось выполнить SQL-файл.")
    
    dovloand_data_sql(folder='SQL',SQLDATA=SQLDATA,db_manager=db_manager)