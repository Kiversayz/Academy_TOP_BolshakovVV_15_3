from SQL.DatabaseManager import DatabaseManager
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Получение параметров подключения из переменных окружения
LOGIN = os.getenv("LOGIN")
PASS = os.getenv("PASS")
SERVER = os.getenv("SERVER")

# Базовая директория и название базы данных
BASE_DIR = os.path.dirname(__file__)
NAME_DB = 'NorthWind'

# Инициализация менеджера базы данных
db_manager = DatabaseManager(server=SERVER, login=LOGIN, password=PASS)

# Проверка существования базы данных
if not db_manager.check_database_exists(NAME_DB):
    # Создание базы данных
    db_manager.create_database(NAME_DB)
else:
    print(f"База данных {NAME_DB} уже существует")

# Создание таблиц с проверкой существования
create_tables_queries = [
    """IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customers_data')
        CREATE TABLE customers_data (
            customer_id NVARCHAR(10) PRIMARY KEY,
            company_name NVARCHAR(100) NOT NULL,
            contact_name NVARCHAR(50) NOT NULL
        );""",
    """IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'employees_data')
        CREATE TABLE employees_data (
            employee_id INT PRIMARY KEY,
            first_name NVARCHAR(100) NOT NULL,
            last_name NVARCHAR(100) NOT NULL,
            title NVARCHAR(100) NOT NULL,
            birth_date DATE NOT NULL,
            notes NVARCHAR(1000) NOT NULL
        );""",
    """IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders_data')
        CREATE TABLE orders_data (
            order_id INT PRIMARY KEY,
            customer_id NVARCHAR(10) FOREIGN KEY REFERENCES customers_data(customer_id),
            employee_id INT FOREIGN KEY REFERENCES employees_data(employee_id),
            order_date DATE NOT NULL,
            ship_city NVARCHAR(100) NOT NULL
        );"""
]

for query in create_tables_queries:
    db_manager.execute_query(
        query=query,
        database=NAME_DB
    )

# Пути к CSV-файлам
CSV_PATHS = {
    'customers': os.path.join(BASE_DIR, 'data/customers_data.csv'),
    'employees': os.path.join(BASE_DIR, 'data/employees_data.csv'),
    'orders': os.path.join(BASE_DIR, 'data/orders_data.csv')
}

# Импорт данных
try:
    # Импорт клиентов (должны быть первыми из-за FOREIGN KEY)
    db_manager.import_csv_to_table(
        csv_file_path=CSV_PATHS['customers'],
        table_name='customers_data',
        database=NAME_DB
    )
    # Импорт сотрудников
    db_manager.import_csv_to_table(
        csv_file_path=CSV_PATHS['employees'],
        table_name='employees_data',
        database=NAME_DB
    )
    # Импорт заказов (последний из-за зависимостей)
    db_manager.import_csv_to_table(
        csv_file_path=CSV_PATHS['orders'],
        table_name='orders_data',
        database=NAME_DB
    )
except Exception as e:
    print(f"Ошибка при импорте данных: {str(e)}")
    exit()

# Проверка результатов
print("\nПроверка данных:")
print("Клиенты:", db_manager.execute_query(
    "SELECT COUNT(*) FROM customers_data", NAME_DB))
print("Сотрудники:", db_manager.execute_query(
    "SELECT COUNT(*) FROM employees_data", NAME_DB))
print("Заказы:", db_manager.execute_query(
    "SELECT COUNT(*) FROM orders_data", NAME_DB))

# Закрытие соединения
db_manager.close_connection()