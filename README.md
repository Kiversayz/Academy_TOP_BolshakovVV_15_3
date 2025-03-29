# Academy_TOP_BolshakovVV_15_3
# Задание 1
Создание БД и таблиц, происходит через PyCharm/VS code, для этого используются соответствующий класс и методы в нем.
Создать БД NorthWind (задать или не задавать конкретные параметры для БД на ваш выбор), создание

Создать таблицы внутри данной БД:
*customers_data*
со следующими столбцами:
- customer_id - nvarchar(10) PK;
- company_name - nvarchar(100) - обязательно;
- contact_name - nvarchar(50) - обязательно.

*employees_data*
со следующими столбцами:
- employee_id - int PK;
- first_name - nvarchar(100) - обязательно;
- last_name - nvarchar(100) - обязательно;
- title - nvarchar(100) - обязательно;
- birth_data - date - обязательно;
- notes - nvarchar(1000) - обязательно

*orders_data*
со следующими столбцами:
- order_id - int PK;
- customer_id - nvarchar(10) - внешний ключ от
- customers_data(customer_id) обязательно;
- employee_id - int - внешний ключ от
- employees_data(employee_id) обязательно;
- order_date - дата - обязательно;
- ship_city - nvarchar(100) - обязательно.

# Задание 2
На уроке № 1 - 2 было показано как можно получить данные из файла формата .csv, а на уроке № 5 - 6 как можно заполнять таблицы большим количеством данных. 

*Ваше задание будет состоять в следующем:*
- написать класс для получения данных из .csv файлов успешно
- получить данные из файлов;
- к данным по employee надо будет добавить id (значения id для каждого должны быть уникальны)
- заполнить этими данными соответствующие таблицы.

В качестве выполненного задания жду ссылку на ваш проект с кодом, к коду должны быть добавлены докстринги, а также скриншоты из Managment studio где будут показаны заполненные данными таблицы.