from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

# Дефолтные аргументы DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Инициализация DAG
with DAG(
        'replicate_postgres_to_mysql',
        default_args=default_args,
        description='Replicate data from PostgreSQL to MySQL with table creation',
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:

    def create_mysql_table(mysql_conn_id, table_name):
        """Создание таблицы в MySQL, если она не существует"""
        mysql_hook = MySqlHook(mysql_conn_id)

        # Определение схемы таблиц
        table_schemas = {
            'Users': """
                CREATE TABLE IF NOT EXISTS Users (
                    user_id INT PRIMARY KEY,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    phone VARCHAR(15) UNIQUE,
                    registration_date TIMESTAMP,
                    loyalty_status VARCHAR(20)
                );
            """,
            'Products': """
                CREATE TABLE IF NOT EXISTS Products (
                    product_id INT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    description TEXT,
                    category_id INT NOT NULL,
                    price DECIMAL(10, 2) NOT NULL,
                    stock_quantity INT,
                    creation_date TIMESTAMP
                );
            """,
            'ProductCategories': """
                CREATE TABLE IF NOT EXISTS ProductCategories (
                    category_id INT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    parent_category_id INT
                );
            """,
            'Orders': """
                CREATE TABLE IF NOT EXISTS Orders (
                    order_id INT PRIMARY KEY,
                    user_id INT,
                    order_date TIMESTAMP,
                    total_amount DECIMAL(10, 2) NOT NULL,
                    status VARCHAR(20),
                    delivery_date TIMESTAMP
                );
            """,
            'OrderDetails': """
                CREATE TABLE IF NOT EXISTS OrderDetails (
                    order_detail_id INT PRIMARY KEY,
                    order_id INT,
                    product_id INT,
                    quantity INT NOT NULL,
                    price_per_unit DECIMAL(10, 2) NOT NULL,
                    total_price DECIMAL(10, 2)
                );
            """,
        }

        # Выполнение SQL-запроса для создания таблицы
        create_table_sql = table_schemas.get(table_name)
        if create_table_sql:
            mysql_hook.run(create_table_sql)
            print(f"Таблица {table_name} создана (если её не было).")


    def replicate_table(postgres_conn_id, mysql_conn_id, table_name):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import pandas as pd

        postgres_hook = PostgresHook(postgres_conn_id)
        mysql_hook = MySqlHook(mysql_conn_id)

        # (Создание таблицы, если нужно)
        create_mysql_table(mysql_conn_id, table_name)

        # Считываем таблицу из PostgreSQL
        sql = f"SELECT * FROM {table_name};"
        records = postgres_hook.get_pandas_df(sql)
        print(f"Извлеченные данные из таблицы {table_name}:")
        print(records.head())

        # Обходим все столбцы и аккуратно заменяем пропуски
        for col in records.columns:
            # Для строковых полей
            if records[col].dtype.kind in {'O', 'U', 'S'}:
                # Пусть пропуски станут пустыми строками (можно и None, если хотите NULL)
                records[col] = records[col].fillna('')

            # Для числовых (включая int, float, unsigned)
            elif records[col].dtype.kind in {'i', 'u', 'f'}:
                # Превращаем колонку в object, чтобы pandas мог хранить None
                records[col] = records[col].astype(object)
                # Заменяем NaN/NaT на None (это будет NULL в MySQL)
                records[col] = records[col].where(records[col].notnull(), None)

            # Для дат (dtype == 'datetime64[ns]')
            elif records[col].dtype.kind == 'M':
                # Переводим в object, чтобы пустые значения могли быть None
                records[col] = records[col].astype(object).where(records[col].notnull(), None)

        # Очищаем таблицу в MySQL перед вставкой
        mysql_hook.run(f"TRUNCATE TABLE {table_name};")

        # Пытаемся вставить данные
        try:
            mysql_hook.insert_rows(
                table=table_name,
                rows=records.values.tolist(),  # Данные в виде списка списков
                target_fields=records.columns.tolist()
            )
            print(f"Таблица {table_name} успешно реплицирована!")
        except Exception as e:
            print(f"Ошибка при репликации таблицы {table_name}: {e}")


    # Список таблиц для репликации
    tables = ['Users', 'Products', 'ProductCategories', 'Orders', 'OrderDetails']

    # Создаём задачи на репликацию для каждой таблицы
    for table in tables:
        replicate_task = PythonOperator(
            task_id=f'replicate_{table.lower()}',
            python_callable=replicate_table,
            op_kwargs={
                'postgres_conn_id': 'postgres_connection',
                'mysql_conn_id': 'mysql_connection',
                'table_name': table,
            },
        )
