from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pyspark.sql import SparkSession
from datetime import datetime

def create_spark_session():
    return SparkSession.builder \
        .appName("MySQL_to_Vittrines") \
        .config("spark.jars", "/path/to/mysql-connector-java.jar") \
        .getOrCreate()

def generate_user_activity_vitrine(mysql_conn_id):
    """Генерация витрины активности пользователей"""
    mysql_hook = MySqlHook(mysql_conn_id)

    spark = create_spark_session()

    users_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/final",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="Users",
        user="username",
        password="password"
    ).load()

    orders_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/final",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="Orders",
        user="username",
        password="password"
    ).load()

    # Подсчет активности пользователей
    user_activity_df = orders_df.groupBy("user_id").agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("total_amount")
    )

    user_activity = users_df.join(user_activity_df, "user_id", "left")

    # Запись витрины в MySQL
    user_activity.write.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/final",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="UserActivityVitrine",
        user="username",
        password="password"
    ).mode("overwrite").save()

def generate_top_products_vitrine(mysql_conn_id):
    """Генерация витрины топ-продуктов"""
    mysql_hook = MySqlHook(mysql_conn_id)

    # Инициализация Spark
    spark = create_spark_session()

    # Загрузка данных из MySQL
    products_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/final",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="Products",
        user="username",
        password="password"
    ).load()

    order_details_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/final",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="OrderDetails",
        user="username",
        password="password"
    ).load()

    # Подсчет продаж по продуктам
    product_sales_df = order_details_df.groupBy("product_id").agg(
        sum("quantity").alias("total_quantity"),
        sum("total_price").alias("total_revenue")
    )

    top_products = products_df.join(product_sales_df, "product_id", "left")

    # Запись витрины в MySQL
    top_products.write.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/final",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="TopProductsVitrine",
        user="username",
        password="password"
    ).mode("overwrite").save()

def generate_vitrines(mysql_conn_id):
    generate_user_activity_vitrine(mysql_conn_id)
    generate_top_products_vitrine(mysql_conn_id)

# Инициализация DAG
with DAG(
        'generate_vitrines',
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
        },
        description='Generate analytic vitrines from MySQL',
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:

    generate_vitrines_task = PythonOperator(
        task_id='generate_vitrines',
        python_callable=generate_vitrines,
        op_kwargs={'mysql_conn_id': 'mysql_connection'}
    )