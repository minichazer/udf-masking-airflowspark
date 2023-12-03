from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook
from airflow.configuration import conf

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, StringType, FloatType, DecimalType

from dataclasses import dataclass
from importlib.machinery import SourceFileLoader
from datetime import datetime
from faker import Faker

import hashlib
import random
import os
import yaml

# учитывая, что на спарк-воркере я установил вручную и сбилдил бинарники python3.8
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.8'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.8'


default_args = {
    'depends_on_past': False,
}
config_DB = yaml.safe_load(
        """
        postgres:
            conn_id: "DB_postgres"
            conn_type: "postgresql"
            description: "Connection to Docker PostgreSQL database"
            login: "postgres"
            password: "postgres"
            host: "127.0.0.1"
            port: 5433
            schema: "public"
            target_name: "postgres"

        mysql_server:
            conn_id: "DB_mysql"
            conn_type: "mysql"
            description: "Connection to Docker My SQL Server database"
            login: "admin"
            password: "admin123"
            host: "127.0.0.1"
            port: 3307
            schema: "maindb"
            target_name: "maindb"
            extra: json.dumps(dict(allowPublicKeyRetrieval="true"))
        """)
connection_configs = {}
drivers_dict = {
    'postgresql': 'org.postgresql.Driver', # :postgresql:42.7.0
    'mysql': 'com.mysql.cj.jdbc.Driver' # :mysql:8.0.13
}
    

def create_db_connections(**kwargs) -> None:
    uri_DB = {}
    connection_configs = {}
    
    for _, db_param in config_DB.items():
        connection_configs[db_param['conn_id']] = {
            'conn_id':db_param['conn_id'],
            'db_type': db_param['conn_type'],
            'host': '192.168.1.53',
            'driver': drivers_dict[db_param['conn_type']],
            'port': db_param['port'],
            'user': db_param['login'],
            'password': db_param['password'],
            'target_name': db_param['target_name'],
            'url': f"jdbc:{db_param['conn_type']}://192.168.1.53:{db_param['port']}/{db_param['target_name']}"
        }
        uri_DB[db_param['conn_type']] = db_param['conn_id']
    kwargs['ti'].xcom_push(key='uri_DB', value=uri_DB)
    kwargs['ti'].xcom_push(key='connection_configs', value=connection_configs)


def generate_data(**kwargs) -> None:
    fake = Faker()
    uri_DB = kwargs['ti'].xcom_pull(key='uri_DB', task_ids='create_db_connections_task')

    for db_type, conn_id in uri_DB.items():
        engine = BaseHook.get_connection(conn_id).get_hook()
        table_name = 'newtable'

        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        engine.run(drop_table_query)

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                salary FLOAT,
                age INTEGER,
                city VARCHAR(50),
                phone_number VARCHAR(50),
                user_id VARCHAR(100)
            );
            """
        engine.run(create_table_query)

        for _ in range(100):
            random_string = ''.join(random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(16))
            hashed_string = hashlib.sha256(random_string.encode()).hexdigest()
            data = {
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'salary' : float(random.randint(20000, 50000)),
            'age': random.randint(21, 80),
            'city': fake.city(),
            'phone_number': fake.phone_number(),
            'user_id': hashed_string,
            }

            values = [
                f"'{value}'" for value in data.values()
                ]
            insert_query = f"""
            INSERT INTO {table_name} (first_name, last_name, salary, age, city, phone_number, user_id)
            VALUES ({
                ', '.join(values)
                });
            """
            engine.run(insert_query)


def import_module(**kwargs) -> None:
    master_url = "spark://9585c32b5aea:7077"
    drivers_list = [
        'mysql-connector-java-8.0.13.jar',
        'postgresql-42.7.0.jar',
    ]
    drivers_path = ",".join([f'/opt/airflow/drivers/{i}' for i in drivers_list])

    with SparkSession.builder \
        .appName("Module Uploader") \
        .config("spark.jars", drivers_path) \
        .master(master_url) \
        .getOrCreate() as spark:
        # spark.sparkContext.setLogLevel("DEBUG")

        uri_DB = kwargs['ti'].xcom_pull(key='uri_DB', task_ids='create_db_connections_task')
        connection_configs = kwargs['ti'].xcom_pull(key='connection_configs', task_ids='create_db_connections_task')
        filepath = r'scripts/maintest.py'
        spark.sparkContext.addPyFile(filepath)

        testmodule = SourceFileLoader("maintest", filepath).load_module()
        # sys.modules["maintest"] = testmodule
        f = testmodule.main

        main_udf = udf(lambda x: f(x), FloatType())

        for db_type, conn_id in uri_DB.items():
            conn_config = connection_configs[conn_id]
            df = spark.read \
                .format("jdbc") \
                .option("url", conn_config['url']) \
                .option("dbtable", "newtable") \
                .option("driver", conn_config['driver']) \
                .option("user", conn_config['user']) \
                .option("password", conn_config['password']) \
                .load()

        dfnew = df.withColumn("new_salary", main_udf(col("salary")))
        dfnew.show()


with DAG(
    dag_id="test_dag",
    default_args=default_args,
    description="Basic masking of data in DBs",
    start_date=datetime(2023, 11, 18),
    schedule_interval='@daily',
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='create_db_connections_task',
        python_callable=create_db_connections,
        provide_context=True,
        dag=dag
    )

    # добавление случайных данных в БДшки
    t2 = PythonOperator(
        task_id='generate_data_task',
        python_callable=generate_data,
        provide_context=True,
        dag=dag
    ) 

    # добавление udf и применение к какой-нибудь таблице через датафрейм
    t3 = PythonOperator(
        task_id='import_module_task',
        python_callable=import_module,
        provide_context=True,
        dag=dag
    )

    t1 >> t2 >> t3
