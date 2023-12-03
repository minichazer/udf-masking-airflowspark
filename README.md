# Применение пользовательских методов маскирования и анонимизации к атрибутам баз данных через UDF
###### здравствуйте, это на диплом, спасибо

Интеграция пользовательской функции PySpark (UDF) для применения к БД с использованием Apache Airflow для оркестрации, в среде Docker.

## Containers startup
```docker
docker compose -f AirFlow.yaml -f Spark.yaml -f create_db_instances.yaml -d --build
```

## Database Connection Test
```
jdbc:mysql://127.0.0.1:3307/maindb
jdbc:postgresql://localhost:5433/postgres
```

## .env
```
AIRFLOW_UID=50000
```


## Config
config_example.py -> config.py


