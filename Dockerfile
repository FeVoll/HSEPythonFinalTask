FROM apache/airflow:2.6.0

USER root

RUN apt-get update && apt-get install -y openjdk-11-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY mysql-connector-j-9.1.0.jar /opt/airflow/jars/mysql-connector-j-9.1.0.jar

USER airflow

RUN pip install --no-cache-dir pyspark==3.4.4



