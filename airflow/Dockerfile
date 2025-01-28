FROM apache/airflow:2.10.4

COPY ./dags/ ./dags/

RUN pip install apache-airflow-providers-microsoft-mssql[common.sql]