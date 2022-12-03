from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.sensors.http import HttpSensor

AWS_ACCESS_KEY_ID=Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=Variable.get('AWS_SECRET_ACCESS_KEY')

with DAG(
        dag_id="covid_tracking",
        start_date=datetime(year=2022, month=12, day=2),
        schedule_interval=None,
        catchup=False
) as dag:
    # Start task
    start_task = EmptyOperator(
        task_id = "Start",
    )

    # End task
    end_task = EmptyOperator(
        task_id = "End"
    )

    # Check exist link
    check_link_task = HttpSensor(
        task_id = "check_download_link",
        http_conn_id= "covid_data_link",
        endpoint= "/s/iv5gfjskdefdo5tuzpeewtlg6ofmh6"
    )

    # download data
    data_download_task = BashOperator(
        task_id = "data_download",
        bash_command= "python3 ../../home/minhhieu/airflow/dags/covid-track/python/data_download.py"
    )

    # data ingestion
    data_ingestion_task = SparkSubmitOperator(
        task_id = "data_ingestion",
        conn_id="spark_local",
        application="../../home/minhhieu/airflow/dags/covid-track/python/data_ingestion.py",
        application_args=[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]
    )

    # data process
    data_process_task = SparkSubmitOperator(
        task_id = "data_process",
        conn_id="spark_local",
        application="../../home/minhhieu/airflow/dags/covid-track/python/data_process.py",
        application_args=[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]
    )

    # create table
    table_create_task = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id="covid_redshift_conn",
        params=dict(
            AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
        ),
        sql = "covid-track/sql/create_table.sql"
    )

    # upsert location_dim table
    upsert_location_dim_task = PostgresOperator(
        task_id = "upsert_location_dim",
        postgres_conn_id="covid_redshift_conn",
        params=dict(
            AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
        ),
        sql = "covid-track/sql/upsert_location_dim.sql"
    )

    # upsert location_dim table
    upsert_date_dim_task = PostgresOperator(
        task_id = "upsert_date_dim",
        postgres_conn_id="covid_redshift_conn",
        params=dict(
            AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
        ),
        sql = "covid-track/sql/upsert_date_dim.sql"
    )
    
    # upsert covid_daily_fact table
    upsert_covid_daily_fact_task = PostgresOperator(
        task_id = "upsert_covid_daily_fact",
        postgres_conn_id="covid_redshift_conn",
        params=dict(
            AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
        ),
        sql = "covid-track/sql/upsert_covid_daily_fact.sql"
    )

    # upsert report table
    upsert_report_task = PostgresOperator(
        task_id = "upsert_report",
        postgres_conn_id="covid_redshift_conn",
        params=dict(
            AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
        ),
        sql = "covid-track/sql/upsert_state_report.sql"
    )


    # create pipline
    start_task >> check_link_task >> data_download_task >> data_ingestion_task >> data_process_task
    data_process_task >> table_create_task >> [upsert_date_dim_task, upsert_location_dim_task] >> upsert_covid_daily_fact_task
    upsert_covid_daily_fact_task>> upsert_report_task >> end_task
