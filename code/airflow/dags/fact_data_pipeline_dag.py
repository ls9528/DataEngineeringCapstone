from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadTableOperator,
                                DataQualityOperator)
from helpers import SqlQueries

EXEC_DATE = "{ds}"

default_args = {
    'owner': 'udacity-lhemelt',
    'start_date': datetime(2016, 4, 1),
    'end_date': datetime(2016, 4, 30)
}

dag = DAG('fact_data_pipeline_dag',
          default_args=default_args,
          description='Loads and transforms fact table data from S3 to Redshift with Airflow',
          schedule_interval='@daily',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigration',
    dag=dag,
    table="staging_immigration",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="lhemelt",
    s3_key="capstone/immigration.parquet/arrival_date_partition=" + EXEC_DATE + "/",
    truncate_data=True,
    data_format="PARQUET"
)

load_immigration_table = LoadTableOperator(
    task_id='Load_immigration_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="fact_immigration",
    sql_statement=SqlQueries.fact_immigration_insert,
    truncate_data=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM fact_immigration where arrival_date = ", 'expected_result': 0 , 'comparison': ">"}
    ],
    include_date = True,
    provide_context=True
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_immigration_to_redshift
stage_immigration_to_redshift >> load_immigration_table
load_immigration_table >> run_quality_checks
run_quality_checks >> end_operator

