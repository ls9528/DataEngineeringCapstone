from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadTableOperator,
                                DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity-lhemelt',
}

dag = DAG('dim_data_pipeline_dag',
          default_args=default_args,
          description='Loads and transforms dimensional table data from S3 to Redshift with Airflow',
          start_date=datetime.now(),
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_demographic_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demographic',
    dag=dag,
    table="staging_demographic",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="lhemelt",
    s3_key="capstone/demographic.parquet/",
    truncate_data=True,
    data_format="PARQUET"
)

stage_date_to_redshift = StageToRedshiftOperator(
    task_id='Stage_date',
    dag=dag,
    table="staging_date",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="lhemelt",
    s3_key="capstone/date.parquet/",
    truncate_data=True,
    data_format="PARQUET"
)

stage_city_to_redshift = StageToRedshiftOperator(
    task_id='Stage_city',
    dag=dag,
    table="staging_city",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="lhemelt",
    s3_key="capstone/dim_city.csv",
    truncate_data=True,
    data_format="CSV",
    ignore_headers=1,
    delimiter="|"
)

stage_country_to_redshift = StageToRedshiftOperator(
    task_id='Stage_country',
    dag=dag,
    table="dim_country",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="lhemelt",
    s3_key="capstone/dim_country.csv",
    truncate_data=True,
    data_format="CSV",
    ignore_headers=1,
    delimiter="|"
)

stage_state_to_redshift = StageToRedshiftOperator(
    task_id='Stage_state',
    dag=dag,
    table="dim_state",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="lhemelt",
    s3_key="capstone/dim_state.csv",
    truncate_data=True,
    data_format="CSV",
    ignore_headers=1,
    delimiter=","
)

stage_visa_type_to_redshift = StageToRedshiftOperator(
    task_id='Stage_visa_type',
    dag=dag,
    table="dim_visa_type",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="lhemelt",
    s3_key="capstone/dim_visa_type.csv",
    truncate_data=True,
    data_format="CSV",
    ignore_headers=1,
    delimiter=","
)

stage_travel_mode_to_redshift = StageToRedshiftOperator(
    task_id='Stage_travel_mode',
    dag=dag,
    table="dim_travel_mode",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="lhemelt",
    s3_key="capstone/dim_travel_mode.csv",
    truncate_data=True,
    data_format="CSV",
    ignore_headers=1,
    delimiter=","
)

load_city_table = LoadTableOperator(
    task_id='Load_city_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="dim_city",
    columns="(city_code,city_name,state_id,median_age,male_population,female_population,total_population,veteran_population,foreign_population,avg_household_size,race_majority)",
    sql_statement=SqlQueries.dim_city_insert
)

load_date_table = LoadTableOperator(
    task_id='Load_date_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="dim_date",
    sql_statement=SqlQueries.dim_date_insert,
    truncate_data=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM dim_city", 'expected_result': 0 , 'comparison': ">"},
        {'check_sql': "SELECT COUNT(*) FROM dim_date", 'expected_result': 0 , 'comparison': ">"},
        {'check_sql': "SELECT COUNT(*) FROM dim_country", 'expected_result': 0 , 'comparison': ">"},
        {'check_sql': "SELECT COUNT(*) FROM dim_state", 'expected_result': 0 , 'comparison': ">"},
        {'check_sql': "SELECT COUNT(*) FROM dim_visa_type", 'expected_result': 0 , 'comparison': ">"},
        {'check_sql': "SELECT COUNT(*) FROM dim_travel_mode", 'expected_result': 0 , 'comparison': ">"}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_demographic_to_redshift, stage_date_to_redshift, stage_city_to_redshift, stage_country_to_redshift, stage_state_to_redshift, stage_visa_type_to_redshift, stage_travel_mode_to_redshift]

stage_date_to_redshift >> load_date_table
[stage_demographic_to_redshift, stage_city_to_redshift] >> load_city_table

[stage_country_to_redshift, stage_state_to_redshift, stage_visa_type_to_redshift, stage_travel_mode_to_redshift, load_date_table, load_city_table] >> run_quality_checks

run_quality_checks >> end_operator

