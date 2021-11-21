from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity-lhemelt',
    'start_date': datetime(2016, 4, 1),
    'end_date': datetime(2016, 4, 30)
    #'depends_on_past': False,
    #'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    #'catchup': False,
    #'email_on_retry': False
}

immigration_data_path = \
    "{{execution_date.strftime('capstone/immigration.parquet/arrival_day=%-d/')}}"

dag = DAG('data_pipeline_dag',
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
    s3_key=immigration_data_path
    truncate_table=True,
    data_format="PARQUET"
)

load_immigration_table = LoadFromTableOperator(
    task_id='Load_immigration_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="fact_immigration",
    sql_statement=SqlQueries.fact_immigration_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_statement=SqlQueries.user_table_insert,
    truncate_data=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_statement=SqlQueries.song_table_insert,
    truncate_data=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_statement=SqlQueries.artist_table_insert,
    truncate_data=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="\"time\"",
    sql_statement=SqlQueries.time_table_insert,
    truncate_data=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM \"time\" WHERE start_time is null", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

