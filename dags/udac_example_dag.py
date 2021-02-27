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
    'owner': 'eddie',
    'start_date': datetime.now(),
    
    'depends_on_past': False,
    'email': 'xyz@xyz.com',
    'email_on_failure': 'xyz@xyz.com',
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    

}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="",
    s3_bucket="",
    s3_key="",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="",
    s3_bucket="",
    s3_key="",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="",
    sql_query = "",
    table = "",
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="",
    sql_query = "",
    table = "",
    clear_load = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="",
    sql_query = "",
    table = "",
    clear_load = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="",
    sql_query = "",
    table = "",
    clear_load = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="",
    sql_query = "",
    table = "",
    clear_load = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

Begin_execution  >> Stage_events
Begin_execution  >> Stage_songs

Stage_events  >> Load_songplays_fact_table
Stage_songs  >> Load_songplays_fact_table

Load_songplays_fact_table  >> Load_song_dim_table
Load_songplays_fact_table  >> Load_user_dim_table
Load_songplays_fact_table  >> Load_artist_dim_table
Load_songplays_fact_table  >> Load_time_dim_table

Load_song_dim_table  >> Run_data_quality_checks
Load_user_dim_table  >> Run_data_quality_checks
Load_artist_dim_table  >> Run_data_quality_checks
Load_time_dim_table  >> Run_data_quality_checks

Run_data_quality_checks  >> Stop_execution

