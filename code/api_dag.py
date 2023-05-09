import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id='api_dag',
        schedule_interval=' 30 17 * * 1',
        start_date=datetime(year=2023, month=4, day=4),
        catchup=False
) as dag:
    # 1. Run pyspark script to pull api data and add raw data to database
    task_api = BashOperator(
        task_id='run_pyspark_api_script',
        bash_command='$SPARK_HOME/bin/spark-submit /Users/Jess/Documents/e63/apiPull/batch_api.py'
    )

    # 2. Transform data, save transformed data in a database table
    task_transform = BashOperator(
        task_id='run_pyspark_transform_script',
        bash_command='$SPARK_HOME/bin/spark-submit /Users/Jess/Documents/e63/apiPull/transform.py'
    )
    
    #3. Visualize data with python altair
    task_viz = BashOperator(
        task_id='run_python_viz_script',
        bash_command='python3 /Users/Jess/Documents/e63/apiPull/visualize.py'
    )

    # Set order of DAG
    task_api >> task_transform >> task_viz