from datetime import datetime

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator

import numpy as np
import pandas as pd

from pymongo import MongoClient


default_args={
    'owner': 'admin',
    'start_date': datetime(2022, 9, 16, 2)
}


def extract_data():
    path = Variable.get('path_to_data')
    data = pd.read_csv(path + '/tiktok_google_play_reviews.csv')
    data.to_csv('/c/Users/Lenovo/airflow/task5/output/raw_reviews.csv')


def transform_data():
    df = pd.read_csv('/c/Users/Lenovo/airflow/task5/output/raw_reviews.csv')
    df = df.replace(np.nan, '-')
    df = df.sort_values(by='at')
    df['content'] = df['content'].replace(r'[^A-Za-z0-9\.!()-;:\'\"\,?@ ]+', '', regex=True)
    df.to_csv('/c/Users/Lenovo/airflow/task5/output/final_reviews.csv')


def load_data():
    data = pd.read_csv('/c/Users/Lenovo/airflow/task5/output/final_reviews.csv')
    data = data.to_dict(orient='records')

    conn_str = Variable.get('mongodb_connection_string')

    client = MongoClient(conn_str)
    db = client['tiktokreviews']
    collection = db['tiktokreviews']

    collection.drop()
    collection.insert_many(data)
    client.close()


with DAG(
    dag_id='airflow_introduction',
    default_args=default_args,
    schedule_interval=None
) as dag:
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag
    )
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag
    )
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag
    )
    extract_data_task >> transform_data_task >> load_data_task
