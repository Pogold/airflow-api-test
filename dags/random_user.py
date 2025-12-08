from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json

def fetch_user():
    url = "https://randomuser.me/api/"
    response = requests.get(url) 
    data = response.json()
    return json.dumps(data)

def parse_and_save(**context):
    raw = context['ti'].xcom_pull(task_ids='fetch_user')
    data = json.loads(raw)['results'][0]

    uuid = data['login']['uuid']
    first_name = data['name']['first']
    last_name = data['name']['last']
    email = data['email']
    country = data['location']['country']

    conn = psycopg2.connect(
        host="postgres_jokes",
        database="jokes",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    query = """
        INSERT INTO random_users (uuid, first_name, last_name, email, country)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (uuid) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            country = EXCLUDED.country;
    """

    cur.execute(query, (uuid, first_name, last_name, email, country))
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='random_user_dag',
    start_date=datetime(2024,1,1),
    schedule='*/5 * * * *',  # каждые 5 минут
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_user',
        python_callable=fetch_user
    )

    save_task = PythonOperator(
        task_id='parse_and_save',
        python_callable=parse_and_save
    )

    fetch_task >> save_task