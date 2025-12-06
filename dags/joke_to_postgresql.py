import requests
import pendulum
from airflow.sdk import dag, task
import psycopg2


@dag(
    dag_id="joke_to_postgresql",
    schedule="0 * * * *",     # каждый час
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["jokes", "postgres", "api"],
)
def joke_to_postgresql():

    @task()
    def fetch_joke():
        url = "https://official-joke-api.appspot.com/random_joke"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    @task()
    def save_to_db(joke):
        conn = psycopg2.connect(
            host="postgres_jokes",
            database="jokes",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()

        sql = """
        INSERT INTO jokes (id, type, setup, punchline)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          type = EXCLUDED.type,
          setup = EXCLUDED.setup,
          punchline = EXCLUDED.punchline;
        """

        cursor.execute(sql, (
            joke["id"],
            joke["type"],
            joke["setup"],
            joke["punchline"]
        ))

        conn.commit()
        cursor.close()
        conn.close()

    joke = fetch_joke()
    save_to_db(joke)


dag = joke_to_postgresql()
