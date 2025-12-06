import requests
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="hourly_joke_dag",
    schedule="0 * * * *",               # каждый час
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jokes", "example"],
)
def hourly_joke_dag():
    @task()
    def fetch_joke():
        url = "https://official-joke-api.appspot.com/random_joke"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        joke = response.json()
        return {
            "setup": joke.get("setup"),
            "punchline": joke.get("punchline"),
        }

    @task()
    def log_joke(joke):
        print("\n===== RANDOM JOKE =====")
        print(joke["setup"])
        print(joke["punchline"])
        print("=======================\n")

    joke = fetch_joke()
    log_joke(joke)


dag = hourly_joke_dag()
