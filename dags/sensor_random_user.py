from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
import logging
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}


def calculate_registered_users(**context):
    execution_date = context['execution_date']
    target_date = execution_date - timedelta(days=1)
    target_date_str = target_date.strftime('%Y-%m-%d')

    logging.info(f"Calculating registrations for date: {target_date_str}")

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    check_query = """
        SELECT id FROM user_registrations_daily 
        WHERE registration_date = %s
    """
    existing = pg_hook.get_first(check_query, parameters=(target_date_str,))

    if existing:
        logging.info("Record exists — skipping")
        return "skipped"

    count_query = """
        SELECT COUNT(*) FROM random_users
        WHERE DATE(created) = %s
    """

    result = pg_hook.get_first(count_query, parameters=(target_date_str,))
    user_count = result[0] if result else 0

    insert_query = """
        INSERT INTO user_registrations_daily (registration_date, user_count)
        VALUES (%s, %s)
        ON CONFLICT (registration_date)
        DO UPDATE SET
            user_count = EXCLUDED.user_count,
            calculation_timestamp = CURRENT_TIMESTAMP
    """

    pg_hook.run(insert_query, parameters=(target_date_str, user_count))

    logging.info(f"Inserted {user_count} users for {target_date_str}")
    return user_count


with DAG(
    dag_id='daily_user_registrations',
    default_args=default_args,
    schedule='30 21 * * *',   # 21:30 UTC = 00:30 МСК
    catchup=False,
    tags=['analytics', 'users'],
    max_active_runs=1,
) as dag:

    wait_for_user_data = ExternalTaskSensor(
        task_id='wait_for_user_data_sync',
        external_dag_id='random_user_dag',
        # allowed_states=['success'],
        # failed_states=['failed', 'skipped'],
        execution_delta=timedelta(days=1),
        mode='reschedule',
        timeout=3600,
        poke_interval=300,
    )

    create_table_if_not_exists = SQLExecuteQueryOperator(
        task_id='create_aggregation_table',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS user_registrations_daily (
                id SERIAL PRIMARY KEY,
                registration_date DATE UNIQUE NOT NULL,
                user_count INTEGER NOT NULL,
                calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    calculate_users = PythonOperator(
        task_id='calculate_daily_registrations',
        python_callable=calculate_registered_users,
    )

    validate_results = SQLExecuteQueryOperator(
        task_id='validate_calculation',
        conn_id='postgres_default',
        sql="""
            DO $$
            DECLARE
                v_count INTEGER;
                v_date DATE := '{{ prev_ds }}';
            BEGIN
                SELECT COUNT(*) INTO v_count
                FROM user_registrations_daily 
                WHERE registration_date = v_date;

                IF v_count = 0 THEN
                    RAISE EXCEPTION 'No data for %', v_date;
                END IF;
            END $$;
        """,
    )

    wait_for_user_data >> create_table_if_not_exists >> calculate_users >> validate_results