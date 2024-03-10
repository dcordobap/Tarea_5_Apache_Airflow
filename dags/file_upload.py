from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd  # Importar pandas
from datetime import datetime, timedelta

def csv_to_postgres():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Borrar el contenido existente en la tabla
    cursor.execute("DELETE FROM penguins;")

    # Asegúrate de que la ruta al archivo CSV sea accesible en el entorno de Airflow
    df = pd.read_csv('/opt/airflow/data/penguins_lter_pipe.csv', sep="|")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS penguins (
            studyName VARCHAR(255),
            sampleNumber INTEGER,
            species VARCHAR(255),
            region VARCHAR(255),
            island VARCHAR(255),
            stage VARCHAR(255),
            individualID VARCHAR(255),
            clutchCompletion BOOLEAN,
            dateEgg DATE,
            culmenLength FLOAT,
            culmenDepth FLOAT,
            flipperLength FLOAT,
            bodyMassG FLOAT,
            sex VARCHAR(255),
            delta15N FLOAT,
            delta13C FLOAT,
            comments TEXT
        );
    """)

    insert_stmt = """
        INSERT INTO penguins (studyName, sampleNumber, species, region, island, stage, individualID, clutchCompletion, dateEgg, culmenLength, culmenDepth, flipperLength, bodyMassG, sex, delta15N, delta13C, comments) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for _, row in df.iterrows():
        cursor.execute(insert_stmt, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'csv_to_postgres',
    default_args=default_args,
    description='Load CSV data into PostgreSQL',
    schedule_interval=None,
    catchup=False,
)

load_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=csv_to_postgres,
    dag=dag,
)