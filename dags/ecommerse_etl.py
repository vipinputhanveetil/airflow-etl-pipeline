from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

def extract_sales_data():
    df = pd.read_csv('dags/sales_data.csv')
    df.to_csv('dags/extracted_sales_data.csv', index=False)  # Save extracted data

def transform_sales_data():
    df = pd.read_csv('dags/extracted_sales_data.csv')
    df['total'] = df['quantity'] * df['price']
    transformed_df = df.groupby(['product_id', 'date']).agg({'total': 'sum'}).reset_index()
    transformed_df.to_csv('dags/transformed_sales_data.csv', index=False)

def load_sales_data():
    hook = PostgresHook(postgres_conn_id='postgres_connection')
    df = pd.read_csv('dags/transformed_sales_data.csv')
    for _, row in df.iterrows():
        hook.run("""
            INSERT INTO sales_data (product_id, purchase_date, total) 
            VALUES (%s, %s, %s);
        """, parameters=(row['product_id'], row['date'], row['total']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('etl_sales_analysis_test', description='ETL workflow for e-commerce sales analysis',
          schedule_interval='@daily',
          start_date=datetime(2024, 1, 1), catchup=False)

extract_task = PythonOperator(
    task_id='extract_sales_data', 
    python_callable=extract_sales_data, 
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_sales_data', 
    python_callable=transform_sales_data, 
    dag=dag
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS sales_data (
        product_id INT,
        purchase_date DATE,
        total DECIMAL
    );
    """,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_sales_data', 
    python_callable=load_sales_data, 
    dag=dag
)

extract_task >> transform_task >> create_table_task >> load_task
