from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

with open(r'./sql/drop_tables.sql', 'r') as f:
    drop_tables_sql = f.read()
with open(r'./sql/create_tables.sql', 'r') as f:
    create_tables_sql = f.read()

dag = DAG(
    'reset_tables',
    schedule_interval=None,
    start_date=datetime.today() - timedelta(days=1)
)

start = DummyOperator(task_id='start', dag=dag)

reset_tables = PostgresOperator(
    task_id='drop_tables',
    sql=drop_tables_sql,
    postgres_conn_id='redshift',
    autocommit=True
)

create_tables = PostgresOperator(
    task_id='create_tables',
    sql=create_tables_sql,
    postgres_conn_id='redshift',
    autocommit=True
)

finish = DummyOperator(task_id='finish', dag=dag)

start >> reset_tables
reset_tables >> create_tables
create_tables >> finish
