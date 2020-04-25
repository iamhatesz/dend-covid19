from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from tasks.market import scrap_market_data, check_if_markets_value_data_was_inserted
from tasks.tweets import extract_sentiment, insert_tweets_sentiment, insert_tweets_sentiment_naive, \
    check_if_tweets_sentiment_data_was_inserted

dag = DAG(
    'tweets_and_market',
    schedule_interval='@daily',
    start_date=datetime(2020, 1, 21),
    end_date=datetime(2020, 1, 31)
)

start = DummyOperator(task_id='start', dag=dag)

scrap_market_data_task = PythonOperator(
    task_id='scrap_market_data',
    dag=dag,
    python_callable=scrap_market_data,
    provide_context=True,
    op_args=(
        'quandl',
        'redshift',
        Variable.get('market_indices').split(',')
    )
)

markets_value_data_quality_check_task = PythonOperator(
    task_id='market_data_quality_check',
    dag=dag,
    python_callable=check_if_markets_value_data_was_inserted,
    provide_context=True,
    op_args=(
        'redshift',
        Variable.get('market_indices').split(',')
    )
)

analyse_tweets_sentiment_task = PythonOperator(
    task_id='analyse_tweets_sentiment',
    dag=dag,
    python_callable=extract_sentiment,
    provide_context=True,
    op_args=(
        'aws',
        './data/tweets',
        './data/tweets-sentiment',
        'en'
    )
)

insert_tweets_sentiment_task = PythonOperator(
    task_id='insert_tweets_sentiment',
    dag=dag,
    python_callable=insert_tweets_sentiment_naive,
    provide_context=True,
    op_args=(
        'redshift',
        './data/tweets-sentiment'
    )
)

tweets_sentiment_data_quality_check_task = PythonOperator(
    task_id='tweets_sentiment_data_quality_check',
    dag=dag,
    python_callable=check_if_tweets_sentiment_data_was_inserted,
    provide_context=True,
    op_args=(
        'redshift',
    )
)

# insert_tweets_sentiment_task = PythonOperator(
#     task_id='insert_tweets_sentiment',
#     dag=dag,
#     python_callable=insert_tweets_sentiment,
#     provide_context=True,
#     op_args=(
#         'redshift',
#         'aws'
#     )
# )

# analyse_tweets_sentiment_task = SparkSubmitOperator(
#     task_id='analyse_tweets_sentiment',
#     dag=dag
# )

finish = DummyOperator(task_id='finish', dag=dag)

start >> scrap_market_data_task
scrap_market_data_task >> markets_value_data_quality_check_task
markets_value_data_quality_check_task >> finish

start >> analyse_tweets_sentiment_task
analyse_tweets_sentiment_task >> insert_tweets_sentiment_task
insert_tweets_sentiment_task >> tweets_sentiment_data_quality_check_task
tweets_sentiment_data_quality_check_task >> finish
