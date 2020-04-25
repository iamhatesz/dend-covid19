import logging
from datetime import datetime
from typing import List

import quandl
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook


with open('./sql/insert_markets_value.sql', 'r') as f:
    insert_markets_value_sql = f.read()


def scrap_market_data(quandl_conn_id: str, redshift_conn_id: str, indices: List[str], **kwargs):
    quandl_hook = BaseHook.get_connection(quandl_conn_id)
    quandl.ApiConfig.api_key = quandl_hook.password
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id, autocommit=True)

    date_key = kwargs['ds']
    data = {}
    for index in indices:
        logging.info(f'Fetching data for {index} on {date_key}')
        row = quandl.get(index, start_date=date_key, end_date=date_key)
        if len(row) > 0:
            data[index] = float(row['Index Value'])
        else:
            data[index] = None

    for index, value in data.items():
        logging.info(f'Storing data for {index} on {date_key}')
        year = kwargs['execution_date'].year
        month = kwargs['execution_date'].month
        day = kwargs['execution_date'].day
        redshift_hook.run(insert_markets_value_sql, parameters=[
            f'{year:04d}-{month:02d}-{day:02d}({index})',
            datetime(year, month, day),
            year,
            month,
            day,
            index,
            value
        ])


def check_if_markets_value_data_was_inserted(redshift_conn_id: str, indices: List[str], **kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id, autocommit=True)
    for index in indices:
        records = redshift_hook.get_records(f"""
            SELECT markets_value_id FROM markets_value WHERE date = %s AND index = %s
        """, parameters=[kwargs['ds'], index])
        if records is None or len(records) < 1:
            raise ValueError(f'Records count for index {index} should be larger than 0')
