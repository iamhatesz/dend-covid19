import logging
import os
from collections import Counter
from datetime import datetime

os.environ['JAVA_HOME'] = r"/usr/lib/jvm/java-8-openjdk-amd64"

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from sparknlp.pretrained import PretrainedPipeline


def extract_sentiment(aws_conn_id: str, tweets_path: str, summary_path: str, language: str, **kwargs):
    aws_hook = AwsHook(aws_conn_id=aws_conn_id)
    aws_credentials = aws_hook.get_credentials()

    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("Analyse sentiment of given tweets")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.kryoserializer.buffer.max", "1000M")
             # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,"
             #                                "org.apache.hadoop:hadoop-common:3.2.0,"
             #                                "org.apache.hadoop:hadoop-annotations:3.2.0,"
             #                                "org.apache.hadoop:hadoop-auth:3.2.0,"
             #                                "org.apache.hadoop:hadoop-client:3.2.0")
             .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.5")
             .config("spark.hadoop.fs.s3a.access.key", aws_credentials.access_key)
             .config("spark.hadoop.fs.s3a.secret.key", aws_credentials.secret_key)
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.endpoint", "s3-eu-central-1.amazonaws.com")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
             .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
             .getOrCreate())

    year = kwargs['execution_date'].year
    month = kwargs['execution_date'].month
    day = kwargs['execution_date'].day
    tweets_path = f'{tweets_path}/{year:04d}/{month:02d}/{day:02d}/*.jsonl.gz'
    summary_path = f'{summary_path}/{year:04d}-{month:02d}-{day:02d}.jsonl'

    logging.info(f'Reading tweets from: {tweets_path}')
    tweets = spark.read.json(tweets_path)

    english_tweets_only = tweets.select('full_text').where(tweets.lang == language)
    original_english_tweets_only = english_tweets_only.where(~english_tweets_only.full_text.startswith('RT @'))

    sentiment_pipeline = PretrainedPipeline('analyze_sentiment', language)
    analysed_tweets = sentiment_pipeline.annotate(original_english_tweets_only, column='full_text')

    main_sentiment = udf(lambda col: Counter(col).most_common(1)[0][0], StringType())

    tweets_with_overall_sentiment = (analysed_tweets
                                     .withColumn('overall_sentiment', main_sentiment(analysed_tweets.sentiment.result))
                                     .drop('document', 'sentence', 'token', 'checked'))

    tweets_sentiment_summary = tweets_with_overall_sentiment.groupBy('overall_sentiment').count()

    tweets_sentiment_record = dict(tweets_sentiment_summary.rdd
                                   .map(lambda r: (r['overall_sentiment'], r['count']))
                                   .collect())
    tweets_sentiment_record['tweets_sentiment_id'] = f'{year:04d}-{month:02d}-{day:02d}({language})'
    tweets_sentiment_record['year'] = year
    tweets_sentiment_record['month'] = month
    tweets_sentiment_record['day'] = day
    tweets_sentiment_record['language'] = language
    tweets_sentiment_record['positive_count'] = tweets_sentiment_record['positive']
    tweets_sentiment_record['negative_count'] = tweets_sentiment_record['negative']
    tweets_sentiment_record['na_count'] = tweets_sentiment_record['na']
    del tweets_sentiment_record['positive']
    del tweets_sentiment_record['negative']
    del tweets_sentiment_record['na']

    logging.info(f'Extracted sentiment summary for {year:04d}-{month:02d}-{day:02d}: {tweets_sentiment_record}')

    tweets_sentiment = spark.createDataFrame([tweets_sentiment_record])
    tweets_sentiment.write.json(summary_path, mode='overwrite')


def insert_tweets_sentiment(redshift_conn_id: str, aws_conn_id: str, **kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id, autocommit=True)
    aws_hook = AwsHook(aws_conn_id=aws_conn_id)
    aws_credentials = aws_hook.get_credentials()

    year = kwargs['execution_date'].year
    month = kwargs['execution_date'].month
    day = kwargs['execution_date'].day

    sql = f"""
        COPY tweets_sentiment
        FROM './../../../data/tweets-sentiment/{year:04d}-{month:02d}-{day:02d}.jsonl'
        ACCESS_KEY_ID '{aws_credentials.access_key}'
        SECRET_ACCESS_KEY '{aws_credentials.secret_key}'
        FORMAT AS JSON 'auto'
    """
    redshift_hook.run(sql)


with open('./sql/insert_tweets_sentiment.sql', 'r') as f:
    insert_tweets_sentiment_sql = f.read()


def insert_tweets_sentiment_naive(redshift_conn_id: str, tweets_sentiment_path: str, **kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id, autocommit=True)

    spark = (SparkSession
             .builder
             .master("local[*]")
             .getOrCreate())

    year = kwargs['execution_date'].year
    month = kwargs['execution_date'].month
    day = kwargs['execution_date'].day

    tweets_sentiment_path = f'{tweets_sentiment_path}/{year:04d}-{month:02d}-{day:02d}.jsonl'
    tweets_sentiment = spark.read.json(tweets_sentiment_path).collect()[0]

    redshift_hook.run(insert_tweets_sentiment_sql, parameters=[
        tweets_sentiment.tweets_sentiment_id,
        datetime(tweets_sentiment.year, tweets_sentiment.month, tweets_sentiment.day),
        tweets_sentiment.year,
        tweets_sentiment.month,
        tweets_sentiment.day,
        tweets_sentiment.language,
        tweets_sentiment.positive_count,
        tweets_sentiment.negative_count,
        tweets_sentiment.na_count
    ])


def check_if_tweets_sentiment_data_was_inserted(redshift_conn_id: str, **kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id, autocommit=True)
    records = redshift_hook.get_records(f"""
        SELECT tweets_sentiment_id FROM tweets_sentiment WHERE date = %s
    """, [kwargs['ds']])
    if records is None or len(records) < 1:
        raise ValueError(f'Records count should be larger than 0')
