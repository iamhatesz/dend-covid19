# A correlation of tweets sentiment and global markets value

## Scope of the project
In this project I processed data to provide a visualization of market indices values and an overall tweets sentiment
through a given range of time. In this particular example I used **COVID-19-TweetIDs** dataset from
[here](https://github.com/echen102/COVID-19-TweetIDs) for tweets data and **NASDAQ OMX Global Index Data**
from [Quandl](https://www.quandl.com/data/NASDAQOMX-NASDAQ-OMX-Global-Index-Data).

### Tweets pipeline
Respecting the Twitter policy, **COVID-19-TweetIDs** contain only tweets IDs.
To gather their contents, one can follow instructions from their README file.
Unfortunately, this process takes at least few days to collect, due to the throttling of Twitter API.
I was able to scrap only small portion of the data using prepared script and running it on Amazon EC2 instance.
Then I uploaded it to Amazon S3 for further processing.

To extract sentiment from tweets texts I used [Spark NLP](https://nlp.johnsnowlabs.com/) and their pre-trained
sentiment analysis pipeline. Unfortunately, my initial idea of storing the data on Amazon S3 failed, due to the
incompatibility of `hadoop` libraries that ship with `PySpark` and that which ship with `spark-nlp` dependencies.
From my investigation, `spark-nlp` depends on `hadoop-aws:3.2.0`, while `pyspark` bases on some earlier artifacts.
This leads to a lot of different errors with incompatible JARs, etc.

As a temporary solution, I based on storing the tweets data locally
(for a few days from January 2020, almost 9 million records, they use only few GBs of space).

Having the tweets stored, I created a pipeline which firstly extracts overall sentiment from each given tweet,
and saves it in an intermediate directory.
Then, another task uploads the data into Amazon Redshift table `tweets_sentiment`:

![tweets_sentiment_table](https://imgur.com/aN0Y2zR.png)

Due to the problem with dependencies of `pyspark` and `spark-nlp`, I had to use Spark local mode to run the analysis.
I was planning of using `SparkSubmitOperator`, which would connect to Amazon EMR and execute the analysis there.
Then the result would also be stored in S3 and copied to Redshift with `COPY ...` statement.
In the workaround solution, a simple `INSERT INTO` SQL statement is used instead.

Finally, a simple data quality check is run, which checks, whether at least a single record was added to the table.

### Markets value pipeline
Data is simply fetched from Quandl API. We use templating based on execution date and variable `market_indices`
to determine what values to fetch. The data is then stored in Redshift in `markets_value` table:

![markets_value_table](https://imgur.com/WEQbPI4.png)

After the insertion, a simple data quality check is run, which checks,
whether at least a single record was added to the table.

### Combined pipeline

Both pipelines were combined and defined as a single DAG in Airflow called `tweets_and_market`:

![tweets_and_market_dag](https://imgur.com/3mMFYlZ.png)

### Tables configuration pipeline

In Airflow there is also the `reset_tables` DAG, which when triggered drops both tables and creates them once again:

![reset_tables_dag](https://imgur.com/xvH8mQa.png)

### Visualization

A simple visualization can be made based on this SQL:

```sql
SELECT mv.date AS date, mv.index, mv.value, ts.positive_count, ts.negative_count FROM markets_value mv
JOIN tweets_sentiment ts ON mv.date = ts.date
```

Results:

![sql_results](https://imgur.com/ZCgd8pQ.png)

### Reproducing results

Install dependencies:

```bash
pip install -r requirements.txt
```

Start Airflow:

```bash
./airflow_start.sh
```

```bash
./airflow_scheduler_start.sh
```

In Airflow configure following connections:
* aws (AWS)
* quandl (HTTP, put your API key in password field)
* redshift (Postgres)

and variables:
* market_indices (comma separated names of data from Quandl, e.g. `NASDAQOMX/XQC,NASDAQOMX/NQPL`)

Put the data into `data/tweets`.

Run the `reset_tables` DAG. Once finished, run the `tweets_and_markets` DAG.

## What if...

### ... the data was increased by 100x?
With the final solution working with an EMR connection, it would be enough to simply scale-up the cluster.
With the current solution we can split daily tweets into smaller portions and increase number of airflow workers
(very dummy solution, but it should work).

### ... the pipelines would be run on a daily basis by 7 am every day?
The pipelines are running on a daily basis right now, so no further actions are needed.
However, if we worry about missing the deadline of 7 am every day, we should define SLA for the pipeline.

### ... the database needed to be accessed by 100+ people?
The final tables in Amazon Redshift database are very small, so there won't be a problem accessing them efficiently
by many people. However, if needed, one can simply scale-up Amazon Redshift cluster to more nodes.
