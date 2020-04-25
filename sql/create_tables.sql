CREATE TABLE IF NOT EXISTS tweets_sentiment
(
    tweets_sentiment_id varchar(14) PRIMARY KEY,
    date timestamp NOT NULL,
    year smallint NOT NULL,
    month smallint NOT NULL,
    day smallint NOT NULL,
    language varchar(2) NOT NULL,
    positive_count int NOT NULL,
    negative_count int NOT NULL,
    na_count int NOT NULL
);
---
CREATE TABLE IF NOT EXISTS markets_value
(
    markets_value_id varchar(40) PRIMARY KEY,
    date timestamp NOT NULL,
    year smallint NOT NULL,
    month smallint NOT NULL,
    day smallint NOT NULL,
    index varchar(20) NOT NULL,
    value real
);
