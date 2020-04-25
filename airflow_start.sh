#!/bin/bash

export AIRFLOW_HOME=$(pwd)/airflow
airflow initdb
airflow webserver -p 8080
