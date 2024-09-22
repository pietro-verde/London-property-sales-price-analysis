FROM apache/airflow:2.9.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

## TO DO: COPY FOLDER WITH OCI CONFIG

WORKDIR $AIRFLOW_HOME


USER $AIRFLOW_UID