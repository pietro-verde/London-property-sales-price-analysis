FROM apache/airflow:2.9.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
# RUN apt-get update -qq \
#     && apt-get install vim -qqq \
#     && apt-get install -y --no-install-recommends \
#     openjdk-17-jre-headless
RUN apt-get update \
    && apt-get install -y openjdk-17-jdk

COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID