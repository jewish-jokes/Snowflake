FROM apache/airflow:2.5.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq && apt-get install vim -qqq
ENV PATH="/root/.local/bin/:$PATH"
RUN chmod -R 777 /opt/airflow/*

USER airflow

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

ENV SHELL /bin/bash
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID
