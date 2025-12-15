ARG AIRFLOW_VERSION=2.9.3
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl \
    libpq-dev \
    openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow
COPY --chown=airflow:root requirements.txt /requirements.txt

ARG PYTHON_MAJOR_MINOR=3.12
RUN pip install --no-cache-dir -r /requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR}.txt"