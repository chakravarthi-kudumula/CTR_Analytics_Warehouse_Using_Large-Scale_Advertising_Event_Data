FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_LOCAL_HOSTNAME=localhost

USER airflow
COPY requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir -r /tmp/project-requirements.txt
