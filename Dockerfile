
FROM apache/airflow:2.8.1-python3.10

USER root

RUN mkdir -p /opt/airflow/data_to_train
RUN chown -R airflow:root /opt/airflow/data_to_train

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    boto3 \
    pymongo \
    python-dotenv \
    requests \
    psycopg2-binary

WORKDIR /opt/airflow
COPY ./config.env .