FROM astrocrpublic.azurecr.io/runtime:3.4-1

COPY include/ /usr/local/airflow/include/
COPY aiven_ca.pem /usr/local/airflow/aiven_ca.pem
ENV PYTHONPATH=/usr/local/airflow
