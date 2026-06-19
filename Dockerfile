ARG IMAGE_NAME=apache/airflow:3.2.2

FROM ${IMAGE_NAME}

COPY src/ src/
COPY pyproject.toml .

RUN uv pip install --no-cache --group example_dag "apache-airflow==${AIRFLOW_VERSION}" .
