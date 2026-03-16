FROM apache/airflow:3.1.8

RUN pip install airflow_provider_sap_hana Faker
