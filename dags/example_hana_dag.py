from __future__ import annotations

import csv

from faker import Faker
from faker.providers import automotive, person
from pendulum import datetime

from airflow.providers.common.sql.decorators.sql import sql_task
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow_provider_sap_hana.hooks.hana import SapHanaHook


@dag(
    dag_id="example_hana_dag",
    start_date=datetime(2024, 12, 20),
    schedule="@once",
    max_active_runs=1,
    catchup=False,
)
def example_hana_dag():
    check_table_exists = BranchSQLOperator(
        task_id="check_table_exists",
        conn_id="hana_default",
        follow_task_ids_if_false=["create_table"],
        follow_task_ids_if_true=["do_nothing"],
        sql="""
        SELECT COUNT(*)
        FROM sys.tables
        WHERE
            schema_name = 'AIRFLOW'
            AND table_name = 'FAKE_VEHICLE_REGISTRATIONS';""",
    )

    @sql_task(conn_id="hana_default")
    def create_table():
        return """
        CREATE TABLE airflow.fake_vehicle_registrations (
            vin NVARCHAR(17),
            owner_name_first NVARCHAR(30),
            owner_name_last NVARCHAR(30),
            address NVARCHAR(100),
            city NVARCHAR(30),
            state NVARCHAR(2),
            postal_code NVARCHAR(20),
            country NVARCHAR(2),
            created_at TIMESTAMP,
            PRIMARY KEY (vin)
          );"""

    do_nothing = EmptyOperator(task_id="do_nothing")

    @task(trigger_rule="none_failed_min_one_success")
    def create_fake_data():
        fake = Faker()
        fake.add_provider(automotive)
        fake.add_provider(person)

        with open("/tmp/fake_data.csv", mode="w", encoding="utf-8") as f:
            writer = csv.writer(f)
            for _ in range(1000000):
                vin = fake.vin()
                owner_name_first = fake.first_name().upper()
                owner_name_last = fake.last_name().upper()
                street = fake.street_address().upper()
                city = fake.city().upper()
                state = fake.state_abbr().upper()
                postal_code = fake.postalcode().upper()
                country = "US"
                created_at = fake.date_time_this_decade().isoformat()
                writer.writerow(
                    (
                        vin,
                        owner_name_first,
                        owner_name_last,
                        street,
                        city,
                        state,
                        postal_code,
                        country,
                        created_at,
                    )
                )

    @task(trigger_rule="none_failed_min_one_success")
    def insert_into_hana():
        hook = SapHanaHook(enable_db_log_messages=True)

        with open("/tmp/fake_data.csv", encoding="utf-8") as f:
            rows = csv.reader(f)
            hook.bulk_insert_rows(
                table="airflow.fake_vehicle_registrations",
                rows=rows,
                commit_every=100000,
                replace=True,
                autocommit=True,
            )
        hook.get_db_log_messages()

    @sql_task(conn_id="hana_default")
    def get_rows():
        return """
        SELECT *
        FROM airflow.fake_vehicle_registrations
        LIMIT 100;
        """

    (
        check_table_exists
        >> [create_table(), do_nothing]
        >> create_fake_data()
        >> insert_into_hana()
        >> get_rows()
    )


example_hana_dag()
