from __future__ import annotations

import csv
from textwrap import dedent
from faker import Faker
from faker.providers import automotive, person
from pendulum import datetime

from airflow.providers.common.sql.decorators.sql import sql_task
from airflow_provider_sap_hana.operators.hana import SapHanaInsertRowsOperator
from airflow.sdk import dag, task, get_current_context, ObjectStoragePath


@dag(
    dag_id="example_hana_dag",
    start_date=datetime(2024, 12, 20),
    schedule="@once",
    max_active_runs=1,
    catchup=False,
)
def example_hana_dag():

    @task
    def create_fake_data():
        fake = Faker()
        fake.add_provider(automotive)
        fake.add_provider(person)

        base = ObjectStoragePath("/tmp/fake_data.csv")

        with base.open(mode="w", encoding="utf-8") as f:
            writer = csv.writer(f)
            for _ in range(1000000):
                writer.writerow(
                    (
                        fake.vin(),
                        fake.first_name().upper(),
                        fake.last_name().upper(),
                        fake.street_address().upper(),
                        fake.city().upper(),
                        fake.state_abbr().upper(),
                        fake.postalcode().upper(),
                        "US",
                        fake.date_time_this_decade().isoformat(),
                    )
                )
        return base

    @task
    def insert_into_hana(tmp_file: ObjectStoragePath, **kwargs):

        ctx = get_current_context()
        task_id = ctx["task"].task_id

        with tmp_file.open(mode="r", encoding="utf-8") as f:
            data = csv.reader(f)
            _ = next(data)
            rows = list(data)

        create_stmt = dedent("""
            DO
            BEGIN
                DECLARE tableExists TINYINT := 0;

                SELECT COUNT(*) INTO tableExists
                FROM sys.tables
                WHERE
                    schema_name = 'AIRFLOW'
                    AND table_name = 'FAKE_VEHICLE_REGISTRATIONS';

                IF tableExists = 0 THEN EXEC
                    'CREATE TABLE airflow.fake_vehicle_registrations (
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
                      )';
                END IF;
            END;""")
        operator = SapHanaInsertRowsOperator(
            task_id=task_id,
            conn_id="hana_default",
            table_name="fake_vehicle_registrations",
            schema="airflow",
            rows=rows,
            insert_args={"replace": True, "commit_every": 10000},
            preoperator=create_stmt,
        )
        operator.execute(ctx)

    @sql_task(conn_id="hana_default")
    def get_rows():
        stmt = dedent("""
            SELECT *
            FROM airflow.fake_vehicle_registrations
            LIMIT 100;""")
        return stmt

    insert_into_hana(create_fake_data()) >> get_rows()


example_hana_dag()
