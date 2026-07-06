from __future__ import annotations

from typing import TYPE_CHECKING
import os
import csv

from faker import Faker
from faker.providers import automotive, person
from pendulum import datetime

from airflow.sdk import dag, task, ObjectStoragePath
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLExecuteQueryOperator,
    SQLInsertRowsOperator,
)

if TYPE_CHECKING:
    from airflow.models.xcom import LazySelectSequence


def _read_tmp_csv(tmp_file: LazySelectSequence, **kwargs):
    for file in tmp_file:
        file: ObjectStoragePath
        with file.open() as f:
            reader = csv.reader(f)
            yield from reader
        os.remove(file.path)


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

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="hana_default",
        sql="""
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
          );""",
    )

    do_nothing = EmptyOperator(task_id="do_nothing")

    @task(trigger_rule="none_failed_min_one_success", map_index_template="Batch {{ tmp_file_suffix }}")
    def create_fake_data(tmp_file_suffix: int):

        fake = Faker()
        fake.add_provider(automotive)
        fake.add_provider(person)

        base = ObjectStoragePath(f"/tmp/fake_data_{tmp_file_suffix}.csv")

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

    fake_data = create_fake_data.expand(tmp_file_suffix=[n for n in range(1, 11)])

    insert_into_hana = SQLInsertRowsOperator(
        task_id="insert_into_hana",
        conn_id="hana_default",
        schema="airflow",
        table_name="fake_vehicle_registrations",
        rows_processor=_read_tmp_csv,
        rows=fake_data,
        insert_args={"replace": True, "fast_executemany": True, "commit_every": 10000},
    )
    check_table_exists >> [create_table, do_nothing] >> fake_data >> insert_into_hana


example_hana_dag()
