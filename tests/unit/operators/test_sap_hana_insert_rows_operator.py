from __future__ import annotations

from unittest import mock

import pytest


@pytest.mark.filterwarnings("ignore:The max_num_rendered_ti_fields_per_task option")
class TestSapHanaInsertRowsOperator:
    @pytest.mark.parametrize(
        "insert_args",
        [
            {"replace": True},
            {"replace": True, "commit_every": 5},
            {"replace": True, "commit_every": 5, "autocommit": False},
        ],
    )
    def test_insert_rows(self, insert_args, mock_insert_rows_operator, mock_insert_values):

        operator = mock_insert_rows_operator(
            table_name="mock.mock_table",
            rows=mock_insert_values,
            columns=["mock_1", "mock_2"],
            insert_args=insert_args,
        )

        hook = operator.get_db_hook()
        hook.bulk_insert_rows = mock.Mock()

        operator.execute({})
        hook.bulk_insert_rows.assert_called_once_with(
            table="mock.mock_table",
            rows=mock_insert_values,
            target_fields=["mock_1", "mock_2"],
            **insert_args,
        )

    def test_insert_rows_no_fail_with_executemany_args(
        self,
        mock_insert_rows_operator,
        mock_insert_values,
        mock_conn,
        mock_dml_cursor,
    ):

        operator = mock_insert_rows_operator(
            table_name="mock.mock_table",
            rows=mock_insert_values,
            columns=["mock_1", "mock_2"],
            insert_args={"commit_every": 5, "executemany": False, "fast_executemany": True},
        )

        hook = operator.get_db_hook()
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_conn.cursor.return_value = mock_dml_cursor

        operator.execute({})
        assert mock_dml_cursor.executemanyprepared.call_count == 4
