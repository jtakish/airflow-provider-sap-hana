from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from airflow.providers.common.sql.operators.sql import SQLInsertRowsOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context
    from airflow_provider_sap_hana.hooks.hana import SapHanaHook


class SapHanaInsertRowsOperator(SQLInsertRowsOperator):
    """
    Insert rows into SAP HANA.

    This operator inherits from `SQLInsertRowsOperator` and overrides the _insert_rows method to utilize
    the SapHanaHook bulk_insert_rows method.
    """

    def _insert_rows(self, rows: Any | Iterable[Any], context: Context):
        if self._rows_processor:
            rows = self._rows_processor(rows, **context)

        hook: SapHanaHook = self.get_db_hook()

        hook.bulk_insert_rows(
            table=self.table_name_with_schema,
            rows=rows,
            target_fields=self.column_names,
            **self.insert_args,
        )
