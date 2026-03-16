from __future__ import annotations

from collections import deque
from collections.abc import Generator, Iterable, Iterator, Mapping, Sequence
from contextlib import closing
from datetime import datetime
from textwrap import indent
from typing import TYPE_CHECKING, Any

import hdbcli.dbapi
from deprecated import deprecated
from methodtools import lru_cache
from more_itertools import chunked
from sqlalchemy import inspect
from sqlalchemy.engine.url import URL

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow_provider_sap_hana.hooks.handlers import stream_handler

if TYPE_CHECKING:
    from hdbcli.dbapi import Connection as HDBCLIConnection
    from hdbcli.resultrow import ResultRow
    from sqlalchemy_hana.dialect import HANAInspector

    from airflow.providers.common.sql.hooks.sql import T


class SapHanaHook(DbApiHook):
    """
    Interact with SAP HANA.

    Additional connection properties and SQLDBC properties can be passed as key-value pairs into the extra connection argument.

    :param replace_with_primary_key: If enabled, SAP HANA will use 'UPSERT {} VALUES ({}) WITH PRIMARY KEY'. If disabled, 'UPSERT {} {} VALUES ({})' will be used.

        Using the 'WITH PRIMARY KEY' clause is recommended syntax for SAP HANA. This is orders of magnitude faster
        than using 'UPSERT' without the additional clause.

    :param enable_db_log_messages: If enabled, logs messages sent to the client during the session. The default options
        are 'SQL=INFO,FLUSH=ON'. To change the log level or other options, pass the 'traceOptions'
        keyword argument into the extra connection argument.
    """

    conn_name_attr = "hana_conn_id"
    default_conn_name = "hana_default"
    conn_type = "hana"
    hook_name = "SAP HANA"
    supports_autocommit = True
    supports_executemany = True
    _test_connection_sql = "SELECT 1 FROM dummy"
    _placeholder = "?"
    sqlalchemy_scheme = "hana"
    ignore_extra_options = ["databasename"]

    def __init__(
        self, *args, replace_with_primary_key: bool = True, enable_db_log_messages: bool = False, **kwargs
    ) -> None:
        if kwargs.get("schema"):
            kwargs["database"] = kwargs["schema"]
        self.database = kwargs.pop("database", None)
        super().__init__(*args, **kwargs)
        self.replace_with_primary_key = replace_with_primary_key
        self.enable_db_log_messages = enable_db_log_messages
        self.db_log_messages: deque = deque(maxlen=50)
        self._sqlalchemy_url = None

    @property
    def replace_statement_format(self) -> str:
        """
        SAP HANA uses 'UPSERT' as it's replace statement.

        Using the 'WITH PRIMARY KEY' clause is recommended syntax for SAP HANA. This is orders of magnitude faster
        than using 'UPSERT' without the additional clause.
        """
        if self._replace_statement_format is None:
            if self.replace_with_primary_key:
                replace_stmt = "UPSERT {} {} VALUES ({}) WITH PRIMARY KEY"
            else:
                replace_stmt = "UPSERT {} {} VALUES ({})"
            self._replace_statement_format = replace_stmt
        return self._replace_statement_format

    @lru_cache(maxsize=None)
    def get_reserved_words(self, dialect_name: str) -> set[str]:
        from sqlalchemy_hana.dialect import RESERVED_WORDS

        result = set(RESERVED_WORDS)
        self.log.debug("reserved words for '%s': %s", dialect_name, result)
        return result

    @property
    def sqlalchemy_url(self) -> URL:
        if not self._sqlalchemy_url:
            connection = self.connection
            query = {}
            for key, val in self.connection_extra_lower.items():
                if key not in self.ignore_extra_options:
                    query[key] = val
            self._sqlalchemy_url = URL.create(
                drivername=self.sqlalchemy_scheme,
                host=connection.host,
                username=connection.login,
                password=connection.password,
                port=connection.port,
                database=self.database or connection.schema,
                query=query,
            )
        return self._sqlalchemy_url

    @property
    def inspector(self) -> HANAInspector:
        """
        Override the DbApiHook 'inspector' property.

        The Inspector used for SAP HANA is an
        instance of HANAInspector and offers an additional method,
        which returns the OID (object id) for the given table name.

        :return: A HANAInspector object.
        """
        engine = self.get_sqlalchemy_engine()
        return inspect(engine)

    def get_uri(self):
        return self.sqlalchemy_url.render_as_string(hide_password=True)

    def get_conn(self) -> HDBCLIConnection:
        """
        Connect to a SAP HANA database.

        The address, user, password, and port are extracted from the Airflow Connection.
        Additional connection properties and SQLDBC properties can be passed as key: value pairs into the extra
        connection argument.

        :return: A hdbcli Connection object.
        """
        sqlalchemy_url = self.sqlalchemy_url
        conn_args = sqlalchemy_url.translate_connect_args(
            host="address", username="user", database="databasename"
        )
        conn_args.update(sqlalchemy_url.query)
        trace_options = conn_args.pop("traceoptions", "SQL=INFO,FLUSH=ON")
        conn = hdbcli.dbapi.connect(**conn_args)
        if self.enable_db_log_messages:
            conn.ontrace(self._log_message, trace_options)  # noqa: hdbcli says ontrace takes no arguments but it does
        return conn

    def _log_message(self, message: str) -> None:
        lines = message.splitlines(True)
        if lines and "libSQLDBCHDB" in lines[0]:
            lines[0] = "\n" + lines[0]
        joined = "".join(lines)
        indented = indent(joined, prefix="    ")
        self.db_log_messages.append(indented)

    def set_autocommit(self, conn: HDBCLIConnection, autocommit: bool) -> None:
        """
        Override the DbApiHook 'set_autocommit' method.

        hdbcli uses an autocommit method and not an autocommit attribute.

        :param conn: A hdbcli Connection object to set autocommit.
        :param autocommit: bool.
        :return: None.
        """
        if self.supports_autocommit:
            conn.setautocommit(autocommit)

    def get_autocommit(self, conn: HDBCLIConnection) -> bool | None:
        """
        Override the DbApiHook 'set_autocommit' method.

        hdbcli uses an autocommit method and not an autocommit attribute.

        :param conn: A hdbcli Connection object to get autocommit setting from.
        :return: bool.
        """
        if self.supports_autocommit:
            return conn.getautocommit()
        return None

    @staticmethod
    def _make_resultrow_cell_serializable(cell: Any) -> Any:
        """
        Convert a ResultRow datetime value to string.

        This is a custom method to make SAP HANA result sets JSON serializable. This method differs from the
        DbApiHook method 'serialize_cells' in that this method is intended to work with data exiting SAP HANA via
        SELECT statements. Datimetime values are converted to str using the datetime 'isoformat' method. All other
        data types (str, int, float, None) are unchanged.

        The DbApiHook method 'serialize_cells' is still called when data is entering SAP HANA via DML statements.

        :param cell: The input cell, which can be of any type.
        :return: The input `cell`, converted to a string if it is a `datetime`, or unchanged if it is of any other type
        """
        if isinstance(cell, datetime):
            return cell.isoformat()
        return cell

    @classmethod
    def _make_resultrow_common(cls, row: ResultRow) -> tuple:
        """
        Convert a ResultRow into a common tuple.

        This is a custom method to make SAP HANA result sets JSON serializable.
        ResultRow objects are not JSON serializable so they must be converted into a tuple.

        :param row: ResultRow object.
        :return: A common row tuple
        """
        return tuple(map(cls._make_resultrow_cell_serializable, row))

    def _make_common_data_structure(self, result: T | Sequence[T]) -> tuple | list[tuple] | None:
        """
        Override the DbApiHook '_make_common_data_structure' method.

        This is a custom method to make SAP HANA result sets JSON serializable.
        ResultRow objects are not JSON serializable so they must be converted into a tuple or a list of tuples.

        :param result: A list of ResultRow objects if the 'fetchall' handler is used,
        a single ResultRow if the 'fetchone' handler is used.
        :return: A list of tuples if the 'fetchall' handler is used. A single tuple if the 'fetchone' handler is used.
        """
        if not result:
            return result
        if isinstance(result, Sequence):
            return list(map(self._make_resultrow_common, result))
        return self._make_resultrow_common(result)

    @deprecated(
        reason=(
            "The 'get_primary_keys' method on the Hook class will soon be deprecated."
            "Please use the similarly named method on the dialect class."
        ),
        category=AirflowProviderDeprecationWarning,
    )
    def get_primary_keys(self, table: str, schema: str | None = None) -> list[str] | None:
        """
        Get the primary key or primary keys for a given table.

        :param table: The table name.
        :param schema: The schema where the table is located.
        :return: A list of primary keys or None if the table cannot be found or has no primary keys.
        """
        return self.dialect.get_primary_keys(table, schema)

    @deprecated(
        reason=(
            "The 'stream_records' method has been replaced by the 'get_records_by_chunks' method. The 'stream_rows'"
            "method is available for backwards compatibility but will be removed in the next release"
        ),
        category=AirflowProviderDeprecationWarning,
    )
    def stream_records(
        self, sql: str, parameters: Iterable | Mapping[str, Any] | None = None
    ) -> Generator[tuple[Any]]:
        return self.get_records_by_chunks(sql, parameters, chunksize=1)

    def get_records_by_chunks(
        self, sql: str, parameters: Iterable | Mapping[str, Any] | None = None, chunksize: int = 10000
    ) -> Generator[tuple[Any]]:
        """
        Streams records from SAP HANA, yielding chunks of rows.

        This is a custom method to fetch large amounts of records without loading them all into memory at once.
        Each record is passed through the '_make_common_data_structure' method to ensure it is JSON serializable.
        The hook attributes 'descriptions' and 'last_description' are available immediately after executing the
        SQL statement, without having to first call 'next' on the iterator.

        :param sql: The SQL statement.
        :param parameters: The parameters to be bound to the SQL statement.
        :param chunksize: The number of records per chunk.
        :return: A generator yielding lists of tuples if chunksize > 1, tuples if chunksize set to 1.
        """
        self.descriptions = []
        conn = None
        cur = None
        try:
            conn = self.get_conn()
            cur = conn.cursor()
            self._run_command(cur, sql, parameters)
            self.descriptions.append(cur.description)
        except Exception as e:
            if cur:
                cur.close()
            if conn:
                conn.close()
            raise e
        return stream_handler(self, conn, cur, chunksize)

    def bulk_insert_rows(
        self,
        table: str,
        rows: Sequence[Any] | Iterator[Any],
        target_fields: list | None = None,
        commit_every: int = 10000,
        *,
        replace: bool = False,
        autocommit: bool = True,
    ) -> None:
        """
        Insert records into SAP HANA using a prepared statement.

        This is a custom method to insert records as efficiently as possible.
        hdbcli Cursors do not have a 'fast_executemany' attribute, but it can be replicated using prepared statements.
        Prepared statements also have significantly less overhead due fewer calls to the database.

        :param table: The table name.
        :param rows: The rows to insert into the table.
        :param target_fields: The names of the columns to fill in the table.
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert.
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :return: None.
        """
        nb_rows = 0
        chunksize = None if not commit_every else commit_every
        chunked_serialized_rows = chunked(map(self._serialize_cells, rows), chunksize)
        with self._create_autocommit_connection(autocommit) as conn:
            with closing(conn.cursor()) as cur:
                sql = self._generate_insert_sql(table, rows[0], target_fields, replace)
                cur.prepare(sql, newcursor=False)
                if self.log_sql:
                    self.log.info("Prepared statement: %s", sql)

                for chunk in chunked_serialized_rows:
                    cur.executemanyprepared(chunk)
                    if not autocommit:
                        conn.commit()
                    nb_rows += cur.rowcount
                    self.log.info("Loaded %s rows into %s so far", nb_rows, table)
        self.log.info("Done loading. Loaded a total of %s rows into %s", nb_rows, table)

    def get_db_log_messages(self, conn: None = None) -> None:
        if self.db_log_messages:
            self.log.info("".join(self.db_log_messages))
