from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import closing
from datetime import time
from textwrap import indent
from typing import TYPE_CHECKING, Any, cast

import hdbcli.dbapi
from methodtools import lru_cache
from more_itertools import chunked, peekable
from sqlalchemy import inspect
from sqlalchemy.engine.url import URL

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow_provider_sap_hana.hooks.handlers import chunk_handler

if TYPE_CHECKING:
    from hdbcli.dbapi import Connection as HDBCLIConnection, Cursor
    from hdbcli.resultrow import ResultRow
    from sqlalchemy_hana.dialect import HANAInspector

    from airflow.providers.common.compat.sdk import Connection
    from airflow.providers.common.sql.hooks.sql import T


class SapHanaHook(DbApiHook):
    """
    Interact with SAP HANA.

    Additional connection properties and SQLDBC properties can be passed
    as key-value pairs into the extra connection argument.

    :param replace_with_primary_key: If enabled, SAP HANA will use 'UPSERT {} VALUES ({}) WITH PRIMARY KEY'.
        If disabled, 'UPSERT {} {} VALUES ({})' will be used. Using the 'WITH PRIMARY KEY' clause is
        recommended syntax for SAP HANA and is significantly faster than using 'UPSERT' without it.
    :param enable_db_log_messages: If enabled, logs messages sent to the client during the session.
        The default options are 'SQL=INFO,FLUSH=ON'. To change the log level or other options,
        pass the ``traceOptions`` keyword argument into the extra connection argument.
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
        self._replace_statement_format = None

    @property
    def replace_statement_format(self) -> str:
        """
        Returns the UPSERT statement format.

        Using the 'WITH PRIMARY KEY' clause is recommended for SAP HANA, as it is
        orders of magnitude faster than standard 'UPSERT' syntax.
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
            connection: Connection = self.connection
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
        Get a SQLAlchemy Inspector.

        The Inspector used for SAP HANA is an instance of ``HANAInspector`` and offers
        an additional method to return the OID (object id) for a given table name.

        :return: A ``HANAInspector`` object.
        """
        engine = self.get_sqlalchemy_engine()
        return cast("HANAInspector", inspect(engine))

    def get_uri(self) -> str:
        return self.sqlalchemy_url.render_as_string(hide_password=True)

    def get_conn(self) -> HDBCLIConnection:
        """
        Connect to a SAP HANA database.

        The address, user, password, and port are extracted from the Airflow Connection.
        Additional connection properties and SQLDBC properties can be passed as key: value pairs into the extra
        connection argument.

        :return: a hdbcli ``Connection`` object.
        """
        sqlalchemy_url = self.sqlalchemy_url
        conn_args = sqlalchemy_url.translate_connect_args(
            host="address", username="user", database="databasename"
        )
        conn_args.update(sqlalchemy_url.query)
        trace_options = conn_args.pop("traceoptions", "SQL=INFO,FLUSH=ON")
        conn = hdbcli.dbapi.connect(**conn_args)
        if self.enable_db_log_messages:
            conn.ontrace(self._log_message, trace_options)
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
        Enable or disable autocommit.

        hdbcli uses an autocommit method and not an autocommit attribute.

        :param conn: a hdbcli ``Connection`` object to set autocommit.
        :param autocommit: bool.
        :return: None.
        """
        if self.supports_autocommit:
            conn.setautocommit(autocommit)

    def get_autocommit(self, conn: HDBCLIConnection) -> bool | None:
        """
        Get autocommit setting for the provided connection.

        hdbcli uses an autocommit method and not an autocommit attribute.

        :param conn: A hdbcli Connection object to get autocommit setting from.
        :return: connection autocommit setting. True if ``autocommit`` is set
            to True on the connection. False if it is either not set.
        """
        if self.supports_autocommit:
            return conn.getautocommit()
        return None

    @staticmethod
    def _make_resultrow_cell_serializable(cell: Any) -> Any:
        """
        Convert a ``ResultRow`` date value to string.

        This method makes SAP HANA result sets JSON serializable. ``time`` values
        are converted using the ``isoformat`` method. All other data types
        (str, int, float, None) remain unchanged.

        Note: This is used for data exiting SAP HANA via SELECT statements.
        The ``serialize_cells`` method is still used for data entering SAP HANA.

        :param cell: The input cell value.
        :return: The cell converted to a string if it is of ``time`` type,
            otherwise returns the original cell.
        """
        if isinstance(cell, time):
            return cell.isoformat()
        return cell

    @classmethod
    def _make_resultrow_common(cls, row: ResultRow) -> tuple[Any, ...]:
        """
        Convert a ``ResultRow`` object into a tuple.

        ``ResultRow`` objects are not JSON serializable, so they must be
        converted into a tuple for serialization.

        :param row: A ``ResultRow`` object.
        :return: A tuple containing the row data.
        """
        return tuple(map(cls._make_resultrow_cell_serializable, row))

    def _make_common_data_structure(self, result: T | Sequence[T]) -> tuple | list[tuple]:
        """
        Make SAP HANA result sets JSON serializable.

        ``ResultRow`` objects are not JSON serializable, so they must be
        converted into a tuple or a list of tuples.

        :param result: A list of ``ResultRow`` objects (for fetchall) or a
            single ``ResultRow`` (for fetchone).
        :return: A list of tuples or a single tuple.
        """
        if not result:
            return cast("tuple | list[tuple]", result)
        if isinstance(result, list):
            return list(map(self._make_resultrow_common, result))
        return self._make_resultrow_common(cast("ResultRow", result))

    def get_records_by_chunks(
        self, sql: str, parameters: Iterable[Any] | Mapping[str, Any] | None = None, chunksize: int = 10000
    ) -> Iterator[tuple[Any, ...] | list[tuple[Any, ...]]]:
        """
        Streams records from SAP HANA, yielding chunks of rows.

        This method allows for fetching large datasets without loading them all
        into memory. Each record is passed through ``_make_common_data_structure``
        to ensure JSON serialization. The ``descriptions`` and ``last_description``
        attributes are available immediately after execution.

        :param sql: The SQL statement.
        :param parameters: Parameters to bind to the SQL statement.
        :param chunksize: The number of records per chunk.
        :return: A generator yielding lists of tuples (or a single tuple if chunksize is 1).
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
        return chunk_handler(self, conn, cur, chunksize)

    def insert_rows(
        self,
        table,
        rows,
        target_fields=None,
        commit_every=1000,
        replace=False,
        *,
        executemany=False,
        fast_executemany=False,
        autocommit=False,
        **kwargs,
    ):
        """
        Insert records into SAP HANA using a prepared statement or executemany.

        hdbcli Cursors do not have a fast_executemany attribute, but it can be replicated using prepared statements.
        Prepared statements also have significantly less overhead due fewer calls to the database.

        :param table: The table name.
        :param rows: The rows to insert into the table.
        :param target_fields: The names of the columns to fill in the table.
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: If True, uses 'UPSERT' instead of 'INSERT' syntax.
        :param executemany: This method uses executemany by default.
        :param fast_executemany: If True, uses executemanyprepared.
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :return: None.
        """
        nb_rows = 0
        chunksize = None if not commit_every else commit_every
        peekable_rows = peekable(rows)
        sample_row = peekable_rows.peek()
        chunked_serialized_rows = chunked(map(self._serialize_cells, peekable_rows), chunksize)
        sql = self._generate_insert_sql(table, sample_row, target_fields, replace)
        with self._create_autocommit_connection(autocommit) as conn:
            with closing(conn.cursor()) as cur:
                cur: Cursor
                if fast_executemany:
                    cur.prepare(sql, newcursor=False)
                    if self.log_sql:
                        self.log.info("Prepared statement: %s", sql)

                for chunk in chunked_serialized_rows:
                    if fast_executemany:
                        cur.executemanyprepared(chunk)
                    else:
                        cur.executemany(sql, chunk)
                    if not autocommit:
                        conn.commit()
                    nb_rows += cur.rowcount
                    self.log.info("Loaded %s rows into %s so far", nb_rows, table)
        self.log.info("Done loading. Loaded a total of %s rows into %s", nb_rows, table)

    def get_db_log_messages(self, conn: None = None) -> None:
        if self.db_log_messages:
            self.log.info("".join(self.db_log_messages))
