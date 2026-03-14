from __future__ import annotations

import datetime
from unittest import mock

import importlib_metadata as md
import pytest
from hdbcli.dbapi import Connection as HDBCLIConnection, Cursor
from hdbcli.resultrow import ResultRow
from packaging.version import Version

from airflow.models.connection import Connection
from airflow_provider_sap_hana.hooks.hana import SapHanaHook


@pytest.fixture
def is_sqlalchemy_v2():
    sa_version = Version(md.version("sqlalchemy"))
    return sa_version.major >= 2


@pytest.fixture
def mock_connection():
    return Connection(
        conn_type="hana",
        conn_id="hana_mock",
        host="hanahost",
        login="user",
        password="pass123",
        port=12345,
    )


@pytest.fixture
def mock_hook(mock_connection):
    hook = SapHanaHook()
    hook.get_connection = mock.Mock(return_value=mock_connection)
    return hook


@pytest.fixture
def mock_resultrows():
    epoch_ts = datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0, microsecond=123456)
    epoch_ts_add_day = epoch_ts + datetime.timedelta(days=1)
    epoch_ts_add_week = epoch_ts + datetime.timedelta(days=7)
    column_names = ("MOCK_STRING", "MOCK_INT", "MOCK_FLOAT", "MOCK_DATETIME", "MOCK_NONE")
    return [
        ResultRow(column_names=column_names, column_values=("test123", 123, 123.00, epoch_ts, None)),
        ResultRow(column_names=column_names, column_values=("test456", 456, 456.00, epoch_ts_add_day, None)),
        ResultRow(column_names=column_names, column_values=("test789", 789, 789.00, epoch_ts_add_week, None)),
    ]


@pytest.fixture
def mock_insert_values():
    return [("mock1", "mock2") for _ in range(20)]


@pytest.fixture
def mock_cursor(mock_resultrows):
    result_iterator = iter(mock_resultrows)
    cur = mock.MagicMock(spec=Cursor, rowcount=-1)
    cur.__iter__.return_value = result_iterator
    cur.fetchone.side_effect = lambda: next(result_iterator, None)
    cur.fetchall.side_effect = lambda: list(result_iterator)
    cur.description = (
        ("MOCK_STRING",),
        ("MOCK_INT",),
        ("MOCK_FLOAT",),
        ("MOCK_DATETIME",),
        ("MOCK_NONE",),
    )
    return cur


@pytest.fixture
def mock_conn(mock_cursor):
    conn = mock.MagicMock(spec=HDBCLIConnection)
    conn.cursor.return_value = mock_cursor

    return conn


@pytest.fixture
def mock_dml_cursor():
    cur = mock.MagicMock(spec=Cursor, rowcount=-1)
    cur.executemany.side_effect = lambda sql, values: setattr(cur, "rowcount", len(values))
    cur.executemanyprepared.side_effect = lambda values: setattr(cur, "rowcount", len(values))
    cur.description = None
    return cur
