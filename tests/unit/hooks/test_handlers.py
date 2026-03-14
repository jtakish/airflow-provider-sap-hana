from __future__ import annotations

from unittest import mock

import pytest

from airflow_provider_sap_hana.hooks.handlers import fetch_many_handler, stream_handler


class TestFetchmanyHandler:
    @pytest.mark.parametrize("fetchsize", [1, 3, 5000])
    def test_fetch_many_handler(self, fetchsize, mock_cursor):
        mock_cursor.fetchmany = mock.Mock()
        fetch_many_handler(mock_cursor, fetchsize)
        mock_cursor.fetchmany.assert_called_once_with(fetchsize)

    def test_fetch_many_not_called(self, mock_cursor):
        mock_cursor.fetchmany = mock.Mock()
        mock_cursor.description = None
        fetch_many_handler(mock_cursor, 1)
        mock_cursor.fetchmany.assert_not_called()


class TestStreamHandler:
    def test_stream_handler_fetch_one(self, mock_hook, mock_conn):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_cursor = mock_conn.cursor()

        row_chunks = stream_handler(hook, mock_conn, mock_cursor, 1)
        next(row_chunks)
        mock_cursor.fetchone.assert_called_once()

        for _ in row_chunks:
            pass
        assert mock_cursor.fetchone.call_count == 6

    @pytest.mark.parametrize("chunksize, expected_call_count", [(2, 4), (3, 3), (5, 2)])
    def test_stream_handler_fetch_many(self, chunksize, expected_call_count, mock_hook, mock_conn):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_cursor = mock_conn.cursor()

        row_chunks = stream_handler(hook, mock_conn, mock_cursor, chunksize)
        next(row_chunks)
        mock_cursor.fetchmany.assert_called_once()

        for _ in row_chunks:
            pass
        assert mock_cursor.fetchmany.call_count == expected_call_count

    def test_stream_handler_resources_closed_normal(self, mock_hook, mock_conn):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_cursor = mock_conn.cursor()
        row_chunks = stream_handler(hook, mock_conn, mock_cursor, 1)
        for _ in row_chunks:
            pass
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_stream_handler_resources_closed_exception(self, mock_hook, mock_conn):
        hook = mock_hook
        hook.get_conn = mock.Mock(return_value=mock_conn)
        mock_cursor = mock_conn.cursor()
        mock_cursor.fetchone.side_effect = SystemExit("Something bad!")

        row_chunks = stream_handler(hook, mock_conn, mock_cursor, 1)
        with pytest.raises(SystemExit):
            next(row_chunks)
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
