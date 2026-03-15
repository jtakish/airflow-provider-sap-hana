from __future__ import annotations

from collections.abc import Callable, Iterable
from functools import wraps
from typing import TYPE_CHECKING, Any

from airflow.providers.common.sql.hooks.handlers import fetch_one_handler

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook


def make_cursor_description_available_immediately(func: Callable):
    """
    Ensure that the cursor description is available immediately after executing the SQL statement.

    The hook attributes 'descriptions' and 'last_description' will be available without having to first call
    'next' on the generator returned by the 'get_records_by_chunks' method.

    :param func: The function to decorate, typically one that streams rows from a cursor.
    :return: The function which yields rows from a cursor.
    """

    @wraps(func)
    def wrapper(hook: DbApiHook, sql: str, parameters: Iterable, chunksize: int):
        conn = None
        cur = None
        try:
            conn = hook.get_conn()
            cur = conn.cursor()
            hook._run_command(cur, sql, parameters)
            hook.descriptions.append(cur.description)
        except Exception as e:
            if cur:
                cur.close()
            if conn:
                conn.close()
            raise e

        return func(hook, conn, cur, chunksize)

    return wrapper


def fetch_many_handler(cursor, fetchsize: int) -> list[tuple[Any]] | None:
    """
    Fetch a specified number of rows.

    :param cursor: The cursor holding the result set.
    :param fetchsize: The number of rows to fetch.
    :return: A list of tuples.
    """
    if cursor.description is not None:
        return cursor.fetchmany(fetchsize)
    return None


@make_cursor_description_available_immediately
def stream_handler(hook: DbApiHook, conn: Any, cursor: Any, chunksize: int):
    """
    Yield rows in batches.

    This allows you to process large datasets without loading
    all the data into memory at once. This method works in tandem with the
    'make_cursor_description_available_immediately' decorator.

    The hook attributes 'descriptions' and 'last_description' will be available without having to first call
    'next' on the generator returned by the 'get_records_by_chunks' method.

    :param hook: The DbApiHook class instance.
    :param conn: A connection object. The connection must be passed in as well as the cursor to
    ensure both resources are closed.
    :param cursor: A DBAPI cursor.
    :param chunksize: The number of records to return per chunk.
    :return: A generator yielding lists of tuples if chunksize > 1, tuples if chunksize set to 1.
    """
    if chunksize == 1:
        handler = fetch_one_handler
        handler_args = (cursor,)
    else:
        handler = fetch_many_handler
        handler_args = (cursor, chunksize)
    try:
        results = hook._make_common_data_structure(handler(*handler_args))
        while results:
            yield results
            results = hook._make_common_data_structure(handler(*handler_args))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
