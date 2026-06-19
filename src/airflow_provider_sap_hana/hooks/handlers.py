from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook

T = TypeVar("T", bound="DbApiHook")


def fetch_many_handler(cursor: Any, fetchsize: int) -> list[tuple[Any, ...]] | None:
    """
    Fetch a specified number of rows.

    :param cursor: The cursor holding the result set.
    :param fetchsize: The number of rows to fetch.
    :return: A list of tuples.
    """
    if cursor.description is not None:
        return cursor.fetchmany(fetchsize)
    return None


def chunk_handler(
    hook: T, conn: Any, cursor: Any, chunksize: int
) -> Iterator[tuple[Any, ...] | list[tuple[Any, ...]]]:
    """
    Yield rows in batches.

    This allows you to process large datasets without loading
    all the data into memory at once. The hook attributes 'descriptions' and 'last_description' will be available without having to first call
    'next' on the generator returned by the 'get_records_by_chunks' method.

    :param hook: The DbApiHook class instance.
    :param conn: A connection object. The connection must be passed in as well as the cursor to
    ensure both resources are closed.
    :param cursor: A DBAPI cursor.
    :param chunksize: The number of records to return per chunk.
    :return: A generator yielding lists of tuples if chunksize > 1, tuples if chunksize set to 1.
    """
    nb_rows = 0
    make_common_data_structure = getattr(hook, "_make_common_data_structure")
    log = getattr(hook, "log")
    try:
        while results := make_common_data_structure(fetch_many_handler(cursor, chunksize)):
            nb_rows += len(results)
            log.info("Fetched %s rows so far", nb_rows)
            yield results
        else:
            log.info("Done fetching. Fetched %s total rows", nb_rows)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
