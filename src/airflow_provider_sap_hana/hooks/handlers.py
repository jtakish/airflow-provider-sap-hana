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
    :return: A list of tuples or None.
    """
    if cursor.description is not None:
        return cursor.fetchmany(fetchsize)
    return None


def chunk_handler(
    hook: T, conn: Any, cursor: Any, chunksize: int
) -> Iterator[tuple[Any, ...] | list[tuple[Any, ...]]]:
    """
    Yield rows in batches.

    This allows for processing large datasets without loading all data into memory.
    The ``descriptions`` and ``last_description`` attributes of the hook are
    available immediately after calling this method.

    :param hook: The ``DbApiHook`` instance.
    :param conn: The database connection object.
    :param cursor: The database cursor.
    :param chunksize: The number of records to return per chunk.
    :return: A generator yielding lists of tuples.
    """
    nb_rows = 0
    make_common_data_structure = hook._make_common_data_structure
    log = hook.log
    try:
        while results := make_common_data_structure(fetch_many_handler(cursor, chunksize)):
            nb_rows += len(results)
            log.info("Fetched %s rows so far", nb_rows)
            yield results
        log.info("Done fetching. Fetched %s total rows", nb_rows)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
