#!/bin/bash

set -euo pipefail

"${HDBSQL_PATH}" -a -x -i 90 -d HXE -u SYSTEM -p "${MASTER_PASSWORD}" -B UTF8 \
"
  DO
  BEGIN
    DECLARE schemaExists TINYINT := 0;

    SELECT COUNT(*) INTO schemaExists
    FROM sys.schemas
    WHERE schema_name = 'AIRFLOW';

    IF schemaExists = 0 THEN
      EXEC 'CREATE SCHEMA airflow';
    END IF;

  END;
"
