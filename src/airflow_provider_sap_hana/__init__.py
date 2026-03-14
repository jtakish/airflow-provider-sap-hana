from __future__ import annotations

import packaging.version

from airflow import __version__ as airflow_version

__version__ = "1.5"

__all__ = ["__version__"]

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse(
    "3.0.0"
):
    raise RuntimeError(f"The package `airflow-providersap-hana-:{__version__}` needs Apache Airflow 3.0.0+")
