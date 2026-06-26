from __future__ import annotations

from airflow_provider_sap_hana import __version__


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-sap-hana",
        "name": "SAP HANA Airflow Provider",
        "description": "An Airflow provider to connect to SAP HANA",
        "hooks": [
            {"integration-name": "SAP Hana", "python-modules": ["airflow_provider_sap_hana.hooks.hana"]}
        ],
        "connection-types": [
            {
                "connection-type": "hana",
                "hook-class-name": "airflow_provider_sap_hana.hooks.hana.SapHanaHook",
            }
        ],
        "versions": [__version__],
    }
