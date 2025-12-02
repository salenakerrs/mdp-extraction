"""Connectivity Test Utilities Module."""

# import: standard
import json
import logging

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import DB_TYPE_MAPPING
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import EnvSettings
from mdp.framework.mdp_extraction_framework.task.data_extractor.mongodb_data_extractor import (
    MongoDatabaseConnector,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    OdbcDatabaseConnector,
)
from mdp.framework.mdp_extraction_framework.utility.common_function import read_file

# import: external
from sqlalchemy import text

logger = logging.getLogger("connectivity_test")


def ConnectivityTest(
    connection_name: str, query: str = None, query_file_path: str = None
) -> tuple[str, list]:
    """Test connectivity.

    Args:
        connection_name (str): The name of the connection to test.
        query (str, optional): The query to execute. Defaults to None.
        query_file_path (str, optional): The path to the query file to execute. Defaults to None.

    Returns:
        tuple[str, list]: A tuple containing the status of the connection test, and the query results
    """

    logger.info("Starting execution of Test Connectivity Module.")
    try:
        # Get Connection Info
        env_file = EnvSettings()

        connection_data = env_file.connection_info.get(connection_name)

        dbtype = connection_data.get("dbtype")
        if not dbtype:
            raise ValueError(f"dbtype not defined for connection '{connection_name}'")

        settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
        if not settings_class:
            raise ValueError(f"Unsupported dbtype '{dbtype}' for connection '{connection_name}'")

        connection_info = settings_class(**connection_data)

        connection_test_status = "FAILED"
        query_result = None

        # Connect to MongoDB
        if dbtype.lower() in ["mongodb", "mongodbsrv"]:
            connector = MongoDatabaseConnector(connection_info=connection_info)

            connection_test_status = "SUCCESS"
            # Execute the query
            if query:
                try:
                    query_dict = json.loads(query.strip(), object_hook=connector._json_object_hook)

                except (json.JSONDecodeError, ValueError) as e:
                    logger.error(f"Invalid MongoDB query string, failed to parse query string: {e}")
                    raise ValueError(f"Invalid MongoDB query string: {query}")

                query_result = list(connector.collection.aggregate(query_dict, allowDiskUse=True))

            if query_file_path:
                template_query = read_file(query_file_path)
                try:
                    query_dict = json.loads(query.strip(), object_hook=connector._json_object_hook)
                except (json.JSONDecodeError, ValueError) as e:
                    logger.error(f"Invalid MongoDB query file, failed to parse query string: {e}")
                    raise ValueError(f"Invalid MongoDB query file: {query_file_path}")

                query_result = list(connector.collection.aggregate(query_dict, allowDiskUse=True))

        # Connect to ODBC Database
        else:
            connector = OdbcDatabaseConnector(connection_info=connection_info)

            with connector.engine.connect() as connection:
                connection_test_status = "SUCCESS"
                # Execute the query
                if query:
                    query_result = connection.execute(text(query)).fetchall()

                if query_file_path:
                    template_query = read_file(query_file_path)
                    query_result = connection.execute(text(template_query)).fetchall()

            connector.engine.dispose()

        return connection_test_status, query_result

    except Exception as e:
        raise e
