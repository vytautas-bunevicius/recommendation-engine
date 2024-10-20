"""Provides a Python API for executing SQL queries on the recommendation engine database.

This module contains the DatabaseAPI class, which offers methods for executing
read and write operations on the database using raw SQL queries.
"""

from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor


class DatabaseAPI:
    """A class to interact with the recommendation engine database.

    This class provides methods to execute read and write SQL queries on the
    database, returning results as dictionaries for easy data manipulation.

    Attributes:
        connection_params: A dictionary containing database connection parameters.
    """

    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        """Initializes the DatabaseAPI with connection parameters.

        Args:
            dbname: Name of the database.
            user: Username for database connection.
            password: Password for database connection.
            host: Host address of the database server.
            port: Port number for the database connection.
        """
        self.connection_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }

    def _get_connection(self) -> connection:
        """Creates and returns a new database connection.

        Returns:
            A new psycopg2 connection object.

        Raises:
            psycopg2.Error: If unable to connect to the database.
        """
        return psycopg2.connect(**self.connection_params, cursor_factory=RealDictCursor)

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """Executes a read query on the database.

        Args:
            query: The SQL query string to execute.
            params: Optional tuple of parameters to pass to the query.

        Returns:
            A list of dictionaries, where each dictionary represents a row in the result set.

        Raises:
            psycopg2.Error: If there's an error executing the query.
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def execute_write(self, query: str, params: Optional[Tuple] = None) -> int:
        """Executes a write query (INSERT, UPDATE, DELETE) on the database.

        Args:
            query: The SQL query string to execute.
            params: Optional tuple of parameters to pass to the query.

        Returns:
            The number of rows affected by the query.

        Raises:
            psycopg2.Error: If there's an error executing the query.
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
                return cur.rowcount

    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """Executes a batch write operation on the database.

            This method is useful for inserting or updating multiple rows efficiently.

            Args:
                query: The SQL query string to execute.
                params_list: A list of tuples, where each tuple contains parameters
                    for one execution of the query.

            Returns:
                The total number of rows affected by the batch operation.

            Raises:
                psycopg2.Error: If there's an error executing the batch operation.
            """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, params_list)
                conn.commit()
                return cur.rowcount


def get_db_api() -> DatabaseAPI:
    """Factory function to create a DatabaseAPI instance with default connection parameters.

    Returns:
        An instance of DatabaseAPI configured with default connection parameters.
    """
    return DatabaseAPI(
        dbname="recommendation_engine",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
