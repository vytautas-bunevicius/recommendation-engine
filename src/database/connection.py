"""Database connection module for the recommendation_engine project.

This module provides functions to establish and close connections to the AWS RDS PostgreSQL database.
"""

import os
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_db_connection() -> Optional[psycopg2.extensions.connection]:
    """Establishes and returns a connection to the AWS RDS PostgreSQL database.

    Returns:
        Optional[psycopg2.extensions.connection]: A database connection object if successful,
                                                  None if connection fails.

    Raises:
        psycopg2.Error: If there's an error connecting to the database.
    """
    try:
        return psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'recommendation_engine'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432'),
            cursor_factory=RealDictCursor
        )
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        return None


def close_db_connection(conn: Optional[psycopg2.extensions.connection]) -> None:
    """Closes the given database connection.

    Args:
        conn (Optional[psycopg2.extensions.connection]): The database connection to close.
    """
    if conn:
        conn.close()
        print("Database connection closed.")
