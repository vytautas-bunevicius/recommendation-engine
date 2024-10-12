"""
Database connection module for the recommendation_engine project.
Provides a function to obtain a connection to the PostgreSQL database.
"""

import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_connection() -> psycopg2.extensions.connection:
    """Establishes and returns a connection to the PostgreSQL database.

    Returns:
        A connection object to the database.
    """
    return psycopg2.connect(
        dbname="recommendation_engine",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        cursor_factory=RealDictCursor
    )
