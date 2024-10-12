import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    return psycopg2.connect(
        dbname="recommendation_engine",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        cursor_factory=RealDictCursor
    )
