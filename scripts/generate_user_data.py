"""
Module for generating synthetic user data and populating the PostgreSQL database.
"""

import random
from typing import List

import psycopg2
from faker import Faker
from psycopg2.extras import execute_batch

fake = Faker()


def generate_users(cursor: psycopg2.extensions.cursor, num_users: int = 10000) -> List[str]:
    """Generates synthetic users and inserts them into the 'users' table.

    Args:
        cursor: The database cursor.
        num_users: Number of users to generate.

    Returns:
        List of user IDs.
    """
    users = []
    for _ in range(num_users):
        user_id = fake.uuid4()
        name = fake.name()
        birth_year = fake.date_of_birth(minimum_age=18, maximum_age=80).year
        users.append((user_id, name, birth_year))
    execute_batch(cursor, """
        INSERT INTO users (id, name, birth_year)
        VALUES (%s, %s, %s)
    """, users)
    return [user[0] for user in users]


def generate_viewing_history(
    cursor: psycopg2.extensions.cursor,
    user_ids: List[str],
    num_views: int = 100000
) -> None:
    """Generates synthetic viewing history for users.

    Args:
        cursor: The database cursor.
        user_ids: List of user IDs.
        num_views: Number of viewing events to generate.
    """
    cursor.execute("SELECT id FROM movies")
    movie_ids = [row[0] for row in cursor.fetchall()]
    if not movie_ids:
        raise ValueError("No movies found in the database.")

    viewing_history = []
    for _ in range(num_views):
        user_id = random.choice(user_ids)
        movie_id = random.choice(movie_ids)
        watch_date = fake.date_between(start_date='-1y', end_date='today')
        watch_duration = random.randint(10, 180)  
        viewing_history.append((user_id, movie_id, watch_date, watch_duration))
    execute_batch(cursor, """
        INSERT INTO viewing_history (user_id, movie_id, watch_date, watch_duration)
        VALUES (%s, %s, %s, %s)
    """, viewing_history)


def main() -> None:
    """Main function to generate synthetic user data and populate the database."""
    conn = psycopg2.connect(
        dbname="recommendation_engine",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    try:
        user_ids = generate_users(cursor)
        generate_viewing_history(cursor, user_ids)
        conn.commit()
        print("Synthetic user data generation completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred during data generation: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
