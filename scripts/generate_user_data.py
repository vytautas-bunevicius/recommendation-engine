"""
Module for generating synthetic user data and populating an AWS RDS PostgreSQL database.

This module provides functionality to create synthetic users and their viewing history,
which can be used for testing and development purposes in a movie recommendation system.
It connects to an AWS RDS PostgreSQL database using a centralized connection function.

The module uses the Faker library to generate realistic user data and the psycopg2
library to interact with the PostgreSQL database. It implements batch processing
to optimize database insertions, especially considering potential network latency
when working with a remote database.

Dependencies:
    - psycopg2
    - Faker
    - Custom database connection module (`src.database.connection`)

Usage:
    Run this script directly to generate synthetic user data and viewing history
    in the connected AWS RDS PostgreSQL database.

        $ python3 generate_user_data.py
"""

import argparse
import logging
import random
import traceback
from typing import List

import psycopg2
from faker import Faker
from psycopg2.extras import DictCursor, execute_values

from src.database.connection import get_db_connection

fake = Faker()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler("user_data_generation.log"),
        logging.StreamHandler()
    ]
)


def generate_users(cursor: psycopg2.extensions.cursor, num_users: int = 10000) -> List[str]:
    """Generates synthetic users and inserts them into the 'users' table.

    This function creates a specified number of synthetic users with randomly generated
    data including UUID, name, and birth year. It uses batch processing to optimize
    database insertions, especially useful for remote databases like AWS RDS.

    Args:
        cursor (psycopg2.extensions.cursor): The database cursor.
        num_users (int, optional): Number of users to generate. Defaults to 10000.

    Returns:
        List[str]: A list of generated user IDs.

    Raises:
        psycopg2.Error: If there's an error during database insertion.
    """
    users = [
        (fake.uuid4(), fake.name(), fake.date_of_birth(minimum_age=18, maximum_age=80).year)
        for _ in range(num_users)
    ]

    batch_size = 1000
    user_ids = []
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        try:
            execute_values(
                cursor,
                """
                INSERT INTO users (id, name, birth_year)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
                RETURNING id
                """,
                batch,
                page_size=batch_size
            )
            inserted = cursor.fetchall()
            if inserted:
                user_ids.extend([row['id'] for row in inserted])
            else:
                logging.warning(f"No new users inserted in batch {i // batch_size + 1}. Using generated IDs.")
                user_ids.extend([user[0] for user in batch])
            logging.info(f"Processed batch {i // batch_size + 1}: {len(batch)} users (total unique: {len(set(user_ids))})")
        except psycopg2.Error as e:
            logging.error(f"Error inserting user batch {i // batch_size + 1}: {e}")
            raise

    unique_user_ids = list(set(user_ids))
    logging.info(f"Total unique users generated: {len(unique_user_ids)}")
    return unique_user_ids


def generate_viewing_history(
    cursor: psycopg2.extensions.cursor,
    user_ids: List[str],
    num_views: int = 100000
) -> None:
    """Generates synthetic viewing history for users.

    This function creates a specified number of viewing history records, randomly
    assigning movies to users with synthetic watch dates and durations. It uses
    batch processing for efficient database insertion.

    Args:
        cursor (psycopg2.extensions.cursor): The database cursor.
        user_ids (List[str]): List of user IDs to generate history for.
        num_views (int, optional): Number of viewing events to generate. Defaults to 100000.

    Raises:
        ValueError: If no movies are found in the database.
        psycopg2.Error: If there's an error during database insertion.
    """
    cursor.execute("SELECT id FROM movies")
    movie_ids = [row['id'] for row in cursor.fetchall()]
    logging.info(f"Retrieved {len(movie_ids)} movie IDs from the database.")
    if not movie_ids:
        raise ValueError("No movies found in the database.")

    viewing_history = [
        (
            random.choice(user_ids),
            random.choice(movie_ids),
            fake.date_between(start_date='-1y', end_date='today'),
            random.randint(10, 180)
        )
        for _ in range(num_views)
    ]

    batch_size = 1000
    for i in range(0, len(viewing_history), batch_size):
        batch = viewing_history[i:i + batch_size]
        try:
            execute_values(
                cursor,
                """
                INSERT INTO viewing_history (user_id, movie_id, watch_date, watch_duration)
                VALUES %s
                """,
                batch,
                page_size=batch_size
            )
            logging.info(f"Inserted {len(batch)} viewing history records (total: {i + len(batch)})")
        except psycopg2.Error as e:
            logging.error(f"Error inserting viewing history batch {i // batch_size + 1}: {e}")
            raise


def verify_movies(cursor: psycopg2.extensions.cursor) -> int:
    """Verify that there are movies in the database and return the count.

    Args:
        cursor (psycopg2.extensions.cursor): The database cursor.

    Returns:
        int: The count of movies in the database.

    Raises:
        ValueError: If no movies are found in the database.
    """
    cursor.execute("SELECT COUNT(*) FROM movies")
    movie_count = cursor.fetchone()[0]
    logging.info(f"Total movies in database: {movie_count}")
    if movie_count == 0:
        raise ValueError("No movies found in the database. Please import movie data first.")
    return movie_count


def main() -> None:
    """Main function to generate synthetic user data and populate the database.

    This function orchestrates the process of generating synthetic users and their
    viewing history. It handles database connection, commits the transaction, and
    ensures proper error handling and resource cleanup.

    Raises:
        psycopg2.Error: If a database-related error occurs.
        Exception: For any other unexpected errors during execution.
    """
    parser = argparse.ArgumentParser(description="Generate synthetic user data.")
    parser.add_argument(
        '--num_users',
        type=int,
        default=10000,
        help='Number of users to generate'
    )
    parser.add_argument(
        '--num_views',
        type=int,
        default=100000,
        help='Number of viewing history records to generate'
    )
    args = parser.parse_args()

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        logging.info("Starting user data generation...")
        user_ids = generate_users(cursor, num_users=args.num_users)
        logging.info(f"Generated {len(user_ids)} unique users.")

        logging.info("Verifying user count in database...")
        cursor.execute("SELECT COUNT(*) FROM users")
        db_user_count = cursor.fetchone()[0]
        logging.info(f"Total users in database: {db_user_count}")

        if not user_ids:
            raise ValueError("No users were generated or inserted.")

        logging.info("Verifying movie data...")
        verify_movies(cursor)

        logging.info("Starting viewing history generation...")
        generate_viewing_history(cursor, user_ids, num_views=args.num_views)
        logging.info("Viewing history generation completed.")

        conn.commit()
        logging.info("Synthetic user data generation completed successfully.")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logging.error(f"Database error occurred: {e}")
        logging.error(f"Error details: {traceback.format_exc()}")
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"An error occurred during data generation: {e}")
        logging.error(f"Error details: {traceback.format_exc()}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
