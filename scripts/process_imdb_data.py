"""
Module for processing IMDb data and populating the PostgreSQL database.
"""

import csv
import logging
from typing import Any, List, Optional

import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def clean_value(value: str, convert_to_int: bool = False) -> Optional[Any]:
    """Cleans the value extracted from the CSV file.

    Args:
        value: The value to clean.
        convert_to_int: Whether to attempt to convert the value to an integer.

    Returns:
        The cleaned value, possibly converted to int, or None if the value is '\\N' or cannot be converted.
    """
    if value == '\\N':
        return None
    if convert_to_int:
        try:
            return int(value)
        except ValueError:
            return None
    return value


def process_title_basics(cursor: psycopg2.extensions.cursor) -> None:
    """Processes the 'title.basics.csv' file and inserts data into the 'movies' table.

    Args:
        cursor: The database cursor.
    """
    with open('data/title.basics.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        data = []
        for i, row in enumerate(reader, start=1):
            try:
                if len(row) < 9:
                    logging.warning(f"Row {i} in title.basics.csv has insufficient columns: {row}")
                    continue
                data.append((
                    row[0],  # id
                    row[2],  # title
                    row[1],  # type
                    clean_value(row[5], convert_to_int=True),  # start_year
                    clean_value(row[6], convert_to_int=True),  # end_year
                    clean_value(row[7], convert_to_int=True),  # runtime_minutes
                    row[8]   # genres
                ))
                if i % 10000 == 0:
                    logging.info(f"Processed {i} rows from title.basics.csv")
            except Exception as e:
                logging.error(f"Error processing row {i} in title.basics.csv: {e}")
                logging.error(f"Problematic row: {row}")
        logging.info(f"Inserting {len(data)} rows into movies table")
        execute_batch(cursor, """
            INSERT INTO movies (id, title, type, start_year, end_year, runtime, genres)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, data)


def process_title_ratings(cursor: psycopg2.extensions.cursor) -> None:
    """Processes the 'title.ratings.csv' file and updates the 'movies' table.

    Args:
        cursor: The database cursor.
    """
    with open('data/title.ratings.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        data = []
        for i, row in enumerate(reader, start=1):
            try:
                if len(row) < 3:
                    logging.warning(f"Row {i} in title.ratings.csv has insufficient columns: {row}")
                    continue
                data.append((
                    float(row[1]),  # average_rating
                    int(row[2]),    # num_votes
                    row[0]          # id
                ))
                if i % 10000 == 0:
                    logging.info(f"Processed {i} rows from title.ratings.csv")
            except Exception as e:
                logging.error(f"Error processing row {i} in title.ratings.csv: {e}")
                logging.error(f"Problematic row: {row}")
        logging.info(f"Updating {len(data)} rows in movies table with ratings")
        execute_batch(cursor, """
            UPDATE movies SET avg_rating = %s, num_votes = %s
            WHERE id = %s
        """, data)


def process_name_basics(cursor: psycopg2.extensions.cursor) -> None:
    """Processes the 'name.basics.csv' file and inserts data into the 'persons' table.

    Args:
        cursor: The database cursor.
    """
    with open('data/name.basics.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        data = []
        for i, row in enumerate(reader, start=1):
            try:
                if len(row) < 6:
                    logging.warning(f"Row {i} in name.basics.csv has insufficient columns: {row}")
                    continue
                data.append((
                    row[0],  # nconst
                    row[1],  # primaryName
                    clean_value(row[2], convert_to_int=True),  # birthYear
                    clean_value(row[3], convert_to_int=True),  # deathYear
                    row[4]   # primaryProfession
                ))
                if i % 10000 == 0:
                    logging.info(f"Processed {i} rows from name.basics.csv")
            except Exception as e:
                logging.error(f"Error processing row {i} in name.basics.csv: {e}")
                logging.error(f"Problematic row: {row}")
        logging.info(f"Inserting {len(data)} rows into persons table")
        execute_batch(cursor, """
            INSERT INTO persons (id, name, birth_year, death_year, primary_profession)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, data)


def process_title_crew(cursor: psycopg2.extensions.cursor) -> None:
    """Processes the 'title.crew.csv' file and inserts data into the 'movie_crew' table.

    Args:
        cursor: The database cursor.
    """
    with open('data/title.crew.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        data = []
        for i, row in enumerate(reader, start=1):
            try:
                if len(row) < 3:
                    logging.warning(f"Row {i} in title.crew.csv has insufficient columns: {row}")
                    continue
                movie_id = row[0]
                directors = row[1].split(',') if row[1] != '\\N' else []
                writers = row[2].split(',') if row[2] != '\\N' else []
                for director_id in directors:
                    data.append((
                        movie_id,
                        director_id,
                        'director'
                    ))
                for writer_id in writers:
                    data.append((
                        movie_id,
                        writer_id,
                        'writer'
                    ))
                if i % 10000 == 0:
                    logging.info(f"Processed {i} rows from title.crew.csv")
            except Exception as e:
                logging.error(f"Error processing row {i} in title.crew.csv: {e}")
                logging.error(f"Problematic row: {row}")
        logging.info(f"Inserting {len(data)} rows into movie_crew table")
        execute_batch(cursor, """
            INSERT INTO movie_crew (movie_id, person_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (movie_id, person_id, role) DO NOTHING
        """, data)


def main() -> None:
    """Main function to process IMDb data and populate the database."""
    conn = psycopg2.connect(
        dbname="recommendation_engine",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    try:
        process_title_basics(cursor)
        process_title_ratings(cursor)
        process_name_basics(cursor)
        process_title_crew(cursor)
        conn.commit()
        logging.info("Data processing completed successfully")
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred during data processing: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
