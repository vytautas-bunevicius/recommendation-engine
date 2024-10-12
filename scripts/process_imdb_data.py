"""
Module for processing movie data and importing into a PostgreSQL database.

This script reads CSV files containing movie data, processes them, and imports
the data into a PostgreSQL database. It handles data cleaning, type conversion,
and batch insertion to optimize performance while avoiding duplicate entries.
The script is designed to be idempotent, allowing multiple runs without
creating duplicate data.
"""

import csv
import logging
from typing import Any, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def clean_value(
    value: str,
    convert_to_int: bool = False,
    convert_to_bool: bool = False
) -> Optional[Any]:
    """Clean and convert a string value."""
    if value == '\\N':
        return None
    if convert_to_bool:
        return value == '1'
    if convert_to_int:
        try:
            return int(value)
        except ValueError:
            return None
    return value


def process_title_basics(cur: cursor, conn: connection) -> Tuple[int, int]:
    """Process the title.basics.csv file and insert/update data in the database."""
    try:
        with open('data/title.basics.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            batch_size = 10000
            data_batch = []
            total_processed = 0
            new_records = 0
            updated_records = 0

            for i, row in enumerate(reader, start=1):
                try:
                    if len(row) < 9:
                        logging.warning(f"Row {i} in title.basics.csv has insufficient columns: {row}")
                        continue
                    data_batch.append((
                        row[0],  # id
                        row[2],  # title
                        row[3],  # original_title
                        row[1],  # type
                        clean_value(row[4], convert_to_bool=True),  # is_adult
                        clean_value(row[5], convert_to_int=True),  # start_year
                        clean_value(row[6], convert_to_int=True),  # end_year
                        clean_value(row[7], convert_to_int=True),  # runtime
                        row[8]   # genres
                    ))
                    if len(data_batch) >= batch_size:
                        n, u = insert_or_update_movies(cur, data_batch)
                        new_records += n
                        updated_records += u
                        conn.commit()
                        total_processed += len(data_batch)
                        logging.info(f"Processed {total_processed} rows from title.basics.csv")
                        data_batch = []
                except Exception as e:
                    logging.error(f"Error processing row {i} in title.basics.csv: {e}")
                    logging.error(f"Problematic row: {row}")
                    conn.rollback()

            if data_batch:
                n, u = insert_or_update_movies(cur, data_batch)
                new_records += n
                updated_records += u
                conn.commit()
                total_processed += len(data_batch)

        logging.info(f"Total rows processed from title.basics.csv: {total_processed}")
        logging.info(f"New records inserted: {new_records}")
        logging.info(f"Existing records updated: {updated_records}")
        return new_records, updated_records
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred during title.basics processing: {e}")
        raise


def insert_or_update_movies(cur: cursor, data: list) -> Tuple[int, int]:
    """Insert new movies or update existing ones."""
    cur.execute("""
        CREATE TEMPORARY TABLE temp_movies (
            id VARCHAR(10),
            title VARCHAR(512),
            original_title VARCHAR(512),
            type VARCHAR(50),
            is_adult BOOLEAN,
            start_year INTEGER,
            end_year INTEGER,
            runtime INTEGER,
            genres VARCHAR(255)
        ) ON COMMIT DROP
    """)

    execute_values(cur, """
        INSERT INTO temp_movies (id, title, original_title, type, is_adult, start_year, end_year, runtime, genres)
        VALUES %s
    """, data)

    cur.execute("""
        WITH upsert AS (
            INSERT INTO movies (id, title, original_title, type, is_adult, start_year, end_year, runtime, genres)
            SELECT id, title, original_title, type, is_adult, start_year, end_year, runtime, genres
            FROM temp_movies
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                original_title = EXCLUDED.original_title,
                type = EXCLUDED.type,
                is_adult = EXCLUDED.is_adult,
                start_year = EXCLUDED.start_year,
                end_year = EXCLUDED.end_year,
                runtime = EXCLUDED.runtime,
                genres = EXCLUDED.genres
            RETURNING (xmax = 0) AS inserted
        )
        SELECT COUNT(*) FILTER (WHERE inserted), COUNT(*) FILTER (WHERE NOT inserted)
        FROM upsert
    """)

    return cur.fetchone()


def process_title_ratings(cur: cursor, conn: connection) -> Tuple[int, int]:
    """Process the title.ratings.csv file and update movie ratings."""
    try:
        with open('data/title.ratings.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            batch_size = 10000
            data_batch = []
            total_processed = 0
            new_ratings = 0
            updated_ratings = 0

            for i, row in enumerate(reader, start=1):
                try:
                    if len(row) < 3:
                        logging.warning(f"Row {i} in title.ratings.csv has insufficient columns: {row}")
                        continue
                    data_batch.append((
                        row[0],  # movie_id
                        float(row[1]),  # avg_rating
                        int(row[2])  # num_votes
                    ))
                    if len(data_batch) >= batch_size:
                        n, u = update_movie_ratings(cur, data_batch)
                        new_ratings += n
                        updated_ratings += u
                        conn.commit()
                        total_processed += len(data_batch)
                        logging.info(f"Processed {total_processed} rows from title.ratings.csv")
                        data_batch = []
                except Exception as e:
                    logging.error(f"Error processing row {i} in title.ratings.csv: {e}")
                    logging.error(f"Problematic row: {row}")
                    conn.rollback()

            if data_batch:
                n, u = update_movie_ratings(cur, data_batch)
                new_ratings += n
                updated_ratings += u
                conn.commit()
                total_processed += len(data_batch)

        logging.info(f"Total rows processed from title.ratings.csv: {total_processed}")
        logging.info(f"New ratings added: {new_ratings}")
        logging.info(f"Existing ratings updated: {updated_ratings}")
        return new_ratings, updated_ratings
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred during title.ratings processing: {e}")
        raise


def update_movie_ratings(cur: cursor, data: list) -> Tuple[int, int]:
    """Update movie ratings or add new ones."""
    cur.execute("""
        CREATE TEMPORARY TABLE temp_ratings (
            movie_id VARCHAR(10),
            avg_rating FLOAT,
            num_votes INTEGER
        ) ON COMMIT DROP
    """)

    execute_values(cur, """
        INSERT INTO temp_ratings (movie_id, avg_rating, num_votes)
        VALUES %s
    """, data)

    cur.execute("""
        WITH upsert AS (
            UPDATE movies m
            SET avg_rating = r.avg_rating,
                num_votes = r.num_votes
            FROM temp_ratings r
            WHERE m.id = r.movie_id
            RETURNING m.id
        )
        SELECT
            (SELECT COUNT(*) FROM temp_ratings) - COUNT(*) as new_ratings,
            COUNT(*) as updated_ratings
        FROM upsert
    """)

    return cur.fetchone()


def process_name_basics(cur: cursor, conn: connection) -> Tuple[int, int]:
    """Process the name.basics.csv file and insert/update data in the database."""
    try:
        with open('data/name.basics.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            batch_size = 10000
            data_batch = []
            total_processed = 0
            new_persons = 0
            updated_persons = 0

            for i, row in enumerate(reader, start=1):
                try:
                    if len(row) < 6:
                        logging.warning(f"Row {i} in name.basics.csv has insufficient columns: {row}")
                        continue
                    data_batch.append((
                        row[0],  # id
                        row[1],  # name
                        clean_value(row[2], convert_to_int=True),  # birth_year
                        clean_value(row[3], convert_to_int=True),  # death_year
                        row[4]  # primary_profession
                    ))
                    if len(data_batch) >= batch_size:
                        n, u = insert_or_update_persons(cur, data_batch)
                        new_persons += n
                        updated_persons += u
                        conn.commit()
                        total_processed += len(data_batch)
                        logging.info(f"Processed {total_processed} rows from name.basics.csv")
                        data_batch = []
                except Exception as e:
                    logging.error(f"Error processing row {i} in name.basics.csv: {e}")
                    logging.error(f"Problematic row: {row}")
                    conn.rollback()

            if data_batch:
                n, u = insert_or_update_persons(cur, data_batch)
                new_persons += n
                updated_persons += u
                conn.commit()
                total_processed += len(data_batch)

        logging.info(f"Total rows processed from name.basics.csv: {total_processed}")
        logging.info(f"New persons inserted: {new_persons}")
        logging.info(f"Existing persons updated: {updated_persons}")
        return new_persons, updated_persons
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred during name.basics processing: {e}")
        raise


def insert_or_update_persons(cur: cursor, data: list) -> Tuple[int, int]:
    """Insert new persons or update existing ones."""
    cur.execute("""
        CREATE TEMPORARY TABLE temp_persons (
            id VARCHAR(10),
            name VARCHAR(255),
            birth_year INTEGER,
            death_year INTEGER,
            primary_profession VARCHAR(255)
        ) ON COMMIT DROP
    """)

    execute_values(cur, """
        INSERT INTO temp_persons (id, name, birth_year, death_year, primary_profession)
        VALUES %s
    """, data)

    cur.execute("""
        WITH upsert AS (
            INSERT INTO persons (id, name, birth_year, death_year, primary_profession)
            SELECT id, name, birth_year, death_year, primary_profession
            FROM temp_persons
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                birth_year = EXCLUDED.birth_year,
                death_year = EXCLUDED.death_year,
                primary_profession = EXCLUDED.primary_profession
            RETURNING (xmax = 0) AS inserted
        )
        SELECT COUNT(*) FILTER (WHERE inserted), COUNT(*) FILTER (WHERE NOT inserted)
        FROM upsert
    """)

    return cur.fetchone()


def process_title_crew(cur: cursor, conn: connection) -> Tuple[int, int]:
    """Process the title.crew.csv file and insert/update data in the database."""
    try:
        with open('data/title.crew.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            batch_size = 10000
            data_batch = []
            total_processed = 0
            new_crew = 0
            existing_crew = 0

            for i, row in enumerate(reader, start=1):
                try:
                    if len(row) < 3:
                        logging.warning(f"Row {i} in title.crew.csv has insufficient columns: {row}")
                        continue
                    movie_id = row[0]
                    directors = row[1].split(',') if row[1] != '\\N' else []
                    writers = row[2].split(',') if row[2] != '\\N' else []
                    for director_id in directors:
                        data_batch.append((movie_id, director_id, 'director'))
                    for writer_id in writers:
                        data_batch.append((movie_id, writer_id, 'writer'))
                    if len(data_batch) >= batch_size:
                        n, e = insert_movie_crew(cur, data_batch)
                        new_crew += n
                        existing_crew += e
                        conn.commit()
                        total_processed += len(data_batch)
                        logging.info(f"Processed {total_processed} rows from title.crew.csv")
                        data_batch = []
                except Exception as e:
                    logging.error(f"Error processing row {i} in title.crew.csv: {e}")
                    logging.error(f"Problematic row: {row}")
                    conn.rollback()

            if data_batch:
                n, e = insert_movie_crew(cur, data_batch)
                new_crew += n
                existing_crew += e
                conn.commit()
                total_processed += len(data_batch)

        logging.info(f"Total crew relationships processed from title.crew.csv: {total_processed}")
        logging.info(f"New crew relationships inserted: {new_crew}")
        logging.info(f"Existing crew relationships: {existing_crew}")
        return new_crew, existing_crew
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred during title.crew processing: {e}")
        raise


def insert_movie_crew(cur: cursor, data: list) -> Tuple[int, int]:
    """Insert new movie crew relationships, ignoring existing ones."""
    cur.execute("""
        CREATE TEMPORARY TABLE temp_movie_crew (
            movie_id VARCHAR(10),
            person_id VARCHAR(10),
            role VARCHAR(50)
        ) ON COMMIT DROP
    """)

    execute_values(cur, """
        INSERT INTO temp_movie_crew (movie_id, person_id, role)
        VALUES %s
    """, data)

    cur.execute("""
        WITH inserted AS (
            INSERT INTO movie_crew (movie_id, person_id, role)
            SELECT t.movie_id, t.person_id, t.role
            FROM temp_movie_crew t
            JOIN movies m ON t.movie_id = m.id
            JOIN persons p ON t.person_id = p.id
            ON CONFLICT (movie_id, person_id, role) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) as new_inserted,
               (SELECT COUNT(*) FROM temp_movie_crew) - COUNT(*) as already_existing
        FROM inserted
    """)

    return cur.fetchone()


def verify_data(cur: cursor) -> None:
    """Verify the data in the database tables."""
    tables = ['movies', 'persons', 'movie_crew']
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        logging.info(f"Total rows in {table} table: {count}")


def process_data(conn: connection, cur: cursor) -> None:
    """Process all data and import into the database."""
    try:
        logging.info("Starting data processing")

        logging.info("Processing movies data")
        new_movies, updated_movies = process_title_basics(cur, conn)
        logging.info(f"Movies processed: {new_movies} new, {updated_movies} updated")

        logging.info("Processing movie ratings")
        new_ratings, updated_ratings = process_title_ratings(cur, conn)
        logging.info(f"Ratings processed: {new_ratings} new, {updated_ratings} updated")

        logging.info("Processing persons data")
        new_persons, updated_persons = process_name_basics(cur, conn)
        logging.info(f"Persons processed: {new_persons} new, {updated_persons} updated")

        logging.info("Processing movie crew data")
        new_crew, existing_crew = process_title_crew(cur, conn)
        logging.info(f"Crew relationships processed: {new_crew} new, {existing_crew} existing")

        logging.info("Data processing completed successfully")
        verify_data(cur)
    except Exception as e:
        logging.error(f"An error occurred during data processing: {e}")
        raise


def main() -> None:
    """Main function to initiate data processing."""
    conn = psycopg2.connect(
        dbname="recommendation_engine",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    try:
        process_data(conn, cur)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
