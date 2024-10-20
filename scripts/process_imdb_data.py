"""Module for processing movie data and importing it into an AWS RDS PostgreSQL database.

This script efficiently reads multiple large CSV files containing movie data,
processes them in parallel batches to maintain data integrity, and imports the data into
an AWS RDS PostgreSQL database. It handles data cleaning, type conversion, and
batch insertion to optimize performance while avoiding duplicate entries. The
script is designed to be idempotent, allowing multiple runs without creating
duplicate data. It leverages connection pooling for efficient database interactions,
uses Pydantic for data validation, and incorporates environment variable configurations
and enhanced logging for monitoring.

Usage:
    python3 scripts/process_imdb_data.py
"""

import csv
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Generator, List, Optional, Tuple, Set

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, field_validator


load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler("data_processing.log"),
        logging.StreamHandler()
    ]
)

DATA_DIR: str = os.getenv('DATA_DIR', 'data')
BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '5000'))
MAX_WORKERS: int = int(os.getenv('MAX_WORKERS', '5'))
MAX_ENTRIES_PER_FILE: int = 100000

DB_USER: str = os.getenv('DB_USER')
DB_PASSWORD: str = os.getenv('DB_PASSWORD')
DB_HOST: str = os.getenv('DB_HOST')
DB_PORT: str = os.getenv('DB_PORT', '5432')
DB_NAME: str = os.getenv('DB_NAME')


class Movie(BaseModel):
    """Model representing a movie entity."""

    id: str
    title: str
    original_title: Optional[str]
    type: Optional[str]
    is_adult: Optional[bool]
    start_year: Optional[int]
    end_year: Optional[int]
    runtime: Optional[int]
    genres: Optional[str]

    @field_validator('is_adult', mode='before')
    def validate_is_adult(cls, value: Any) -> Optional[bool]:
        """Validate and convert the 'is_adult' field to a boolean."""
        return value == '1' if isinstance(value, str) else value


class Rating(BaseModel):
    """Model representing a movie rating."""

    movie_id: str
    avg_rating: float
    num_votes: int


class Person(BaseModel):
    """Model representing a person (e.g., actor, director)."""

    id: str
    name: str
    birth_year: Optional[int]
    death_year: Optional[int]
    professions: Optional[str]


class Crew(BaseModel):
    """Model representing a crew relationship between a movie and a person."""

    movie_id: str
    person_id: str
    role: str


def clean_value(
    value: str,
    convert_to_int: bool = False,
    convert_to_bool: bool = False
) -> Optional[Any]:
    """Clean and convert a string value.

    Args:
        value: The input string to clean and potentially convert.
        convert_to_int: If True, attempt to convert the value to an integer.
        convert_to_bool: If True, convert the value to a boolean.

    Returns:
        The cleaned and potentially converted value, or None if the input is invalid.
    """
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


def read_csv_in_batches(
    file_path: str,
    has_header: bool = True,
    max_entries: Optional[int] = None
) -> Generator[List[List[str]], None, None]:
    """Generator to read a CSV file in batches, optionally limiting to max_entries.

    Args:
        file_path: The path to the CSV file.
        has_header: Indicates if the CSV file has a header row.
        max_entries: The maximum number of rows to read from the CSV file.

    Yields:
        A batch of rows from the CSV file.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        if has_header:
            next(reader, None)
        batch: List[List[str]] = []
        count = 0
        for row in reader:
            if row:
                batch.append(row)
                count += 1
                if len(batch) == BATCH_SIZE:
                    yield batch
                    batch = []
                if max_entries and count >= max_entries:
                    break
        if batch and (not max_entries or count <= max_entries):
            yield batch


def insert_into_db(
    insert_query: str,
    data: List[Tuple],
    cursor: psycopg2.extensions.cursor
) -> int:
    """Execute a batch insert into the database and return the number of inserted rows.

    Args:
        insert_query: The SQL insert query with a RETURNING clause.
        data: The data to insert.
        cursor: The database cursor.

    Returns:
        The number of rows successfully inserted.
    """
    execute_values(cursor, insert_query, data)
    inserted_rows = cursor.fetchall()
    inserted_ids = [row[0] for row in inserted_rows]
    logging.debug(f"Inserted IDs: {inserted_ids}")
    return len(inserted_rows)


def get_existing_ids(
    table: str,
    db_pool: pool.SimpleConnectionPool
) -> Set[str]:
    """Fetch existing IDs from the specified table.

    Args:
        table: The name of the table to fetch IDs from.
        db_pool: The database connection pool.

    Returns:
        A set of existing IDs in the specified table.
    """
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {table}")
            return set(row[0] for row in cur.fetchall())
    finally:
        db_pool.putconn(conn)


def process_batch_title_basics(
    batch: List[List[str]],
    insert_query: str,
    db_pool: pool.SimpleConnectionPool,
    batch_number: int,
    existing_ids: Set[str],
    existing_ids_lock: threading.Lock
) -> Tuple[int, int]:
    """Process a single batch from title.basics.csv.

    Args:
        batch: The batch of rows to process.
        insert_query: The SQL insert query for movies.
        db_pool: The database connection pool.
        batch_number: The batch number for logging.
        existing_ids: A set of existing movie IDs.
        existing_ids_lock: A threading.Lock object for synchronizing access to existing_ids.

    Returns:
        A tuple containing the count of inserted and skipped records.
    """
    inserted, skipped = 0, 0
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            data_batch: List[Tuple[Any, ...]] = []
            for row in batch:
                if len(row) < 9:
                    logging.warning(f"Batch {batch_number}: Insufficient columns in row: {row}")
                    continue
                try:
                    movie = Movie(
                        id=row[0],
                        title=row[2],
                        original_title=row[3],
                        type=row[1],
                        is_adult=clean_value(row[4], convert_to_bool=True),
                        start_year=clean_value(row[5], convert_to_int=True),
                        end_year=clean_value(row[6], convert_to_int=True),
                        runtime=clean_value(row[7], convert_to_int=True),
                        genres=row[8]
                    )
                    with existing_ids_lock:
                        if movie.id not in existing_ids:
                            data_batch.append((
                                movie.id,
                                movie.title,
                                movie.original_title,
                                movie.type,
                                movie.is_adult,
                                movie.start_year,
                                movie.end_year,
                                movie.runtime,
                                movie.genres
                            ))
                            existing_ids.add(movie.id)
                        else:
                            skipped += 1
                except ValidationError as ve:
                    logging.error(f"Batch {batch_number}: Validation error for row {row}: {ve}")
            if data_batch:
                inserted = insert_into_db(insert_query, data_batch, cur)
                conn.commit()
                logging.info(f"Batch {batch_number}: Inserted {inserted} new movies, skipped {skipped}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Batch {batch_number}: Error processing title.basics.csv: {e}", exc_info=True)
    finally:
        db_pool.putconn(conn)
    return inserted, skipped


def process_title_basics(
    file_path: str,
    insert_query: str,
    db_pool: pool.SimpleConnectionPool,
    existing_ids_lock: threading.Lock,
    max_entries: Optional[int] = MAX_ENTRIES_PER_FILE
) -> Tuple[int, int]:
    """Process the title.basics.csv file and insert/update data in the database.

    Args:
        file_path: The path to the title.basics.csv file.
        insert_query: The SQL insert query for movies.
        db_pool: The database connection pool.
        existing_ids_lock: A threading.Lock object for synchronizing access to existing_ids.
        max_entries: The maximum number of entries to process.

    Returns:
        A tuple containing the count of new records and skipped duplicates.
    """
    logging.info(f"Started processing file: {file_path} with a cap of {max_entries} entries")
    new_records, skipped_records = 0, 0
    existing_ids = get_existing_ids('movies', db_pool)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for batch_number, batch in enumerate(read_csv_in_batches(file_path, max_entries=max_entries), start=1):
            futures.append(
                executor.submit(
                    process_batch_title_basics,
                    batch,
                    insert_query,
                    db_pool,
                    batch_number,
                    existing_ids,
                    existing_ids_lock
                )
            )
        for future in as_completed(futures):
            try:
                inserted, skipped = future.result()
                new_records += inserted
                skipped_records += skipped
            except Exception as e:
                logging.error(f"Error in processing batch: {e}", exc_info=True)
    logging.info(f"title.basics.csv - Total New: {new_records}, Total Skipped: {skipped_records}")
    return new_records, skipped_records


def process_batch_title_ratings(
    batch: List[List[str]],
    insert_query: str,
    db_pool: pool.SimpleConnectionPool,
    batch_number: int,
    existing_ids: Set[str],
    existing_ids_lock: threading.Lock
) -> Tuple[int, int]:
    """Process a single batch from title.ratings.csv.

    Args:
        batch: The batch of rows to process.
        insert_query: The SQL insert query for movie ratings.
        db_pool: The database connection pool.
        batch_number: The batch number for logging.
        existing_ids: A set of existing movie IDs.
        existing_ids_lock: A threading.Lock object for synchronizing access to existing_ids.

    Returns:
        A tuple containing the count of inserted and skipped records.
    """
    inserted, skipped = 0, 0
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            data_batch: List[Tuple[Any, ...]] = []
            for row in batch:
                if len(row) < 3:
                    logging.warning(f"Batch {batch_number}: Insufficient columns in row: {row}")
                    continue
                try:
                    rating = Rating(
                        movie_id=row[0],
                        avg_rating=float(row[1]),
                        num_votes=int(row[2])
                    )
                    with existing_ids_lock:
                        if rating.movie_id in existing_ids:
                            data_batch.append((
                                rating.movie_id,
                                rating.avg_rating,
                                rating.num_votes
                            ))
                        else:
                            skipped += 1
                except (ValueError, ValidationError) as ve:
                    logging.error(f"Batch {batch_number}: Validation error for row {row}: {ve}")
            if data_batch:
                inserted = insert_into_db(insert_query, data_batch, cur)
                conn.commit()
                logging.info(f"Batch {batch_number}: Inserted/Updated {inserted} ratings, skipped {skipped}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Batch {batch_number}: Error processing title.ratings.csv: {e}", exc_info=True)
    finally:
        db_pool.putconn(conn)
    return inserted, skipped


def process_title_ratings(
    file_path: str,
    insert_query: str,
    db_pool: pool.SimpleConnectionPool,
    max_entries: Optional[int] = MAX_ENTRIES_PER_FILE
) -> Tuple[int, int]:
    """Process the title.ratings.csv file and update movie ratings.

    Args:
        file_path: The path to the title.ratings.csv file.
        insert_query: The SQL insert query for movie ratings.
        db_pool: The database connection pool.
        max_entries: The maximum number of entries to process.

    Returns:
        A tuple containing the count of new ratings and skipped duplicates.
    """
    logging.info(f"Started processing file: {file_path} with a cap of {max_entries} entries")
    new_ratings, skipped_ratings = 0, 0
    existing_ids = get_existing_ids('movies', db_pool)
    existing_ids_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for batch_number, batch in enumerate(read_csv_in_batches(file_path, max_entries=max_entries), start=1):
            futures.append(
                executor.submit(
                    process_batch_title_ratings,
                    batch,
                    insert_query,
                    db_pool,
                    batch_number,
                    existing_ids,
                    existing_ids_lock
                )
            )
        for future in as_completed(futures):
            try:
                inserted, skipped = future.result()
                new_ratings += inserted
                skipped_ratings += skipped
            except Exception as e:
                logging.error(f"Error in processing batch: {e}", exc_info=True)
    logging.info(f"title.ratings.csv - Total New/Updated: {new_ratings}, Total Skipped: {skipped_ratings}")
    return new_ratings, skipped_ratings


def process_batch_name_basics(
    batch: List[List[str]],
    insert_query: str,
    db_pool: pool.SimpleConnectionPool,
    batch_number: int,
    existing_ids: Set[str],
    existing_ids_lock: threading.Lock
) -> Tuple[int, int]:
    """Process a single batch from name.basics.csv.

    Args:
        batch: The batch of rows to process.
        insert_query: The SQL insert query for persons.
        db_pool: The database connection pool.
        batch_number: The batch number for logging.
        existing_ids: A set of existing person IDs.
        existing_ids_lock: A threading.Lock object for synchronizing access to existing_ids.

    Returns:
        A tuple containing the count of inserted and skipped records.
    """
    inserted, skipped = 0, 0
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            data_batch: List[Tuple[Any, ...]] = []
            for row in batch:
                if len(row) < 6:
                    logging.warning(f"Batch {batch_number}: Insufficient columns in row: {row}")
                    continue
                try:
                    person = Person(
                        id=row[0],
                        name=row[1],
                        birth_year=clean_value(row[2], convert_to_int=True),
                        death_year=clean_value(row[3], convert_to_int=True),
                        professions=row[4]
                    )
                    with existing_ids_lock:
                        if person.id not in existing_ids:
                            data_batch.append((
                                person.id,
                                person.name,
                                person.birth_year,
                                person.death_year,
                                person.professions
                            ))
                            existing_ids.add(person.id)
                        else:
                            skipped += 1
                except ValidationError as ve:
                    logging.error(f"Batch {batch_number}: Validation error for row {row}: {ve}")
            if data_batch:
                inserted = insert_into_db(insert_query, data_batch, cur)
                conn.commit()
                logging.info(f"Batch {batch_number}: Inserted {inserted} new persons, skipped {skipped}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Batch {batch_number}: Error processing name.basics.csv: {e}", exc_info=True)
    finally:
        db_pool.putconn(conn)
    return inserted, skipped


def process_name_basics(
    file_path: str,
    insert_query: str,
    db_pool: pool.SimpleConnectionPool,
    existing_ids_lock: threading.Lock,
    max_entries: Optional[int] = MAX_ENTRIES_PER_FILE
) -> Tuple[int, int]:
    """Process the name.basics.csv file and insert/update data in the database.

    Args:
        file_path: The path to the name.basics.csv file.
        insert_query: The SQL insert query for persons.
        db_pool: The database connection pool.
        existing_ids_lock: A threading.Lock object for synchronizing access to existing_ids.
        max_entries: The maximum number of entries to process.

    Returns:
        A tuple containing the count of new persons and skipped duplicates.
    """
    logging.info(f"Started processing file: {file_path} with a cap of {max_entries} entries")
    new_persons, skipped_persons = 0, 0
    existing_ids = get_existing_ids('persons', db_pool)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for batch_number, batch in enumerate(read_csv_in_batches(file_path, max_entries=max_entries), start=1):
            futures.append(
                executor.submit(
                    process_batch_name_basics,
                    batch,
                    insert_query,
                    db_pool,
                    batch_number,
                    existing_ids,
                    existing_ids_lock
                )
            )
        for future in as_completed(futures):
            try:
                inserted, skipped = future.result()
                new_persons += inserted
                skipped_persons += skipped
            except Exception as e:
                logging.error(f"Error in processing batch: {e}", exc_info=True)
    logging.info(f"name.basics.csv - Total New: {new_persons}, Total Skipped: {skipped_persons}")
    return new_persons, skipped_persons


def process_batch_title_crew(
    batch: List[List[str]],
    insert_query: str,
    db_pool: pool.SimpleConnectionPool,
    batch_number: int,
    existing_movie_ids: Set[str],
    existing_person_ids: Set[str],
    movie_ids_lock: threading.Lock,
    person_ids_lock: threading.Lock
) -> Tuple[int, int]:
    """Process a single batch from title.crew.csv.

    Args:
        batch: The batch of rows to process.
        insert_query: The SQL insert query for movie crew.
        db_pool: The database connection pool.
        batch_number: The batch number for logging.
        existing_movie_ids: A set of existing movie IDs.
        existing_person_ids: A set of existing person IDs.
        movie_ids_lock: Lock for synchronizing access to existing_movie_ids.
        person_ids_lock: Lock for synchronizing access to existing_person_ids.

    Returns:
        A tuple containing the count of inserted and skipped records.
    """
    inserted, skipped = 0, 0
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            crew_entries: List[Tuple[str, str, str]] = []
            for row in batch:
                if len(row) < 3:
                    logging.warning(f"Batch {batch_number}: Insufficient columns in row: {row}")
                    continue
                movie_id = row[0]
                with movie_ids_lock:
                    if movie_id not in existing_movie_ids:
                        skipped += 1
                        continue
                directors = row[1].split(',') if row[1] != '\\N' else []
                writers = row[2].split(',') if row[2] != '\\N' else []
                for director_id in directors:
                    with person_ids_lock:
                        if director_id in existing_person_ids:
                            crew_entries.append((movie_id, director_id, 'director'))
                        else:
                            logging.warning(f"Batch {batch_number}: Director ID {director_id} not found in persons table.")
                            skipped += 1
                for writer_id in writers:
                    with person_ids_lock:
                        if writer_id in existing_person_ids:
                            crew_entries.append((movie_id, writer_id, 'writer'))
                        else:
                            logging.warning(f"Batch {batch_number}: Writer ID {writer_id} not found in persons table.")
                            skipped += 1
            if crew_entries:
                inserted = insert_into_db(insert_query, crew_entries, cur)
                conn.commit()
                logging.info(f"Batch {batch_number}: Inserted {inserted} new crew relationships, skipped {skipped}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Batch {batch_number}: Error processing title.crew.csv: {e}", exc_info=True)
    finally:
        db_pool.putconn(conn)
    return inserted, skipped


def process_title_crew(
    file_path: str,
    insert_query: str,
    db_pool: pool.SimpleConnectionPool,
    max_entries: Optional[int] = MAX_ENTRIES_PER_FILE
) -> Tuple[int, int]:
    """Process the title.crew.csv file and insert/update data in the database.

    Args:
        file_path: The path to the title.crew.csv file.
        insert_query: The SQL insert query for movie crew.
        db_pool: The database connection pool.
        max_entries: The maximum number of entries to process.

    Returns:
        A tuple containing the count of new crew relationships and skipped duplicates.
    """
    logging.info(f"Started processing file: {file_path} with a cap of {max_entries} entries")
    new_crew, skipped_crew = 0, 0
    existing_movie_ids = get_existing_ids('movies', db_pool)
    existing_person_ids = get_existing_ids('persons', db_pool)
    movie_ids_lock = threading.Lock()
    person_ids_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for batch_number, batch in enumerate(read_csv_in_batches(file_path, max_entries=max_entries), start=1):
            futures.append(
                executor.submit(
                    process_batch_title_crew,
                    batch,
                    insert_query,
                    db_pool,
                    batch_number,
                    existing_movie_ids,
                    existing_person_ids,
                    movie_ids_lock,
                    person_ids_lock
                )
            )
        for future in as_completed(futures):
            try:
                inserted, skipped = future.result()
                new_crew += inserted
                skipped_crew += skipped
            except Exception as e:
                logging.error(f"Error in processing batch: {e}", exc_info=True)
    logging.info(f"title.crew.csv - Total New: {new_crew}, Total Skipped: {skipped_crew}")
    return new_crew, skipped_crew


def verify_data(db_pool: pool.SimpleConnectionPool) -> None:
    """Verify the data in the database tables.

    Args:
        db_pool: The database connection pool.
    """
    tables: List[str] = ['movies', 'persons', 'movie_crew', 'movie_ratings']
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                logging.info(f"Total rows in {table} table: {count}")
    except Exception as e:
        logging.error(f"Error verifying data: {e}", exc_info=True)
        raise
    finally:
        db_pool.putconn(conn)


def process_all_files(
    db_pool: pool.SimpleConnectionPool,
    max_entries_per_file: int = MAX_ENTRIES_PER_FILE
) -> None:
    """Process all CSV files in the correct order to maintain referential integrity.

    Args:
        db_pool: The database connection pool.
        max_entries_per_file: The maximum number of entries to process from each file.
    """
    persons_lock = threading.Lock()
    movies_lock = threading.Lock()
    tasks: List[Tuple[str, str, Optional[threading.Lock]]] = [
        (
            os.path.join(DATA_DIR, 'name.basics.csv'),
            """
            INSERT INTO persons (id, name, birth_year, death_year, professions)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            """,
            persons_lock
        ),
        (
            os.path.join(DATA_DIR, 'title.basics.csv'),
            """
            INSERT INTO movies (
                id, title, original_title, type, is_adult,
                start_year, end_year, runtime, genres
            ) VALUES %s
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            """,
            movies_lock
        ),
        (
            os.path.join(DATA_DIR, 'title.ratings.csv'),
            """
            INSERT INTO movie_ratings (movie_id, avg_rating, num_votes)
            VALUES %s
            ON CONFLICT (movie_id) DO UPDATE SET
                avg_rating = EXCLUDED.avg_rating,
                num_votes = EXCLUDED.num_votes
            RETURNING movie_id
            """,
            None
        ),
        (
            os.path.join(DATA_DIR, 'title.crew.csv'),
            """
            INSERT INTO movie_crew (movie_id, person_id, role)
            VALUES %s
            ON CONFLICT (movie_id, person_id, role) DO NOTHING
            RETURNING movie_id
            """,
            None
        )
    ]

    for file_path, insert_query, lock in tasks:
        if os.path.exists(file_path):
            logging.info(f"Processing file: {file_path}")
            try:
                if 'name.basics.csv' in file_path:
                    process_name_basics(file_path, insert_query, db_pool, lock, max_entries_per_file)
                elif 'title.basics.csv' in file_path:
                    process_title_basics(file_path, insert_query, db_pool, lock, max_entries_per_file)
                elif 'title.ratings.csv' in file_path:
                    process_title_ratings(file_path, insert_query, db_pool, max_entries_per_file)
                elif 'title.crew.csv' in file_path:
                    process_title_crew(file_path, insert_query, db_pool, max_entries_per_file)
            except Exception as e:
                logging.error(f"Failed to process file {file_path}: {e}", exc_info=True)
                continue
        else:
            logging.warning(f"File not found: {file_path}")


def main() -> None:
    """Main function to initiate data processing."""
    try:
        db_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=MAX_WORKERS,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        logging.info(f"Connected to database: {DB_NAME} at {DB_HOST}:{DB_PORT} as user {DB_USER}")
    except Exception as e:
        logging.error(f"Failed to create database connection pool: {e}", exc_info=True)
        raise

    try:
        logging.info("Starting data processing")
        process_all_files(db_pool)
        logging.info("Data processing completed successfully")
        verify_data(db_pool)
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if db_pool:
            db_pool.closeall()
            logging.info("Database connection pool closed")


if __name__ == "__main__":
    main()
