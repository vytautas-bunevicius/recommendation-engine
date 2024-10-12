import csv
import psycopg2
from psycopg2.extras import execute_batch
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_value(value, convert_to_int=False):
    if value == '\\N':
        return None
    if convert_to_int:
        try:
            return int(value)
        except ValueError:
            return None
    return value

def process_title_basics(cursor):
    with open('data/title.basics.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        data = []
        for i, row in enumerate(reader, start=1):
            try:
                if len(row) < 9:
                    logging.warning(f"Row {i} in title.basics.csv has insufficient columns: {row}")
                    continue
                data.append((
                    row[0],
                    row[2],
                    row[1],
                    clean_value(row[5], convert_to_int=True),
                    clean_value(row[6], convert_to_int=True),
                    clean_value(row[7], convert_to_int=True),
                    row[8]
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

def process_title_ratings(cursor):
    with open('data/title.ratings.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        data = []
        for i, row in enumerate(reader, start=1):
            try:
                if len(row) < 3:
                    logging.warning(f"Row {i} in title.ratings.csv has insufficient columns: {row}")
                    continue
                data.append((
                    clean_value(row[1]),
                    clean_value(row[2], convert_to_int=True),
                    row[0]
                ))
                if i % 10000 == 0:
                    logging.info(f"Processed {i} rows from title.ratings.csv")
            except Exception as e:
                logging.error(f"Error processing row {i} in title.ratings.csv: {e}")
                logging.error(f"Problematic row: {row}")

        logging.info(f"Updating {len(data)} rows in movies table")
        execute_batch(cursor, """
            UPDATE movies SET avg_rating = %s, num_votes = %s
            WHERE id = %s
        """, data)

def process_name_basics(cursor):
    with open('data/name.basics.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        data = []
        for i, row in enumerate(reader, start=1):
            try:
                if len(row) < 5:
                    logging.warning(f"Row {i} in name.basics.csv has insufficient columns: {row}")
                    continue
                data.append((
                    row[0],
                    row[1],
                    clean_value(row[2], convert_to_int=True),
                    clean_value(row[3], convert_to_int=True),
                    row[4]
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

def process_title_crew(cursor):
    with open('data/title.crew.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for i, row in enumerate(reader, start=1):
            try:
                if len(row) < 3:
                    logging.warning(f"Row {i} in title.crew.csv has insufficient columns: {row}")
                    continue
                movie_id = row[0]
                directors = row[1].split(',') if row[1] != '\\N' else []
                writers = row[2].split(',') if row[2] != '\\N' else []

                for director_id in directors:
                    cursor.execute("""
                        INSERT INTO movie_crew (movie_id, person_id, role)
                        VALUES (%s, %s, 'director')
                        ON CONFLICT (movie_id, person_id, role) DO NOTHING
                    """, (movie_id, director_id))

                for writer_id in writers:
                    cursor.execute("""
                        INSERT INTO movie_crew (movie_id, person_id, role)
                        VALUES (%s, %s, 'writer')
                        ON CONFLICT (movie_id, person_id, role) DO NOTHING
                    """, (movie_id, writer_id))

                if i % 10000 == 0:
                    logging.info(f"Processed {i} rows from title.crew.csv")
            except Exception as e:
                logging.error(f"Error processing row {i} in title.crew.csv: {e}")
                logging.error(f"Problematic row: {row}")

def main():
    conn = psycopg2.connect(
        dbname="recommendation_engine",
        user="postgres",
        password="password",
        host="localhost"
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
