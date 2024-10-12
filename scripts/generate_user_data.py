import psycopg2
import random
from faker import Faker
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch

fake = Faker()

def generate_users(cursor, num_users=10000):
    users = [(fake.uuid4(), fake.name(), fake.date_of_birth(minimum_age=18, maximum_age=80).year) for _ in range(num_users)]
    execute_batch(cursor, """
        INSERT INTO users (id, name, birth_year)
        VALUES (%s, %s, %s)
    """, users)
    return [user[0] for user in users]

def generate_viewing_history(cursor, user_ids, num_views=100000):
    cursor.execute("SELECT id FROM movies")
    movie_ids = [row[0] for row in cursor.fetchall()]

    viewing_history = []
    for _ in range(num_views):
        user_id = random.choice(user_ids)
        movie_id = random.choice(movie_ids)
        watch_date = fake.date_between(start_date='-1y', end_date='today')
        watch_duration = random.randint(10, 180)  # 10 to 180 minutes
        viewing_history.append((user_id, movie_id, watch_date, watch_duration))

    execute_batch(cursor, """
        INSERT INTO viewing_history (user_id, movie_id, watch_date, watch_duration)
        VALUES (%s, %s, %s, %s)
    """, viewing_history)

def main():
    conn = psycopg2.connect(
        dbname="recommendation_engine",
        user="postgres",
        password="password",
        host="localhost"
    )
    cursor = conn.cursor()

    user_ids = generate_users(cursor)
    generate_viewing_history(cursor, user_ids)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
