from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

def get_content_based_recommendations(cursor, user_id, limit=10):
    # Get user's viewing history
    cursor.execute("""
        SELECT DISTINCT m.id, m.title, m.genres
        FROM viewing_history vh
        JOIN movies m ON vh.movie_id = m.id
        WHERE vh.user_id = %s
    """, (user_id,))
    user_movies = cursor.fetchall()

    if not user_movies:
        return []

    # Get all movies
    cursor.execute("SELECT id, title, genres FROM movies")
    all_movies = cursor.fetchall()

    # Prepare data for TF-IDF
    movie_data = {movie['id']: f"{movie['title']} {movie['genres']}" for movie in all_movies}

    # Create TF-IDF matrix
    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf.fit_transform(movie_data.values())

    # Calculate similarity between user's movies and all movies
    user_movie_ids = [movie['id'] for movie in user_movies]
    user_movie_indices = [list(movie_data.keys()).index(movie_id) for movie_id in user_movie_ids]
    user_movie_vectors = tfidf_matrix[user_movie_indices]

    cosine_similarities = cosine_similarity(user_movie_vectors, tfidf_matrix)

    # Aggregate similarities
    aggregated_similarities = np.mean(cosine_similarities, axis=0)

    # Get top similar movies
    similar_indices = aggregated_similarities.argsort()[::-1]
    similar_items = [(movie_id, aggregated_similarities[i])
                     for i, movie_id in enumerate(movie_data.keys())
                     if movie_id not in user_movie_ids]

    # Sort by similarity and get top recommendations
    recommendations = sorted(similar_items, key=lambda x: x[1], reverse=True)[:limit]

    # Fetch movie details for recommendations
    movie_ids = [rec[0] for rec in recommendations]
    placeholders = ','.join(['%s'] * len(movie_ids))
    cursor.execute(f"""
        SELECT id, title, genres, avg_rating
        FROM movies
        WHERE id IN ({placeholders})
    """, movie_ids)

    recommended_movies = cursor.fetchall()

    return recommended_movies

def get_similar_movies(cursor, movie_id, limit=10):
    # Get the target movie
    cursor.execute("SELECT id, title, genres FROM movies WHERE id = %s", (movie_id,))
    target_movie = cursor.fetchone()

    if not target_movie:
        return []

    # Get all movies
    cursor.execute("SELECT id, title, genres FROM movies")
    all_movies = cursor.fetchall()

    # Prepare data for TF-IDF
    movie_data = {movie['id']: f"{movie['title']} {movie['genres']}" for movie in all_movies}

    # Create TF-IDF matrix
    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf.fit_transform(movie_data.values())

    # Calculate similarity between target movie and all movies
    target_movie_index = list(movie_data.keys()).index(target_movie['id'])
    target_movie_vector = tfidf_matrix[target_movie_index]

    cosine_similarities = cosine_similarity(target_movie_vector, tfidf_matrix)

    # Get top similar movies
    similar_indices = cosine_similarities.argsort()[0][::-1]
    similar_items = [(movie_id, cosine_similarities[0][i])
                     for i, movie_id in enumerate(movie_data.keys())
                     if movie_id != target_movie['id']]

    # Sort by similarity and get top recommendations
    recommendations = sorted(similar_items, key=lambda x: x[1], reverse=True)[:limit]

    # Fetch movie details for recommendations
    movie_ids = [rec[0] for rec in recommendations]
    placeholders = ','.join(['%s'] * len(movie_ids))
    cursor.execute(f"""
        SELECT id, title, genres, avg_rating
        FROM movies
        WHERE id IN ({placeholders})
    """, movie_ids)

    similar_movies = cursor.fetchall()

    return similar_movies
