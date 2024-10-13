"""
Module providing content-based recommendation functions.
Includes functions to generate recommendations based on user viewing history and to find similar movies.
"""

import logging
from typing import Any, Dict, List

import numpy as np
from psycopg2.extensions import cursor
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def get_content_based_recommendations(
    cursor: cursor,
    user_id: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Generates content-based movie recommendations for a user.

    Args:
        cursor: Database cursor.
        user_id: The UUID of the user.
        limit: Number of recommendations to return.

    Returns:
        List of recommended movies as dictionaries.
    """
    logging.info(f"Getting content-based recommendations for user {user_id}")

    cursor.execute("""
        SELECT DISTINCT m.id, m.title, m.genres
        FROM viewing_history vh
        JOIN movies m ON vh.movie_id = m.id
        WHERE vh.user_id = %s
    """, (user_id,))
    user_movies = cursor.fetchall()

    logging.info(f"User has watched {len(user_movies)} unique movies")

    if not user_movies:
        logging.warning(f"No viewing history found for user {user_id}")
        return get_popular_movies(cursor, limit)

    cursor.execute("SELECT id, title, genres FROM movies")
    all_movies = cursor.fetchall()

    logging.info(f"Total movies in database: {len(all_movies)}")

    movie_ids = [movie['id'] for movie in all_movies]
    movie_texts = [f"{movie['title']} {movie['genres']}" for movie in all_movies]
    movie_id_to_index = {movie_id: idx for idx, movie_id in enumerate(movie_ids)}

    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf.fit_transform(movie_texts)

    user_movie_indices = [movie_id_to_index[movie['id']] for movie in user_movies if movie['id'] in movie_id_to_index]
    if not user_movie_indices:
        logging.warning("No valid movies in user's history")
        return get_popular_movies(cursor, limit)

    user_movie_vectors = tfidf_matrix[user_movie_indices]
    cosine_similarities = cosine_similarity(user_movie_vectors, tfidf_matrix)

    aggregated_similarities = cosine_similarities.mean(axis=0)

    user_seen_movie_ids = set(movie['id'] for movie in user_movies)
    similar_indices = np.argsort(-aggregated_similarities)

    recommendations = []
    for idx in similar_indices:
        movie_id = movie_ids[idx]
        if movie_id not in user_seen_movie_ids:
            recommendations.append((movie_id, aggregated_similarities[idx]))
        if len(recommendations) >= limit:
            break

    recommended_movie_ids = [rec[0] for rec in recommendations]
    placeholders = ','.join(['%s'] * len(recommended_movie_ids))
    cursor.execute(f"""
        SELECT id, title, genres, avg_rating, start_year
        FROM movies
        WHERE id IN ({placeholders})
    """, recommended_movie_ids)
    recommended_movies = cursor.fetchall()

    movie_id_to_similarity = {rec[0]: rec[1] for rec in recommendations}
    recommended_movies.sort(key=lambda x: movie_id_to_similarity[x['id']], reverse=True)

    logging.info(f"Returning {len(recommended_movies)} recommendations")
    return recommended_movies

def get_similar_movies(
    cursor: cursor,
    movie_id: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Finds movies similar to the given movie based on content.

    Args:
        cursor: Database cursor.
        movie_id: The ID of the target movie.
        limit: Number of similar movies to return.

    Returns:
        List of similar movies as dictionaries.
    """
    cursor.execute("SELECT id, title, genres FROM movies WHERE id = %s", (movie_id,))
    target_movie = cursor.fetchone()

    if not target_movie:
        return []

    cursor.execute("SELECT id, title, genres FROM movies")
    all_movies = cursor.fetchall()

    movie_ids = [movie['id'] for movie in all_movies]
    movie_texts = [f"{movie['title']} {movie['genres']}" for movie in all_movies]
    movie_id_to_index = {movie_id: idx for idx, movie_id in enumerate(movie_ids)}

    tfidf = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf.fit_transform(movie_texts)

    target_index = movie_id_to_index[target_movie['id']]
    target_vector = tfidf_matrix[target_index]

    cosine_similarities = cosine_similarity(target_vector, tfidf_matrix).flatten()

    similar_indices = np.argsort(-cosine_similarities)

    recommendations = []
    for idx in similar_indices:
        if movie_ids[idx] != target_movie['id']:
            recommendations.append((movie_ids[idx], cosine_similarities[idx]))
        if len(recommendations) >= limit:
            break

    recommended_movie_ids = [rec[0] for rec in recommendations]
    placeholders = ','.join(['%s'] * len(recommended_movie_ids))
    cursor.execute(f"""
        SELECT id, title, genres, avg_rating, start_year
        FROM movies
        WHERE id IN ({placeholders})
    """, recommended_movie_ids)
    similar_movies = cursor.fetchall()

    movie_id_to_similarity = {rec[0]: rec[1] for rec in recommendations}
    similar_movies.sort(key=lambda x: movie_id_to_similarity[x['id']], reverse=True)

    return similar_movies

def get_popular_movies(cursor: cursor, limit: int = 10) -> List[Dict[str, Any]]:
    """Gets popular movies as a fallback recommendation.

    Args:
        cursor: Database cursor.
        limit: Number of movies to return.

    Returns:
        List of popular movies as dictionaries.
    """
    logging.info("Fetching popular movies as fallback")
    cursor.execute("""
        SELECT id, title, genres, avg_rating, start_year
        FROM movies
        WHERE num_votes > 1000
        ORDER BY avg_rating DESC
        LIMIT %s
    """, (limit,))
    popular_movies = cursor.fetchall()
    logging.info(f"Returning {len(popular_movies)} popular movies")
    return popular_movies
