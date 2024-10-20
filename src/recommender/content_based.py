"""Module providing content-based recommendation functions.

Includes a class ContentBasedRecommender that precomputes the TF-IDF matrix
and generates recommendations based on user viewing history or similar movies.
"""

import logging
from typing import Any, Dict, List

import numpy as np
from psycopg2.extensions import cursor
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


class ContentBasedRecommender:
    """Content-based recommender system using TF-IDF and cosine similarity."""

    def __init__(self, cursor: cursor) -> None:
        """Initializes the recommender by precomputing the TF-IDF matrix.

        Args:
            cursor: Database cursor to fetch movie data.
        """
        logging.info('Initializing ContentBasedRecommender')
        cursor.execute(
            '''
            SELECT m.id, m.title, m.genres, mr.num_votes
            FROM movies m
            JOIN movie_ratings mr ON m.id = mr.movie_id
            WHERE mr.num_votes > 1000
            '''
        )
        all_movies = cursor.fetchall()

        self.movie_ids = [movie['id'] for movie in all_movies]
        self.movie_texts = [
            f"{movie['title']} {movie['genres']}" for movie in all_movies
        ]
        self.movie_id_to_index = {
            movie_id: idx for idx, movie_id in enumerate(self.movie_ids)
        }
        self.index_to_movie_id = {
            idx: movie_id for idx, movie_id in enumerate(self.movie_ids)
        }

        logging.info('Computing TF-IDF matrix')
        self.tfidf = TfidfVectorizer(stop_words='english')
        self.tfidf_matrix = self.tfidf.fit_transform(self.movie_texts)
        logging.info('TF-IDF matrix computed')

    def get_content_based_recommendations(
        self, cursor: cursor, user_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Generates content-based movie recommendations for a user.

        Args:
            cursor: Database cursor.
            user_id: The UUID of the user.
            limit: Number of recommendations to return.

        Returns:
            A list of recommended movies as dictionaries.
        """
        logging.info(f'Getting recommendations for user {user_id}')
        cursor.execute(
            """
            SELECT DISTINCT m.id
            FROM viewing_history vh
            JOIN movies m ON vh.movie_id = m.id
            JOIN movie_ratings mr ON m.id = mr.movie_id
            WHERE vh.user_id = %s AND mr.num_votes > 1000
            """,
            (user_id,),
        )
        user_movies = cursor.fetchall()

        if not user_movies:
            logging.warning(f'No viewing history found for user {user_id}')
            return self.get_popular_movies(cursor, limit)

        user_movie_indices = [
            self.movie_id_to_index[movie['id']]
            for movie in user_movies
            if movie['id'] in self.movie_id_to_index
        ]

        if not user_movie_indices:
            logging.warning('No valid movies in user history')
            return self.get_popular_movies(cursor, limit)

        user_profile = np.mean(self.tfidf_matrix[user_movie_indices].toarray(), axis=0)
        cosine_similarities = cosine_similarity([user_profile], self.tfidf_matrix.toarray()).flatten()

        user_seen_movie_ids = {movie['id'] for movie in user_movies}
        similar_indices = np.argsort(-cosine_similarities)

        recommendations = []
        for idx in similar_indices:
            movie_id = self.index_to_movie_id[idx]
            if movie_id not in user_seen_movie_ids:
                recommendations.append(movie_id)
            if len(recommendations) >= limit:
                break

        if not recommendations:
            return self.get_popular_movies(cursor, limit)

        placeholders = ','.join(['%s'] * len(recommendations))
        cursor.execute(
            f"""
            SELECT m.id, m.title, m.genres, mr.avg_rating, m.start_year
            FROM movies m
            JOIN movie_ratings mr ON m.id = mr.movie_id
            WHERE m.id IN ({placeholders})
            ORDER BY mr.avg_rating DESC
            """,
            recommendations,
        )
        recommended_movies = cursor.fetchall()
        return recommended_movies

    def get_similar_movies(
        self, cursor: cursor, movie_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Finds movies similar to the given movie based on content.

        Args:
            cursor: Database cursor.
            movie_id: The ID of the target movie.
            limit: Number of similar movies to return.

        Returns:
            A list of similar movies as dictionaries.
        """
        if movie_id not in self.movie_id_to_index:
            logging.warning(f'Movie ID {movie_id} not found in index')
            return []

        target_index = self.movie_id_to_index[movie_id]
        target_vector = self.tfidf_matrix[target_index].toarray()

        cosine_similarities = cosine_similarity(target_vector, self.tfidf_matrix.toarray()).flatten()
        similar_indices = np.argsort(-cosine_similarities)

        recommendations = []
        for idx in similar_indices:
            if self.index_to_movie_id[idx] != movie_id:
                recommendations.append(self.index_to_movie_id[idx])
            if len(recommendations) >= limit:
                break

        if not recommendations:
            return []

        placeholders = ','.join(['%s'] * len(recommendations))
        cursor.execute(
            f"""
            SELECT m.id, m.title, m.genres, mr.avg_rating, m.start_year
            FROM movies m
            JOIN movie_ratings mr ON m.id = mr.movie_id
            WHERE m.id IN ({placeholders})
            ORDER BY mr.avg_rating DESC
            """,
            recommendations,
        )
        similar_movies = cursor.fetchall()
        return similar_movies

    def get_popular_movies(
        self, cursor: cursor, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Gets popular movies as a fallback recommendation.

        Args:
            cursor: Database cursor.
            limit: Number of movies to return.

        Returns:
            A list of popular movies as dictionaries.
        """
        logging.info('Fetching popular movies as fallback')
        cursor.execute(
            """
            SELECT m.id, m.title, m.genres, mr.avg_rating, m.start_year
            FROM movies m
            JOIN movie_ratings mr ON m.id = mr.movie_id
            WHERE mr.num_votes > 10000
            ORDER BY mr.avg_rating DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cursor.fetchall()
