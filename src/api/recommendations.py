"""
API module for handling recommendation-related endpoints.
Provides endpoints to get personalized recommendations and similar movies.
"""

from typing import Any

from flask import Blueprint, jsonify, request

from src.database.connection import get_db_connection
from src.recommender.content_based import (
    get_content_based_recommendations,
    get_similar_movies
)

recommendations_bp = Blueprint('recommendations', __name__)


@recommendations_bp.route('/users/<uuid:user_id>/recommendations', methods=['GET'])
def get_recommendations(user_id) -> Any:
    """Endpoint to get personalized movie recommendations for a user.

    Args:
        user_id: The UUID of the user.

    Query Parameters:
        limit: Number of recommendations to return (default 10).

    Returns:
        JSON response containing a list of recommended movies.
    """
    limit = request.args.get('limit', 10, type=int)

    conn = get_db_connection()
    cursor = conn.cursor()

    recommendations = get_content_based_recommendations(cursor, str(user_id), limit)

    cursor.close()
    conn.close()

    return jsonify(recommendations)


@recommendations_bp.route('/movies/<string:movie_id>/similar', methods=['GET'])
def get_similar_movies_endpoint(movie_id: str) -> Any:
    """Endpoint to get movies similar to a given movie.

    Args:
        movie_id: The ID of the movie.

    Query Parameters:
        limit: Number of similar movies to return (default 10).

    Returns:
        JSON response containing a list of similar movies.
    """
    limit = request.args.get('limit', 10, type=int)

    conn = get_db_connection()
    cursor = conn.cursor()

    similar_movies = get_similar_movies(cursor, movie_id, limit)

    cursor.close()
    conn.close()

    return jsonify(similar_movies)
