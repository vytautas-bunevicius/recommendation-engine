"""
API module for handling recommendation-related endpoints.
Provides endpoints to get personalized recommendations and similar movies.
"""

import logging
from typing import Any

from flask import Blueprint, jsonify, request
from flask_cors import CORS

from src.database.connection import get_db_connection
from src.recommender.content_based import (
    get_content_based_recommendations,
    get_similar_movies
)

recommendations_bp = Blueprint('recommendations', __name__)
CORS(recommendations_bp)

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
    logging.info(f"Fetching recommendations for user {user_id} with limit {limit}")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        recommendations = get_content_based_recommendations(cursor, str(user_id), limit)
        logging.info(f"Retrieved {len(recommendations)} recommendations")
        return jsonify(recommendations)
    except Exception as e:
        logging.error(f"Error getting recommendations: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

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
    logging.info(f"Fetching similar movies for movie {movie_id} with limit {limit}")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        similar_movies = get_similar_movies(cursor, movie_id, limit)
        logging.info(f"Retrieved {len(similar_movies)} similar movies")
        return jsonify(similar_movies)
    except Exception as e:
        logging.error(f"Error getting similar movies: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()
