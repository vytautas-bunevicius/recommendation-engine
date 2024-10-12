"""
API module for handling movie-related endpoints.
Provides endpoints to retrieve movie information and search for movies.
"""

from typing import Any

from flask import Blueprint, jsonify, request

from database.connection import get_db_connection

movies_bp = Blueprint('movies', __name__)


@movies_bp.route('/movies', methods=['GET'])
def get_movies() -> Any:
    """Endpoint to retrieve a list of movies.

    Query Parameters:
        limit: Number of movies to return (default 10).
        offset: Number of movies to skip (default 0).

    Returns:
        JSON response containing a list of movies.
    """
    limit = request.args.get('limit', 10, type=int)
    offset = request.args.get('offset', 0, type=int)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM movies LIMIT %s OFFSET %s", (limit, offset))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(movies)


@movies_bp.route('/movies/<string:movie_id>', methods=['GET'])
def get_movie(movie_id: str) -> Any:
    """Endpoint to retrieve details of a specific movie.

    Args:
        movie_id: The ID of the movie.

    Returns:
        JSON response containing movie details or an error message.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM movies WHERE id = %s", (movie_id,))
    movie = cursor.fetchone()

    cursor.close()
    conn.close()

    if movie:
        return jsonify(movie)
    else:
        return jsonify({"error": "Movie not found"}), 404


@movies_bp.route('/movies/search', methods=['GET'])
def search_movies() -> Any:
    """Endpoint to search for movies by title.

    Query Parameters:
        query: The search query string.
        limit: Number of movies to return (default 10).
        offset: Number of movies to skip (default 0).

    Returns:
        JSON response containing a list of movies matching the search query.
    """
    query = request.args.get('query', '', type=str)
    limit = request.args.get('limit', 10, type=int)
    offset = request.args.get('offset', 0, type=int)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM movies
        WHERE title ILIKE %s
        ORDER BY num_votes DESC NULLS LAST
        LIMIT %s OFFSET %s
    """, (f'%{query}%', limit, offset))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(movies)
