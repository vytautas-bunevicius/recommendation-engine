"""
API module for handling movie-related endpoints.
Provides endpoints to retrieve movie information and search for movies.
"""

from typing import Any

from flask import Blueprint, jsonify, request
from flask_cors import CORS

from src.database.connection import get_db_connection

movies_bp = Blueprint('movies', __name__)
CORS(movies_bp)


@movies_bp.route('/movies', methods=['GET'])
def get_movies() -> Any:
    """Endpoint to retrieve a list of movies.

    Query Parameters:
        limit: Number of movies to return (default 10, max 100).
        offset: Number of movies to skip (default 0).
        sort: Sort order ('popularity', 'rating', 'year', default 'popularity').
        order: Sort direction ('asc' or 'desc', default 'desc').
        type: Type of content ('movie', 'tvSeries', default 'movie').
        min_votes: Minimum number of votes (default 1000).

    Returns:
        JSON response containing a list of movies.
    """
    limit = min(int(request.args.get('limit', 10)), 100)
    offset = int(request.args.get('offset', 0))
    sort = request.args.get('sort', 'popularity')
    order = request.args.get('order', 'desc').upper()
    content_type = request.args.get('type', 'movie')
    min_votes = int(request.args.get('min_votes', 1000))

    sort_options = {
        'popularity': 'num_votes',
        'rating': 'avg_rating',
        'year': 'start_year'
    }
    sort_column = sort_options.get(sort, 'num_votes')

    conn = get_db_connection()
    cursor = conn.cursor()

    query = f"""
        SELECT id, title, original_title, type, start_year,
               runtime, genres, avg_rating, num_votes
        FROM movies
        WHERE type = %s AND num_votes >= %s
        ORDER BY {sort_column} {order} NULLS LAST
        LIMIT %s OFFSET %s
    """

    cursor.execute(query, (content_type, min_votes, limit, offset))
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
        limit: Number of movies to return (default 10, max 100).
        offset: Number of movies to skip (default 0).
        type: Type of content ('movie', 'tvSeries', default 'movie').
        min_votes: Minimum number of votes (default 100).

    Returns:
        JSON response containing a list of movies matching the search query.
    """
    query = request.args.get('query', '', type=str)
    limit = min(int(request.args.get('limit', 10)), 100)
    offset = int(request.args.get('offset', 0))
    content_type = request.args.get('type', 'movie')
    min_votes = int(request.args.get('min_votes', 100))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, title, original_title, type, start_year,
               runtime, genres, avg_rating, num_votes
        FROM movies
        WHERE title ILIKE %s AND type = %s AND num_votes >= %s
        ORDER BY num_votes DESC NULLS LAST
        LIMIT %s OFFSET %s
    """, (f'%{query}%', content_type, min_votes, limit, offset))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(movies)
