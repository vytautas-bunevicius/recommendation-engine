from flask import Blueprint, jsonify, request
from database.connection import get_db_connection

movies_bp = Blueprint('movies', __name__)

@movies_bp.route('/movies', methods=['GET'])
def get_movies():
    conn = get_db_connection()
    cursor = conn.cursor()

    limit = request.args.get('limit', 10, type=int)
    offset = request.args.get('offset', 0, type=int)

    cursor.execute("SELECT * FROM movies LIMIT %s OFFSET %s", (limit, offset))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(movies)

@movies_bp.route('/movies/<string:movie_id>', methods=['GET'])
def get_movie(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM movies WHERE id = %s", (movie_id,))
    movie = cursor.fetchone()

    if movie is None:
        return jsonify({"error": "Movie not found"}), 404

    cursor.close()
    conn.close()

    return jsonify(movie)

@movies_bp.route('/movies/search', methods=['GET'])
def search_movies():
    conn = get_db_connection()
    cursor = conn.cursor()

    query = request.args.get('query', '')
    limit = request.args.get('limit', 10, type=int)
    offset = request.args.get('offset', 0, type=int)

    cursor.execute("""
        SELECT * FROM movies
        WHERE title ILIKE %s
        ORDER BY num_votes DESC
        LIMIT %s OFFSET %s
    """, (f'%{query}%', limit, offset))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(movies)
