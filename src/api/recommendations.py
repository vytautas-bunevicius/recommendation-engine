from flask import Blueprint, jsonify, request
from database.connection import get_db_connection
from recommender.content_based import get_content_based_recommendations

recommendations_bp = Blueprint('recommendations', __name__)

@recommendations_bp.route('/users/<uuid:user_id>/recommendations', methods=['GET'])
def get_recommendations(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    limit = request.args.get('limit', 10, type=int)

    recommendations = get_content_based_recommendations(cursor, str(user_id), limit)

    cursor.close()
    conn.close()

    return jsonify(recommendations)

@recommendations_bp.route('/movies/<string:movie_id>/similar', methods=['GET'])
def get_similar_movies(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    limit = request.args.get('limit', 10, type=int)

    # Create a temporary user with only this movie in their history
    cursor.execute("INSERT INTO users (id, name, birth_year) VALUES (uuid_generate_v4(), 'Temporary User', 2000) RETURNING id")
    temp_user_id = cursor.fetchone()['id']

    cursor.execute("INSERT INTO viewing_history (user_id, movie_id, watch_date, watch_duration) VALUES (%s, %s, CURRENT_DATE, 120)", (temp_user_id, movie_id))

    recommendations = get_content_based_recommendations(cursor, temp_user_id, limit)

    # Clean up temporary user
    cursor.execute("DELETE FROM viewing_history WHERE user_id = %s", (temp_user_id,))
    cursor.execute("DELETE FROM users WHERE id = %s", (temp_user_id,))

    cursor.close()
    conn.close()

    return jsonify(recommendations)
