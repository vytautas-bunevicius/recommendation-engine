from flask import Blueprint, jsonify, request
from database.connection import get_db_connection

users_bp = Blueprint('users', __name__)

@users_bp.route('/users/<uuid:user_id>', methods=['GET'])
def get_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE id = %s", (str(user_id),))
    user = cursor.fetchone()

    if user is None:
        return jsonify({"error": "User not found"}), 404

    cursor.close()
    conn.close()

    return jsonify(user)

@users_bp.route('/users/<uuid:user_id>/viewing_history', methods=['GET'])
def get_user_viewing_history(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT vh.*, m.title
        FROM viewing_history vh
        JOIN movies m ON vh.movie_id = m.id
        WHERE vh.user_id = %s
        ORDER BY vh.watch_date DESC
    """, (str(user_id),))
    history = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(history)
