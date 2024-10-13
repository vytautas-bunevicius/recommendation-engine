"""
API module for handling user-related endpoints.
Provides endpoints to retrieve user information and viewing history.
"""

from typing import Any

from flask import Blueprint, jsonify, request
from flask_cors import CORS

from src.database.connection import get_db_connection

users_bp = Blueprint('users', __name__)
CORS(users_bp)

@users_bp.route('/users', methods=['GET'])
def get_all_users():
    """Endpoint to retrieve all users.

    Returns:
        JSON response containing a list of all users.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, name FROM users")
    users = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(users)


@users_bp.route('/users/<uuid:user_id>', methods=['GET'])
def get_user(user_id) -> Any:
    """Endpoint to retrieve user information.

    Args:
        user_id: The UUID of the user.

    Returns:
        JSON response containing user details or an error message.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE id = %s", (str(user_id),))
    user = cursor.fetchone()

    cursor.close()
    conn.close()

    if user:
        return jsonify(user)
    else:
        return jsonify({"error": "User not found"}), 404


@users_bp.route('/users/<uuid:user_id>/viewing_history', methods=['GET'])
def get_user_viewing_history(user_id) -> Any:
    """Endpoint to retrieve a user's viewing history.

    Args:
        user_id: The UUID of the user.

    Returns:
        JSON response containing the user's viewing history.
    """
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

@users_bp.route('/users/<uuid:user_id>/viewing_history', methods=['POST'])
def add_viewing_history(user_id):
    data = request.json
    required_fields = ['movie_id', 'watch_date', 'watch_duration']

    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO viewing_history (user_id, movie_id, watch_date, watch_duration)
            VALUES (%s, %s, %s, %s)
        """, (str(user_id), data['movie_id'], data['watch_date'], data['watch_duration']))
        conn.commit()
        return jsonify({"message": "Viewing history added successfully"}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 400
    finally:
        cursor.close()
        conn.close()
