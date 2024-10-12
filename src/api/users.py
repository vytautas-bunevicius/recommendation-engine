"""
API module for handling user-related endpoints.
Provides endpoints to retrieve user information and viewing history.
"""

from typing import Any

from flask import Blueprint, jsonify

from database.connection import get_db_connection

users_bp = Blueprint('users', __name__)


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
